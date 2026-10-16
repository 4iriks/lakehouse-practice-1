"""Reproducible Q18 lakehouse experiment. Run from the repository root."""
import argparse
import csv
import json
import platform
import io
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
import pyarrow.parquet as pq
import pyarrow.orc as orc
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results'
OUT.mkdir(exist_ok=True)
HTTP = requests.Session()
HTTP.trust_env = False
Q18 = (ROOT / 'sql/q18.sql').read_text(encoding='utf-8')
VARIANTS = [('parquet_gzip', 'PARQUET', 'GZIP'), ('parquet_zstd', 'PARQUET', 'ZSTD'),
            ('orc_snappy', 'ORC', 'SNAPPY'), ('orc_zstd', 'ORC', 'ZSTD')]
TABLES = {'customer': 'custkey, name',
          'orders': 'orderkey, custkey, orderdate, totalprice',
          'lineitem': 'orderkey, linenumber, quantity'}


def save(name, value):
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')


def log(message):
    print(datetime.now(timezone.utc).isoformat(timespec='seconds'), message, flush=True)


def compose(*args):
    subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True)


def wait_trino():
    for _ in range(90):
        try:
            r = HTTP.get('http://localhost:8080/v1/info', timeout=3)
            if r.ok and not r.json().get('starting', True):
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError('Trino did not become ready')


def query(sql, session=None):
    headers = {'X-Trino-User': 'vadim', 'X-Trino-Source': 'practice-1'}
    if session:
        headers['X-Trino-Session'] = ','.join(f'{k}={v}' for k, v in session.items())
    started = time.perf_counter()
    r = HTTP.post('http://localhost:8080/v1/statement', data=sql.encode(), headers=headers, timeout=300)
    r.raise_for_status()
    rows, columns = [], []
    while True:
        body = r.json()
        if 'error' in body:
            raise RuntimeError(json.dumps(body['error'], ensure_ascii=False))
        columns = body.get('columns', columns)
        rows.extend(body.get('data', []))
        if not body.get('nextUri'):
            break
        r = HTTP.get(body['nextUri'], headers=headers, timeout=300)
        r.raise_for_status()
    elapsed = time.perf_counter() - started
    details = HTTP.get('http://localhost:8080/v1/query/' + body['id'], headers=headers, timeout=20)
    details.raise_for_status()
    stats = compact_stats(details.json()['queryStats'])
    return {'query_id': body['id'], 'elapsed_s': elapsed, 'rows': rows,
            'columns': [c['name'] for c in columns], 'stats': body.get('stats', {}),
            'query_stats': stats}


def compact_stats(stats):
    keys = ['createTime','endTime','elapsedTime','queuedTime','analysisTime','planningTime',
            'executionTime','finishingTime','totalCpuTime','totalScheduledTime','totalBlockedTime',
            'physicalInputDataSize','physicalInputPositions','physicalInputReadTime',
            'processedInputDataSize','processedInputPositions','physicalWrittenDataSize',
            'outputDataSize','outputPositions','peakUserMemoryReservation','spilledDataSize']
    return {key: stats[key] for key in keys if key in stats}


def sql(text, session=None):
    return query(text, session)['rows']


def s3():
    return boto3.client('s3', endpoint_url='http://localhost:9000',
                        aws_access_key_id='lab-admin', aws_secret_access_key='local-lab-minio-password',
                        region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))


def setup():
    client = s3()
    if 'warehouse' not in [b['Name'] for b in client.list_buckets()['Buckets']]:
        client.create_bucket(Bucket='warehouse')
    base = 'http://localhost:8181/management/v1'
    info = HTTP.get(base + '/info', timeout=20)
    log(f'Lakekeeper info: {info.status_code} {info.text[:400]}')
    response = HTTP.post(base + '/bootstrap', json={'accept-terms-of-use': True}, timeout=30)
    if response.status_code not in (200, 201, 204, 400, 409):
        response.raise_for_status()
    payload = {'warehouse-name': 'practice1', 'project-id': '00000000-0000-0000-0000-000000000000',
               'storage-profile': {'type': 's3', 'bucket': 'warehouse', 'region': 'us-east-1',
                                   'endpoint': 'http://minio:9000', 'path-style-access': True,
                                   'flavor': 'minio', 'sts-enabled': False},
               'storage-credential': {'type': 's3', 'credential-type': 'access-key',
                                      'aws-access-key-id': 'lab-admin',
                                      'aws-secret-access-key': 'local-lab-minio-password'}}
    response = HTTP.post(base + '/warehouse', json=payload, timeout=30)
    if response.status_code not in (200, 201, 409):
        raise RuntimeError(response.text)
    log(f'Warehouse: {response.status_code}')


def canonical(rows):
    # Order within tied sort keys is unspecified by Q18. Compare by order key.
    return sorted([[str(x) if i == 3 else round(float(x), 6) if i in (4, 5) else x
                    for i, x in enumerate(row)] for row in rows], key=lambda x: x[2])


def assert_q18(rows):
    expected = json.loads((OUT / 'reference_q18.json').read_text(encoding='utf-8'))['rows']
    if canonical(rows) != canonical(expected):
        raise AssertionError('Q18 result does not match tpch.sf1')


def create_bronze(namespace, fmt, codec, target='128MB'):
    sql(f'CREATE SCHEMA IF NOT EXISTS lake.{namespace}')
    measurements = []
    for table, columns in TABLES.items():
        sql(f'DROP TABLE IF EXISTS lake.{namespace}.{table}')
        result = query(f"CREATE TABLE lake.{namespace}.{table} WITH (format='{fmt}') AS "
                       f'SELECT {columns} FROM tpch.sf1.{table}',
                       {'lake.compression_codec': codec, 'lake.target_max_file_size': target,
                        'task_min_writer_count': '1', 'task_max_writer_count': '1',
                        'task_scale_writers_enabled': 'false', 'scale_writers': 'false'})
        measurements.append({'table': table, 'elapsed_s': result['elapsed_s'],
                             'query_id': result['query_id'], 'query_stats': result['query_stats']})
    return measurements


def inventory(namespace, table):
    full = f'lake.{namespace}.{table}'
    files = query(f'SELECT file_path, file_size_in_bytes, record_count, file_format FROM lake.{namespace}."{table}$files"')
    paths = {row[0].split('warehouse/', 1)[1] for row in files['rows']}
    # Get the table location from SHOW CREATE TABLE, which includes its unique location.
    ddl = sql(f'SHOW CREATE TABLE {full}')[0][0]
    import re
    match = re.search(r"location\s*=\s*'s3[a]?://warehouse/([^']+)'", ddl)
    if match:
        prefix = match.group(1).rstrip('/') + '/'
    elif paths:
        prefix = next(iter(paths)).split('/data/')[0] + '/'
    else:
        raise RuntimeError('Cannot identify table prefix')
    objects = []
    for page in s3().get_paginator('list_objects_v2').paginate(Bucket='warehouse', Prefix=prefix):
        objects.extend(page.get('Contents', []))
    metadata_bytes = sum(o['Size'] for o in objects if '/metadata/' in o['Key'])
    active_bytes = sum(r[1] for r in files['rows'])
    obsolete_bytes = sum(o['Size'] for o in objects if '/data/' in o['Key'] and o['Key'] not in paths)
    codecs=set()
    embedded_metadata_bytes=0
    client=s3()
    for file_path, file_bytes, _, file_format in files['rows']:
        key=file_path.split('warehouse/',1)[1]
        if file_format=='PARQUET':
            trailer=client.get_object(Bucket='warehouse',Key=key,Range='bytes=-8')['Body'].read()
            footer_size=int.from_bytes(trailer[:4],'little')
            footer=client.get_object(Bucket='warehouse',Key=key,Range=f'bytes=-{footer_size+8}')['Body'].read()
            meta=pq.read_metadata(io.BytesIO(b'PAR1'+footer))
            codecs.update(meta.row_group(g).column(c).compression for g in range(meta.num_row_groups) for c in range(meta.num_columns))
            embedded_metadata_bytes+=footer_size+8
        else:
            reader=orc.ORCFile(io.BytesIO(client.get_object(Bucket='warehouse',Key=key)['Body'].read()))
            codecs.add(reader.compression)
            embedded_metadata_bytes+=reader.file_footer_length+reader.file_postscript_length+reader.stripe_statistics_length
    return {'schema': namespace, 'table': table, 'rows': sql(f'SELECT count(*) FROM {full}')[0][0],
            'active_data_bytes': active_bytes, 'metadata_bytes': metadata_bytes,
            'retained_data_bytes': obsolete_bytes, 'data_files': len(paths),
            'metadata_files': sum('/metadata/' in o['Key'] for o in objects),
            'files': files['rows'], 'ddl': ddl,'physical_codecs':sorted(codecs),
            'embedded_footer_bytes':embedded_metadata_bytes}


def load():
    wait_trino()
    if not (OUT/'reference_q18.json').exists():
        save('reference_q18.json', query(Q18.format(schema='tpch.sf1')))
    source = {t: sql(f'SELECT count(*) FROM tpch.sf1.{t}')[0][0] for t in TABLES}
    save('source_counts.json', source)
    writes = []
    for trial in range(1, 4):
        # Rotate the order to reduce a systematic first-codec warmup advantage.
        ordered = VARIANTS[trial-1:] + VARIANTS[:trial-1]
        for name, fmt, codec in ordered:
            log(f'Write {name}, trial {trial}')
            timings = create_bronze('bronze_' + name, fmt, codec)
            writes.append({'variant': name, 'format': fmt, 'codec': codec, 'trial': trial,
                           'elapsed_s': sum(t['elapsed_s'] for t in timings), 'tables': timings})
            save('writes.json', writes)
    inventories = []
    for name, _, _ in VARIANTS:
        for table in TABLES:
            item = inventory('bronze_' + name, table)
            assert item['rows'] == source[table]
            assert item['physical_codecs']==[name.split('_')[1].upper()], item['physical_codecs']
            inventories.append(item)
    save('bronze_inventory.json', inventories)
    # A stable view name keeps the medallion SQL readable without copying the source again.
    sql('CREATE SCHEMA IF NOT EXISTS lake.bronze')
    for table in TABLES:
        sql(f'CREATE OR REPLACE VIEW lake.bronze.{table} AS SELECT * FROM lake.bronze_parquet_zstd.{table}')
    quality = {}
    for table, key in [('customer','custkey'), ('orders','orderkey'), ('lineitem','orderkey, linenumber')]:
        null_cond = ' OR '.join(f'{c.strip()} IS NULL' for c in TABLES[table].split(','))
        quality[table] = {'null_rows': sql(f'SELECT count(*) FROM lake.bronze.{table} WHERE {null_cond}')[0][0],
                          'duplicate_keys': sql(f'SELECT count(*) FROM (SELECT {key} FROM lake.bronze.{table} GROUP BY {key} HAVING count(*) > 1)')[0][0]}
    quality['orphan_orders'] = sql('SELECT count(*) FROM lake.bronze.orders o LEFT JOIN lake.bronze.customer c ON o.custkey=c.custkey WHERE c.custkey IS NULL')[0][0]
    quality['orphan_lineitems'] = sql('SELECT count(*) FROM lake.bronze.lineitem l LEFT JOIN lake.bronze.orders o ON l.orderkey=o.orderkey WHERE o.orderkey IS NULL')[0][0]
    save('quality.json', quality)
    assert all(v==0 for item in quality.values() for v in (item.values() if isinstance(item,dict) else [item])), 'Source quality checks failed'
    for ns in ('silver', 'gold'):
        sql(f'CREATE SCHEMA IF NOT EXISTS lake.{ns}')
    for t in ('lake.gold.q18', 'lake.silver.large_orders', 'lake.silver.order_quantities'):
        sql(f'DROP TABLE IF EXISTS {t}')
    sql("CREATE TABLE lake.silver.order_quantities WITH (format='PARQUET') AS SELECT orderkey, cast(sum(quantity) AS decimal(18,2)) AS total_quantity FROM lake.bronze.lineitem GROUP BY orderkey HAVING sum(quantity)>300")
    sql("CREATE TABLE lake.silver.large_orders WITH (format='PARQUET') AS SELECT trim(c.name) AS name,c.custkey,o.orderkey,o.orderdate,cast(o.totalprice AS decimal(15,2)) AS totalprice,q.total_quantity FROM lake.silver.order_quantities q JOIN lake.bronze.orders o ON q.orderkey=o.orderkey JOIN lake.bronze.customer c ON c.custkey=o.custkey")
    sql("CREATE TABLE lake.gold.q18 WITH (format='PARQUET') AS SELECT * FROM lake.silver.large_orders ORDER BY totalprice DESC, orderdate LIMIT 100")
    gold = query('SELECT * FROM lake.gold.q18 ORDER BY totalprice DESC, orderdate')
    assert_q18(gold['rows'])
    save('gold_q18.json', gold)
    with (OUT / 'gold_q18.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle); writer.writerow(gold['columns']); writer.writerows(gold['rows'])
    save('medallion_inventory.json', [inventory(ns,t) for ns,t in [('silver','order_quantities'),('silver','large_orders'),('gold','q18')]])
    log('Medallion and reference verified')


def benchmark():
    measurements = []
    for series in range(1,4):
        ordered = VARIANTS[series-1:] + VARIANTS[:series-1]
        for name, _, _ in ordered:
            log(f'Trino benchmark {name}, series {series}')
            compose('restart', 'trino'); wait_trino()
            for run in range(4):
                result = query(Q18.format(schema='lake.bronze_' + name))
                assert_q18(result['rows'])
                measurements.append({'engine':'Trino','variant':name,'series':series,'run':run,
                                     'state':'first' if run==0 else 'repeat',
                                     'elapsed_s':result['elapsed_s'],'query_id':result['query_id'],
                                     'stats':result['stats'],'query_stats':result['query_stats'],'correct':True})
                save('queries_trino.json', measurements)
                log(f"  run {run}: {result['elapsed_s']:.3f}s")


def metadata():
    log('Small file comparison')
    create_bronze('bronze_smallfiles','PARQUET','ZSTD','1MB')
    small=[inventory('bronze_smallfiles',t) for t in TABLES]
    save('smallfiles_inventory.json',small)
    log(f"Actual small-file count: {sum(x['data_files'] for x in small)}")
    results=[]
    for series in range(1,4):
        order=('bronze_parquet_zstd','bronze_smallfiles')
        if series%2==0: order=tuple(reversed(order))
        for ns in order:
            log(f'Metadata benchmark {ns}, series {series}')
            compose('restart','trino'); wait_trino()
            for run in range(4):
                r=query(Q18.format(schema='lake.'+ns)); assert_q18(r['rows'])
                results.append({'schema':ns,'series':series,'run':run,'state':'first' if run==0 else 'repeat',
                                'elapsed_s':r['elapsed_s'],'query_id':r['query_id'],'query_stats':r['query_stats']})
                save('metadata_queries.json',results)
                log(f"  run {run}: {r['elapsed_s']:.3f}s")


def evolution():
    table='lake.silver.evolution_demo'
    sql(f'DROP TABLE IF EXISTS {table}')
    sql(f"CREATE TABLE {table} WITH (format='PARQUET') AS SELECT * FROM lake.silver.large_orders")
    before=sql(f'SELECT * FROM {table}')
    files_before=sql('SELECT file_path FROM lake.silver."evolution_demo$files"')
    sql(f'ALTER TABLE {table} ADD COLUMN processing_note varchar')
    sql(f'ALTER TABLE {table} RENAME COLUMN processing_note TO etl_note')
    files_after=sql('SELECT file_path FROM lake.silver."evolution_demo$files"')
    assert sorted(files_before)==sorted(files_after)
    after_schema=sql(f'SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity FROM {table}')
    assert canonical(after_schema)==canonical(before)
    assert sql(f'SELECT count(*) FROM {table} WHERE etl_note IS NOT NULL')[0][0]==0
    snapshot=sql('SELECT snapshot_id FROM lake.silver."evolution_demo$snapshots" ORDER BY committed_at DESC LIMIT 1')[0][0]
    orderkey=before[0][2]
    sql(f'DELETE FROM {table} WHERE orderkey={orderkey}')
    after_count=sql(f'SELECT count(*) FROM {table}')[0][0]
    assert after_count==len(before)-1
    history=sql(f'SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity FROM {table} FOR VERSION AS OF {snapshot}')
    assert canonical(history)==canonical(before)
    sql(f'ALTER TABLE {table} EXECUTE rollback_to_snapshot(snapshot_id => {snapshot})')
    restored=sql(f'SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity FROM {table}')
    assert canonical(restored)==canonical(before)
    save('evolution.json',{'table':table,'count_before':len(before),'count_after_delete':after_count,
                           'count_after_restore':len(restored),'snapshot_id':snapshot,'deleted_orderkey':orderkey,
                           'data_files_unchanged_by_schema_evolution':True,'time_travel_matches':True,
                           'schema_evolution_data_matches':True,
                           'restored_matches':True,'columns':sql(f'SHOW COLUMNS FROM {table}'),
                           'snapshots':sql('SELECT committed_at,snapshot_id,parent_id,operation FROM lake.silver."evolution_demo$snapshots"')})
    (ROOT/'sql/snapshot_example.sql').write_text(f'-- Snapshot ID from the recorded run.\nSELECT * FROM {table} FOR VERSION AS OF {snapshot};\nALTER TABLE {table} EXECUTE rollback_to_snapshot(snapshot_id => {snapshot});\n',encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('stage', choices=['setup','load','benchmark','metadata','evolution'])
    stage=parser.parse_args().stage
    globals()[stage]()


if __name__=='__main__':
    main()
