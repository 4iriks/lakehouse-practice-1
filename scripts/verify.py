"""Verify full Bronze content and record storage and service evidence."""
import json
import subprocess
from lab import TABLES, VARIANTS, OUT, inventory, log, save, sql, s3, wait_trino

wait_trino()
checks = []
for table, columns in TABLES.items():
    expression = f'count(*), to_hex(checksum(ROW({columns})))'
    expected = sql(f'SELECT {expression} FROM tpch.sf1.{table}')[0]
    selected = [x.strip() for x in columns.split(',')]
    expected_types = {row[0]: row[1].split('(')[0] for row in sql(f'SHOW COLUMNS FROM tpch.sf1.{table}')
                      if row[0] in selected}
    for name in [variant[0] for variant in VARIANTS] + ['smallfiles']:
        actual = sql(f'SELECT {expression} FROM lake.bronze_{name}.{table}')[0]
        assert actual == expected, (name, table, expected, actual)
        actual_columns = sql(f'SHOW COLUMNS FROM lake.bronze_{name}.{table}')
        actual_types = {row[0]: row[1].split('(')[0] for row in actual_columns}
        assert actual_types == expected_types, (name, table, expected_types, actual_types)
        checks.append({'variant': name, 'table': table, 'rows': actual[0],
                       'checksum': actual[1], 'source_checksum': expected[1], 'matches': True,
                       'types_match': True, 'source_types': expected_types, 'columns': actual_columns})
save('bronze_checksums.json', checks)

all_tables = []
for namespace in sql('SHOW SCHEMAS FROM lake'):
    ns = namespace[0]
    if ns.startswith('bronze_') or ns in ('silver', 'gold'):
        for row in sql(f'SHOW TABLES FROM lake.{ns}'):
            all_tables.append(inventory(ns, row[0]))
save('final_inventory.json', all_tables)
objects = []
for page in s3().get_paginator('list_objects_v2').paginate(Bucket='warehouse'):
    objects.extend(page.get('Contents', []))
active = sum(x['active_data_bytes'] for x in all_tables)
retained = sum(x['retained_data_bytes'] for x in all_tables)
metadata = sum(x['metadata_bytes'] for x in all_tables)
total = sum(x['Size'] for x in objects)
save('storage_audit.json', {'bucket': 'warehouse', 'objects': len(objects), 'total_bytes': total,
                           'active_data_bytes': active, 'retained_data_bytes_current_tables': retained,
                           'metadata_bytes_current_tables': metadata,
                           'other_bytes_including_dropped_trials': total-active-retained-metadata})

commands = {
    'services.txt': ['docker', 'compose', 'ps', '-a'],
    'images.json': ['docker', 'image', 'inspect', '--format', '{{json .}}',
                    'postgres:16.10-bookworm', 'quay.io/lakekeeper/catalog:v0.9.5',
                    'trinodb/trino:476', 'apache/spark:3.5.6',
                    'lakehouse-practice-1-minio:2025-09-07'],
}
for filename, command in commands.items():
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', check=True)
    if filename == 'images.json':
        images = [json.loads(line) for line in result.stdout.splitlines()]
        save(filename, [{'Id': x['Id'], 'RepoTags': x['RepoTags'], 'RepoDigests': x['RepoDigests'],
                         'Created': x['Created'], 'Architecture': x['Architecture'],
                         'Os': x['Os']} for x in images])
    else:
        output = '\n'.join(line.rstrip() for line in result.stdout.splitlines()) + '\n'
        (OUT/filename).write_text(output, encoding='utf-8')
log('Full Bronze checksums, types, final storage and service evidence verified')
