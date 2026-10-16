"""One fresh Spark application per configuration and series; collect full Q18 output."""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession

ROOT=Path('/lab')
parser=argparse.ArgumentParser()
parser.add_argument('--variant', default='parquet_zstd')
parser.add_argument('--series', type=int, default=1)
args=parser.parse_args()
assert args.variant in ['parquet_gzip','parquet_zstd','orc_snappy','orc_zstd']
spark=SparkSession.builder.appName('practice1-q18-'+args.variant).getOrCreate()
spark.sparkContext.setLogLevel('WARN')
query=(ROOT/'sql/q18.sql').read_text().format(schema='lake.bronze_'+args.variant)
expected=json.loads((ROOT/'results/reference_q18.json').read_text())['rows']

def canonical(rows):
    return sorted([[str(x) if i==3 else round(float(x),6) if i in (4,5) else x
                    for i,x in enumerate(row)] for row in rows],key=lambda x:x[2])

results=[]
for run in range(4):
    started_at=datetime.now(timezone.utc).isoformat(timespec='seconds')
    started=time.perf_counter()
    rows=[list(row) for row in spark.sql(query).collect()]
    elapsed=time.perf_counter()-started
    assert canonical(rows)==canonical(expected), 'Spark Q18 differs from tpch.sf1'
    results.append({'engine':'Spark','variant':args.variant,'series':args.series,'run':run,
                    'state':'first' if run==0 else 'repeat','elapsed_s':elapsed,
                    'correct':True,'rows':len(rows),'started_at':started_at,
                    'spark_version':spark.version,'application_id':spark.sparkContext.applicationId})
    print('MEASUREMENT '+json.dumps(results[-1]),flush=True)
    (ROOT/f'results/queries_spark_{args.variant}_{args.series}.json').write_text(json.dumps(results,indent=2)+'\n')

if args.variant=='parquet_zstd' and args.series==1:
    bronze={}
    for table in ('customer','orders','lineitem'):
        name='lake.bronze_parquet_zstd.'+table
        frame=spark.table(name)
        bronze[table]={'count':frame.count(),'schema':frame.schema.jsonValue(),
                       'sample':[r.asDict() for r in frame.limit(5).collect()]}
        print('BRONZE '+table+' '+json.dumps(bronze[table],default=str),flush=True)
    (ROOT/'results/spark_bronze.json').write_text(json.dumps(bronze,indent=2,default=str)+'\n')
spark.stop()
