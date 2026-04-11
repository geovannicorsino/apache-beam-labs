"""
WriteToParquet | Write a PCollection of dicts to a Parquet file.

Parquet is a columnar binary format optimized for analytical queries — reading one
column does not require loading others. It is the standard format for data lakes
on GCS/S3 and integrates natively with BigQuery external tables.
The schema is defined using PyArrow.

Example input:
    {'id': 1, 'name': 'Alice', 'age': 30}
    {'id': 2, 'name': 'Bob',   'age': 25}
Example output (data/test/people.parquet):
    columnar binary file readable by BigQuery, Spark, DuckDB, pandas, etc.
"""
import apache_beam as beam
import pyarrow
from apache_beam.io import WriteToParquet

SCHEMA = pyarrow.schema([
    ('id',   pyarrow.int32()),
    ('name', pyarrow.string()),
    ('age',  pyarrow.int32()),
])

if __name__ == '__main__':
    with beam.Pipeline() as p:
        (
            p
            | 'Create rows' >> beam.Create([
                {'id': 1, 'name': 'Alice', 'age': 30},
                {'id': 2, 'name': 'Bob',   'age': 25},
                {'id': 3, 'name': 'Carol', 'age': 35},
            ])
            | 'WriteToParquet' >> WriteToParquet(
                file_path_prefix='data/test/people',
                schema=SCHEMA,
                file_name_suffix='.parquet',
                shard_name_template='',
                num_shards=1,
            )
        )
