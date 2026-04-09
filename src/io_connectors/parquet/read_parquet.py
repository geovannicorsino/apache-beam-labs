"""
ReadFromParquet | Read records from a Parquet file into a PCollection of dicts.

By default reads all columns. Use the columns parameter to select only the columns
you need — since Parquet is columnar, this avoids loading unnecessary data and is
significantly faster on large files.
Run write_parquet.py first to generate the input file.

Example input (data/test/people.parquet):
    columnar Parquet file with Person records
Example output:
    {'id': 1, 'name': 'Alice', 'age': 30}
    {'id': 2, 'name': 'Bob',   'age': 25}
    {'id': 3, 'name': 'Carol', 'age': 35}
"""
import apache_beam as beam
from apache_beam.io import ReadFromParquet

with beam.Pipeline() as p:
    (
        p
        | 'ReadFromParquet' >> ReadFromParquet(
            'data/test/people.parquet',            
        )
        | 'Print' >> beam.Map(print)
    )
