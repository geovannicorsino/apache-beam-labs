"""
ReadFromAvro | Read records from an Avro file into a PCollection of dicts.

The schema is embedded in the Avro file itself, so no schema definition is needed
on read — each record is automatically deserialized into a Python dict.
Run write_avro.py first to generate the input file.

Example input (data/test/people.avro):
    binary Avro file with Person records
Example output:
    {'id': 1, 'name': 'Alice', 'age': 30}
    {'id': 2, 'name': 'Bob',   'age': 25}
    {'id': 3, 'name': 'Carol', 'age': 35}
"""

import apache_beam as beam
from apache_beam.io import ReadFromAvro

if __name__ == "__main__":
    with beam.Pipeline() as p:
        (p | "ReadFromAvro" >> ReadFromAvro("data/test/people.avro") | "Print" >> beam.Map(print))
