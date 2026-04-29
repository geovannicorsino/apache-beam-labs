"""
WriteToAvro | Write a PCollection of dicts to an Avro file.

Avro is a row-based binary format with an embedded schema, making it compact and
self-describing. Common in data pipelines as an intermediate format between systems.
The schema is defined as a JSON dict following the Apache Avro specification.

Example input:
    {'id': 1, 'name': 'Alice', 'age': 30}
    {'id': 2, 'name': 'Bob',   'age': 25}
Example output (data/test/people.avro):
    binary file readable by any Avro-compatible tool
"""

import apache_beam as beam
from apache_beam.io import WriteToAvro

SCHEMA = {
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": ["int", "null"]},
    ],
}

if __name__ == "__main__":
    with beam.Pipeline() as p:
        (
            p
            | "Create rows"
            >> beam.Create(
                [
                    {"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25},
                    {"id": 3, "name": "Carol", "age": 35},
                ]
            )
            | "WriteToAvro"
            >> WriteToAvro(
                file_path_prefix="data/test/people",
                schema=SCHEMA,
                file_name_suffix=".avro",
                shard_name_template="",
                num_shards=1,
            )
        )
