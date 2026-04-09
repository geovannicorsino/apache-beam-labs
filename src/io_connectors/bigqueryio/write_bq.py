"""
WriteToBigQuery | Write a PCollection of dicts to a BigQuery table.

Each dict becomes a row. The schema defines column names and types.
WRITE_TRUNCATE replaces the table on each run — useful for full refreshes.
CREATE_IF_NEEDED creates the table automatically if it does not exist.

Write methods:
    STREAMING_INSERTS — inserts rows directly via the BQ API (no GCS needed, good for local/dev)
    FILE_LOADS        — exports to GCS first, then loads into BQ (default, better for large batches)

Write dispositions:
    WRITE_TRUNCATE   — truncate and replace the table
    WRITE_APPEND     — append rows to an existing table
    WRITE_EMPTY      — only write if the table is empty, otherwise fail

Create dispositions:
    CREATE_IF_NEEDED — create the table if it does not exist
    CREATE_NEVER     — fail if the table does not exist

Example input:
    {'id': 1, 'name': 'Alice', 'age': 30}
    {'id': 2, 'name': 'Bob',   'age': 25}
Example output (BigQuery table geovanni-corsino-labs:beam_labs.people):
    id | name  | age
    1  | Alice | 30
    2  | Bob   | 25
"""
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

TABLE = 'geovanni-corsino-labs:beam_labs.people'

SCHEMA = {
    'fields': [
        {'name': 'id',   'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING',  'mode': 'REQUIRED'},
        {'name': 'age',  'type': 'INTEGER', 'mode': 'NULLABLE'},
    ]
}

with beam.Pipeline() as p:
    (
        p
        | 'Create rows' >> beam.Create([
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob',   'age': 25},
            {'id': 3, 'name': 'Carol', 'age': 35},
        ])
        | 'WriteToBigQuery' >> WriteToBigQuery(
            table=TABLE,
            schema=SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # STREAMING_INSERTS: writes directly to BQ without needing a GCS temp bucket
            # FILE_LOADS (default): requires custom_gcs_temp_location or --temp_location
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )
