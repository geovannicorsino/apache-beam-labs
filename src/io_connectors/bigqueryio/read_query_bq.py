"""
ReadFromBigQuery (Query) | Execute a SQL query on BigQuery and read results into a PCollection.

Use this when you need to filter, join, or transform data before it enters the pipeline.
Requires a GCS bucket for temporary storage during query execution.
use_standard_sql=True enables GoogleSQL syntax (recommended over legacy SQL).

Example output:
    {'ID': 1, 'FIRST_NAME': 'Alice'}
    {'ID': 2, 'FIRST_NAME': 'Bob'}
"""
import apache_beam as beam


with beam.Pipeline() as p:
    result = (
        p
        | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
            query='SELECT ID, FIRST_NAME FROM `geovanni-corsino-labs.jaffle_shop.customers` LIMIT 10',
            use_standard_sql=True,
            project='geovanni-corsino-labs',
            # GCS bucket used as temp storage during query export
            gcs_location='gs://geo-test-labs/temp',
        )
        | 'PrintOutput' >> beam.Map(print)
    )
