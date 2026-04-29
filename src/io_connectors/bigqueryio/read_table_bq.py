"""
ReadFromBigQuery (Table) | Read all rows from a BigQuery table into a PCollection.

Each row becomes a dict where keys are column names. DIRECT_READ uses the BigQuery
Storage Read API, which is faster than the default export-based method for large tables.

Table format: 'project:dataset.table'

Example output:
    {'id': 1, 'first_name': 'Alice', 'last_name': 'Smith', ...}
    {'id': 2, 'first_name': 'Bob',   'last_name': 'Jones', ...}
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        result = (
            p
            | "ReadFromBigQuery"
            >> beam.io.ReadFromBigQuery(
                table="geovanni-corsino-labs:jaffle_shop.customers",
                # DIRECT_READ uses the Storage Read API — faster for large tables
                # EXPORT (default) exports to GCS first, then reads
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
            )
            | "PrintOutput" >> beam.Map(print)
        )
