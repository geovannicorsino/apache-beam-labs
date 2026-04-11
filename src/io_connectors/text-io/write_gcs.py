"""
WriteToText (GCS) | Write a PCollection of strings to a text file on Google Cloud Storage.

Each element becomes a line in the output file. By default, Beam appends a shard suffix
to the filename (e.g. text-00000-of-00001). Use shard_name_template="" to disable it
and file_name_suffix to control the file extension.

Example input:
    ['Alice', 'Bob', 'Charlie']
Example output (gs://bucket/data/test/names.txt):
    Alice
    Bob
    Charlie
"""
import apache_beam as beam


if __name__ == '__main__':
    with beam.Pipeline() as p:
        names = (
            p
            | "Create a list of names" >> beam.Create(["Alice", "Bob", "Charlie"])
            | "Write to GCS" >> beam.io.WriteToText(
                "gs://geo-test-labs/data/test/names",
                file_name_suffix=".txt",
                shard_name_template="",
                num_shards=1,
            )
        )
