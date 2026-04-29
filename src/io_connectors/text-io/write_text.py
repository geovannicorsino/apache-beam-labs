"""
WriteToText | Write a PCollection of strings to one or more text files on disk.

Each element becomes a line in the output file(s). num_shards controls how many
part files are produced; setting it to 1 writes everything to a single file.
Use it for local development, debugging, or whenever a flat text file is the target sink.

Example input:
    ['Alice', 'Bob', 'Charlie']
Example output (./data/test/names.txt):
    Alice
    Bob
    Charlie
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        names = (
            p
            | "Create a list of names" >> beam.Create(["Alice", "Bob", "Charlie"])
            | "Write to Text File"
            >> beam.io.WriteToText(
                "./data/test/names",
                file_name_suffix=".txt",
                shard_name_template="",
                num_shards=1,
            )
        )
