"""
WriteToText | Write a PCollection of strings to one or more text files on disk.

Each element becomes a line in the output file(s). num_shards controls how many
part files are produced; setting it to 1 writes everything to a single file.
Use it for local development, debugging, or whenever a flat text file is the target sink.

Example input:
    ['Alice', 'Bob', 'Charlie']
Example output (./data/test/text-00000-of-00001):
    Alice
    Bob
    Charlie
"""
import apache_beam as beam

p1 = beam.Pipeline()


names = (
    p1
    | "Create a list of names" >> beam.Create(["Alice", "Bob", "Charlie"])
    | "Write to Text File" >> beam.io.WriteToText("./data/test/text", num_shards=1)
)

p1.run().wait_until_finish()
