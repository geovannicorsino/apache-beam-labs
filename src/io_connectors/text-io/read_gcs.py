"""
ReadFromText (GCS) | Read lines from a text file stored in Google Cloud Storage.

Works identically to reading a local file — the only difference is the gs:// URI.
Beam handles authentication via Application Default Credentials (ADC).
Each line of the file becomes one element in the PCollection.

Example input (gs://bucket/data/test/text.txt):
    Alice
    Bob
    Charlie
Example output:
    Alice
    Bob
    Charlie
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        names = (
            p
            | "Read from Text File" >> beam.io.ReadFromText("gs://geo-test-labs/data/test/text.txt")
            | "Print output" >> beam.Map(print)
        )
