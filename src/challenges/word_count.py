"""
Word Count | Count the frequency of every word in a text file.

Reads lines from a file, tokenises each line into lowercase words with a regex,
then counts how many times each word appears across the entire corpus.
This is the classic "Hello, World" of distributed data processing.

Example input (data/data.txt):
    "Hello world hello beam"
Example output:
    ('hello', 2)
    ('world', 1)
    ('beam', 1)
"""
import apache_beam as beam
import re


INPUT_FILE = 'data/data.txt'

if __name__ == '__main__':
    with beam.Pipeline() as p:
        word_counts = (
            p
            | 'Read' >> beam.io.ReadFromText(INPUT_FILE)
            | 'Split' >> beam.FlatMap(lambda line: re.findall(r'\w+', line.lower()))
            | 'Count' >> beam.combiners.Count.PerElement()
            | 'Print' >> beam.Map(print)
        )
