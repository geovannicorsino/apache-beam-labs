import apache_beam as beam
import re


INPUT_FILE = 'data/data.txt'

with beam.Pipeline() as p:
    word_counts = (
        p
        | 'Read' >> beam.io.ReadFromText(INPUT_FILE)
        | 'Split' >> beam.FlatMap(lambda line: re.findall(r'\w+', line.lower()))
        | 'Count' >> beam.combiners.Count.PerElement()
    )
