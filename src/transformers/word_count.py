import apache_beam as beam
import re


def run():
    input_file = 'data/data.txt'

    with beam.Pipeline() as p:
        word_counts = (
            p
            | 'Read' >> beam.io.ReadFromText(input_file)
            | 'Split' >> beam.FlatMap(lambda line: re.findall(r'\w+', line.lower()))
            | 'Count' >> beam.combiners.Count.PerElement()
        )


if __name__ == '__main__':
    run()
