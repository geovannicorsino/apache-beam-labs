"""
CombineGlobally | Reduce an entire PCollection to a single value using a combining function.

Applies an associative and commutative function across all elements, producing one output.
Use it for global aggregations such as total sum, count, or maximum across the whole dataset.

Example input:
    [1, 2, 3, 4, 5]
Example output:
    15
"""
import apache_beam as beam

if __name__ == '__main__':
    with beam.Pipeline() as p:
        results = (
            p
            | 'Numbers' >> beam.Create([
                1,
                2,
                3,
                4,
                5
            ])
            | "Combine all numbers" >> beam.CombineGlobally(sum)
            | "Print results" >> beam.Map(print)
        )
