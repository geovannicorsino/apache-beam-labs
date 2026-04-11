"""
CombinePerKey(sum) | Sum numeric values grouped by key.

Aggregates all values associated with the same key using Python's built-in sum.
Use it to produce per-key totals from a PCollection of (key, value) pairs.

Example input:
    [('Messi', 1), ('Ronaldo', 2), ('Ronaldo', 1), ('Ronaldo', 3), ('Messi', 1)]
Example output:
    ('Messi', 2)
    ('Ronaldo', 6)
"""
import apache_beam as beam

if __name__ == '__main__':
    with beam.Pipeline() as p:
        results = (
            p
            | 'Create players goals' >> beam.Create([
                (
                    'Messi', 1
                ),
                (
                    'Ronaldo', 2
                ),
                (
                    'Ronaldo', 1
                ),
                (
                    'Ronaldo', 3
                ),
                (
                    'Messi', 1
                ),
            ])
            | "Group goals by player" >> beam.CombinePerKey(sum)
            | "Print results" >> beam.Map(print)
        )
