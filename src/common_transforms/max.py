"""
CombinePerKey(max) | Find the maximum value for each key in a PCollection.

Reduces all values associated with the same key to the single highest value.
Use it to find peak records (best score, highest sale, etc.) per group.

Example input:
    [('Messi', 45), ('Ronaldo', 46), ('Ronaldo', 41), ('Ronaldo', 69), ('Messi', 91)]
Example output:
    ('Messi', 91)
    ('Ronaldo', 69)
"""
import apache_beam as beam

with beam.Pipeline() as p:
    results = (
        p
        | 'Create players with goals per season' >> beam.Create([
            (
                'Messi', 45
            ),
            (
                'Ronaldo', 46),
            (
                'Ronaldo', 41
            ),
            (
                'Ronaldo', 69
            ),
            (
                'Messi', 91
            ),
        ])
        | "Group goals by player" >> beam.CombinePerKey(max)
        | "Print results" >> beam.Map(print)
    )

