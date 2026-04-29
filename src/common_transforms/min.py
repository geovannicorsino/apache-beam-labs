"""
CombinePerKey(min) | Find the minimum value for each key in a PCollection.

Reduces all values associated with the same key to the single lowest value.
Use it to find the worst-case or baseline record (lowest score, cheapest price, etc.) per group.

Example input:
    [('Messi', 45), ('Ronaldo', 46), ('Ronaldo', 41), ('Ronaldo', 44), ('Messi', 91)]
Example output:
    ('Messi', 45)
    ('Ronaldo', 41)
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        results = (
            p
            | "Create players with goals per season"
            >> beam.Create(
                [
                    ("Messi", 45),
                    ("Ronaldo", 46),
                    ("Ronaldo", 41),
                    ("Ronaldo", 44),
                    ("Messi", 91),
                ]
            )
            | "Group goals by player" >> beam.CombinePerKey(min)
            | "Print results" >> beam.Map(print)
        )
