"""
Count.PerElement | Count occurrences of each unique element in a PCollection.

Applies a frequency count to every distinct value, returning (element, count) tuples.
Use it when you need to aggregate how many times each item appears across the collection.

Example input:
    ['Alice', 'Bob', 'Alice', 'Charlie', 'Ronaldo', 'Bob', 'Alice', 'Messi']
Example output:
    ('Alice', 3)
    ('Bob', 2)
    ('Charlie', 1)
    ('Ronaldo', 1)
    ('Messi', 1)
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        name_appearances = (
            p
            | "Create Names"
            >> beam.Create(["Alice", "Bob", "Alice", "Charlie", "Ronaldo", "Bob", "Alice", "Messi"])
            | "Count appearances" >> beam.combiners.Count.PerElement()
            | "Print results" >> beam.Map(print)
        )
