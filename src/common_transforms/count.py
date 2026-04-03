import apache_beam as beam

with beam.Pipeline() as p:
    name_appearances = (
        p |
        'Create Names' >> beam.Create([
            'Alice',
            'Bob',
            'Alice',
            'Charlie',
            'Ronaldo',
            'Bob',
            'Alice',
            'Messi'
        ])
        | 'Count appearances' >> beam.combiners.Count.PerElement()
        | 'Print results' >> beam.Map(print)
    )

# ('Messi', 1)
# ('Alice', 3)
# ('Bob', 2)
# ('Charlie', 1)
# ('Ronaldo', 1)