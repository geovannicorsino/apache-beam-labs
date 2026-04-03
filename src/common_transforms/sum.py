import apache_beam as beam

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

# ('Messi', 2)
# ('Ronaldo', 6)