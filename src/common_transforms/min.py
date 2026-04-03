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
                'Ronaldo', 44
            ),
            (
                'Messi', 91
            ),
        ])
        | "Group goals by player" >> beam.CombinePerKey(min)
        | "Print results" >> beam.Map(print)
    )

# ('Messi', 45)
# ('Ronaldo', 41)
