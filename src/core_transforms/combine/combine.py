import apache_beam as beam


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

# Output: 15
