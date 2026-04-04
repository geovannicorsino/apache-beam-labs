import apache_beam as beam

with beam.Pipeline() as p:
    input = p | 'Fruits' >> beam.Create([
        ("banana", 2),
        ("apple", 4),
        ("lemon", 3),
        ("Apple", 1),
        ("Banana", 5),
        ("Lemon", 2)
    ])

    ret = (
        input
        | "Lowercase keys" >> beam.Map(lambda x: (x[0].lower(), x[1]))
        | "GroupByKey" >> beam.GroupByKey()
        | "Sum values" >> beam.Map(lambda x: (x[0], sum(x[1])))
        | "Print results" >> beam.Map(print)
    )
