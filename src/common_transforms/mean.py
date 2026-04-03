import apache_beam as beam

with beam.Pipeline() as p:
    final_mean = (
        p
        | 'Create number' >> beam.Create([1, 2, 3, 4, 5])
        | 'Calculate mean' >> beam.combiners.Mean.Globally()
        | 'Print result' >> beam.Map(print)
    )

# 3.0
