"""
Mean.Globally | Compute the arithmetic mean of all elements in a PCollection.

Reduces the entire collection to a single floating-point average value.
Use it when you need a global summary statistic across all records.

Example input:
    [1, 2, 3, 4, 5]
Example output:
    3.0
"""
import apache_beam as beam

with beam.Pipeline() as p:
    final_mean = (
        p
        | 'Create number' >> beam.Create([1, 2, 3, 4, 5])
        | 'Calculate mean' >> beam.combiners.Mean.Globally()
        | 'Print result' >> beam.Map(print)
    )

