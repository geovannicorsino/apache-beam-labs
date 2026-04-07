"""
ParDo (one-to-one) | Apply a DoFn that emits exactly one output per input element.

ParDo is the general-purpose parallel processing primitive in Beam.
In the one-to-one variant each call to process() yields a single value, making it
functionally equivalent to Map but with the full power of a class-based DoFn
(setup/teardown hooks, state, timers, etc.).

Example input:
    [1, 2, 3, 4, 5]
Example output:
    10
    20
    30
    40
    50
"""
import apache_beam as beam


class MultiplyByTenDoFn(beam.DoFn):
    def process(self, element):
        yield element * 10


with beam.Pipeline() as p:
    (p | beam.Create([1, 2, 3, 4, 5])
       | beam.ParDo(MultiplyByTenDoFn())
       | beam.Map(print)
     )

