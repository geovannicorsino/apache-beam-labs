import apache_beam as beam


class MultiplyByTenDoFn(beam.DoFn):
    def process(self, element):
        yield element * 10


with beam.Pipeline() as p:
    (p | beam.Create([1, 2, 3, 4, 5])
       | beam.ParDo(MultiplyByTenDoFn())
       | beam.Map(print)
     )
