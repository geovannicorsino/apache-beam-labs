import apache_beam as beam


class RonaldoSaysSiu(beam.DoFn):
    def process(self, element):
        for char in element:
            yield char


with beam.Pipeline() as p:
    (p | beam.Create(['Siiiiiiiiiiiiiuuuuu!'])
       | beam.ParDo(RonaldoSaysSiu())
       | beam.Map(print)
     )
