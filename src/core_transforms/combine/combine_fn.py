import apache_beam as beam


class SumEvenFn(beam.CombineFn):
    def create_accumulator(self):
        return 0

    def add_input(self, accumulator, element):
        return accumulator + (element if element % 2 == 0 else 0)

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator


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
        | "Combine even numbers" >> beam.CombineGlobally(SumEvenFn())
        | "Print results" >> beam.Map(print)
    )

# 6
