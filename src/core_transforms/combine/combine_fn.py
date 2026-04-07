"""
CombineFn | Define a custom combining strategy by implementing the four-phase accumulator protocol.

Subclass beam.CombineFn and implement create_accumulator, add_input, merge_accumulators,
and extract_output to express aggregations that cannot be reduced to a single built-in function.
Use it when you need custom logic such as filtering, conditional accumulation, or complex state.

Example input:
    [1, 2, 3, 4, 5]
Example output:
    6   (sum of even numbers: 2 + 4)
"""
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

