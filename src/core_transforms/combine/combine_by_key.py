"""
CombinePerKey | Apply a CombineFn independently to the values of each key in a keyed PCollection.

Works like CombineGlobally but scoped to each key, so every key gets its own aggregated result.
Use it when you need per-group custom aggregations on (key, value) pairs.

Example input:
    [('Ronaldo', 1), ('Messi', 2), ('Messi', 3), ('Neymar', 4), ('Neymar', 5)]
Example output:
    ('Ronaldo', {'even': 0, 'odd': 1})
    ('Messi',   {'even': 2, 'odd': 3})
    ('Neymar',  {'even': 4, 'odd': 5})
"""
import apache_beam as beam


class SumEvenOrOddFn(beam.CombineFn):
    def create_accumulator(self):
        return {"even": 0, "odd": 0}

    def add_input(self, accumulator, element):
        if element % 2 == 0:
            accumulator["even"] += element
        else:
            accumulator["odd"] += element
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {"even": 0, "odd": 0}
        for acc in accumulators:
            merged["even"] += acc["even"]
            merged["odd"] += acc["odd"]
        return merged

    def extract_output(self, accumulator):
        return accumulator


if __name__ == '__main__':
    with beam.Pipeline() as p:
        results = (
            p
            | 'Numbers' >> beam.Create([
                ('Ronaldo', 1),
                ('Messi', 2),
                ('Messi', 3),
                ('Neymar', 4),
                ('Neymar', 5),
            ])
            | "Combine by key" >> beam.CombinePerKey(SumEvenOrOddFn())
            | "Print results" >> beam.Map(print)
        )

