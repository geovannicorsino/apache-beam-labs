import apache_beam as beam


class ConcatFn(beam.CombineFn):
    def create_accumulator(self):
        return ""

    def add_input(self, acc, element):
        return acc + ", " + element if acc else element

    def merge_accumulators(self, accumulators):
        return ", ".join(a for a in accumulators if a)

    def extract_output(self, acc):
        return acc


p = beam.Pipeline()

# Combine all values into a single string
(
    p
    | "Create a list of names" >> beam.Create(["Alice", "Bob", "Charlie"])
    | "Combine names into a single string" >> beam.CombineGlobally(ConcatFn())
    | "Print the result" >> beam.Map(print)
)
# ["Alice", "Bob", "Charlie"] → "Alice, Bob, Charlie"


# Combine values by key
result = (
    p
    | beam.Create([
        ("HR", 5000),
        ("HR", 6000),
        ("Engineering", 8000),
        ("Engineering", 9000),
    ])
    | beam.CombinePerKey(sum)
    | beam.Map(print)
)
# ('HR', 11000)
# ('Engineering', 17000)


p.run().wait_until_finish()
