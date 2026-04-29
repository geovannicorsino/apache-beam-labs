"""
GroupByKey | Collect all values that share the same key into a single (key, iterable) pair.

After grouping, each key maps to an iterable of all its associated values, which downstream
steps can then aggregate or process together.
Use it when you need to consolidate scattered key-value pairs before applying a per-group operation.

Example input:
    [('banana', 2), ('apple', 4), ('lemon', 3), ('Apple', 1), ('Banana', 5), ('Lemon', 2)]
Example output:
    ('banana', 7)
    ('apple', 5)
    ('lemon', 5)
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        input = p | "Fruits" >> beam.Create(
            [("banana", 2), ("apple", 4), ("lemon", 3), ("Apple", 1), ("Banana", 5), ("Lemon", 2)]
        )

        ret = (
            input
            | "Lowercase keys" >> beam.Map(lambda x: (x[0].lower(), x[1]))
            | "GroupByKey" >> beam.GroupByKey()
            | "Sum values" >> beam.Map(lambda x: (x[0], sum(x[1])))
            | "Print results" >> beam.Map(print)
        )
