"""
Map | Apply a one-to-one transformation to every element in a PCollection.

Each input element produces exactly one output element; the collection size is unchanged.
Use Map for simple, stateless transformations such as formatting, type conversion, or field
extraction.
Multiple Map steps can be chained to build a sequential transformation pipeline.

Example input:
    [('Alice', 'Smith'), ('Bob', 'Johnson'), ('Charlie', 'Brown')]
Example output:
    Hello, Alice Smith!
    HELLO, ALICE SMITH!
    Hello, Bob Johnson!
    ...
"""
import apache_beam as beam


def print_element(element):
    print(element)
    return element


if __name__ == '__main__':
    with beam.Pipeline() as p:
        names_data = [("Alice", "Smith"), ("Bob", "Johnson"), ("Charlie", "Brown")]
        names = (
            p
            | "Create a list of names" >> beam.Create(names_data)
            | "Transform Names" >> beam.Map(lambda name: f"Hello, {name[0]} {name[1]}!")
            | "Print" >> beam.Map(print_element)
        )

        final = (
            names
            | "Convert to uppercase" >> beam.Map(lambda x: x.upper())
            | "Print uppercase" >> beam.Map(print)
        )
