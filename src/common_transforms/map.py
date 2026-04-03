import apache_beam as beam


def print_element(element):
    print(element)
    return element


with beam.Pipeline() as p:
    names = (
        p
        | "Create a list of names" >> beam.Create([("Alice", "Smith"), ("Bob", "Johnson"), ("Charlie", "Brown")])
        | "Transform Names" >> beam.Map(lambda name: f"Hello, {name[0]} {name[1]}!")
        | "Print" >> beam.Map(print_element)
    )

    final = (
        names
        | "Convert to uppercase" >> beam.Map(lambda x: x.upper())
        | "Print uppercase" >> beam.Map(print)
    )
