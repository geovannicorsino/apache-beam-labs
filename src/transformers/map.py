import apache_beam as beam

p1 = beam.Pipeline()


def print_element(element):
    print(element)
    return element


names = (
    p1
    | "Create a list of names" >> beam.Create([("Alice", "Smith"), ("Bob", "Johnson"), ("Charlie", "Brown")])
    | "Transform Names" >> beam.Map(lambda name: f"Hello, {name[0]} {name[1]}!")
    | "Print" >> beam.Map(print_element)
)


final = (names
         | "Convert to uppercase" >> beam.Map(lambda x: x.upper())
         | "Print uppercase" >> beam.Map(print)
         )

p1.run().wait_until_finish()
