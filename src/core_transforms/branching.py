import apache_beam as beam

p = beam.Pipeline()


def print_element(element):
    print(element)
    return element


input_employees = (
    p
    | "Create a list of employees" >> beam.Create([{"name": "Alice", "department": "HR"},
                                                   {"name": "Bob",
                                                    "department": "Engineering"},
                                                   {"name": "Charlie", "department": "HR"}])
)

hr_team = (
    input_employees
    | "Filter HR Employees" >> beam.Filter(lambda emp: emp["department"] == "HR")
    | "Print HR Employees" >> beam.Map(print_element)
)


engineering_team = (
    input_employees
    | "Filter Engineering Employees" >> beam.Filter(lambda emp: emp["department"] == "Engineering")
    | "Print Engineering Employees" >> beam.Map(print_element)
)

all_employees = (
    (hr_team, engineering_team)
    | "Reunite Employees" >> beam.Flatten()
    | "Print All Employees" >> beam.Map(print_element)
)

p.run().wait_until_finish()
