"""
Branching | Fan a single PCollection out into multiple independent downstream paths.

Any intermediate PCollection can be consumed by more than one transform simultaneously.
Use it to apply different processing logic to the same data without re-reading the source.

Example input:
    [{'name': 'Alice', 'department': 'HR'}, {'name': 'Bob', 'department': 'Engineering'}, ...]
Example output:
    HR branch:          {'name': 'Alice', 'department': 'HR'}
                        {'name': 'Charlie', 'department': 'HR'}
    Engineering branch: {'name': 'Bob', 'department': 'Engineering'}
"""
import apache_beam as beam


def print_element(element):
    print(element)
    return element


if __name__ == '__main__':
    with beam.Pipeline() as p:
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
            | "Filter Engineering Employees" >> beam.Filter(
                lambda emp: emp["department"] == "Engineering"
            )
            | "Print Engineering Employees" >> beam.Map(print_element)
        )
