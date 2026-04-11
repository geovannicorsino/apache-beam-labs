"""
Flatten | Merge multiple PCollections of the same type into one PCollection.

Acts as the inverse of branching: multiple independent streams are reunited into a single collection.
Use it after processing branches separately when you need to apply a unified downstream transform.

Example input:
    hr_team:          [{'name': 'Alice', ...}, {'name': 'Charlie', ...}]
    engineering_team: [{'name': 'Bob', ...}]
Example output:
    {'name': 'Alice', 'department': 'HR'}
    {'name': 'Charlie', 'department': 'HR'}
    {'name': 'Bob', 'department': 'Engineering'}
"""
import apache_beam as beam


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
        )

        engineering_team = (
            input_employees
            | "Filter Engineering Employees" >> beam.Filter(lambda emp: emp["department"] == "Engineering")
        )

        all_employees = (
            (hr_team, engineering_team)
            | "Reunite Employees" >> beam.Flatten()
            | "Print All Employees" >> beam.Map(print)
        )
