import apache_beam as beam


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

# Output:
# {'name': 'Alice', 'department': 'HR'}
# {'name': 'Charlie', 'department': 'HR'}
# {'name': 'Bob', 'department': 'Engineering'}
