import apache_beam as beam

employees = [
    ("Alice", "HR"),
    ("Bob", "Engineering"),
    ("Charlie", "HR"),
]

salaries = [
    ("Alice", 5000),
    ("Bob", 8000),
    ("Charlie", 6000),
]

with beam.Pipeline() as p:
    emp_pc = p | "Employees" >> beam.Create(employees)
    sal_pc = p | "Salaries" >> beam.Create(salaries)

    result = (
        {"employee": emp_pc, "salary": sal_pc}
        | "CoGroup" >> beam.CoGroupByKey()
        | "Print" >> beam.Map(print)
    )

# ("Alice",   {"employee": ["HR"],          "salary": [5000]})
# ("Bob",     {"employee": ["Engineering"], "salary": [8000]})
# ("Charlie", {"employee": ["HR"],          "salary": [6000]})
