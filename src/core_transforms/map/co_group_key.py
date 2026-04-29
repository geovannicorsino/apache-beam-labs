"""
CoGroupByKey | Join two or more keyed PCollections on a shared key, similar to a SQL JOIN.

Each output element is a (key, {tag: [values]}) pair containing all values from every
input collection that share that key.
Use it to combine related data from separate sources without a database.

Example input:
    employees: [('Alice', 'HR'), ('Bob', 'Engineering'), ('Charlie', 'HR')]
    salaries:  [('Alice', 5000), ('Bob', 8000), ('Charlie', 6000)]
Example output:
    ('Alice',   {'employee': ['HR'],          'salary': [5000]})
    ('Bob',     {'employee': ['Engineering'], 'salary': [8000]})
    ('Charlie', {'employee': ['HR'],          'salary': [6000]})
"""

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

if __name__ == "__main__":
    with beam.Pipeline() as p:
        emp_pc = p | "Employees" >> beam.Create(employees)
        sal_pc = p | "Salaries" >> beam.Create(salaries)

        result = (
            {"employee": emp_pc, "salary": sal_pc}
            | "CoGroup" >> beam.CoGroupByKey()
            | "Print" >> beam.Map(print)
        )
