"""
Side Input | Inject an auxiliary PCollection into a transform as a read-only parameter.

Side inputs let a DoFn access data computed by another branch of the pipeline at runtime,
without hardcoding values. AsSingleton wraps a single-element PCollection as a scalar value.
Use it when your transform depends on a dynamically computed lookup value or threshold.

Example input:
    employees: [{'name': 'Alice', 'salary': 5000}, {'name': 'Bob', 'salary': 8000}, ...]
    avg_salary (side input): 6333.33
Example output:
    {'name': 'Alice', 'salary': 5000, 'above_avg': False}
    {'name': 'Bob',   'salary': 8000, 'above_avg': True}
    {'name': 'Charlie','salary': 6000,'above_avg': False}
"""

import apache_beam as beam


class EnrichDoFn(beam.DoFn):
    def process(self, element, avg_salary):  # avg_salary chega como parâmetro
        element["above_avg"] = element["salary"] > avg_salary
        yield element


if __name__ == "__main__":
    employees = [
        {"name": "Alice", "department": "HR", "salary": 5000},
        {"name": "Bob", "department": "Engineering", "salary": 8000},
        {"name": "Charlie", "department": "HR", "salary": 6000},
    ]

    with beam.Pipeline() as p:
        employees_pc = p | "Employees" >> beam.Create(employees)

        avg_salary = (
            employees_pc
            | "Extract salary" >> beam.Map(lambda e: e["salary"])
            | "Avg salary" >> beam.CombineGlobally(beam.combiners.MeanCombineFn())
        )

        result = (
            employees_pc
            | "Enrich" >> beam.ParDo(EnrichDoFn(), avg_salary=beam.pvalue.AsSingleton(avg_salary))
            | beam.Map(print)
        )
