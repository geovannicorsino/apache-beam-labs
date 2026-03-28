import apache_beam as beam


class EnrichDoFn(beam.DoFn):
    def process(self, element, avg_salary):  # avg_salary chega como parâmetro
        element["above_avg"] = element["salary"] > avg_salary
        yield element

# Output:
# {"name": "Alice", "salary": 5000, "above_avg": False}
# {"name": "Bob",   "salary": 8000, "above_avg": True}
# {"name": "Charlie","salary": 6000,"above_avg": False}


employees = [
    {"name": "Alice", "department": "HR", "salary": 5000},
    {"name": "Bob", "department": "Engineering", "salary": 8000},
    {"name": "Charlie", "department": "HR", "salary": 6000},
]

with beam.Pipeline() as p:

    employees_pc = p | "Employees" >> beam.Create(employees)

    # calcula média salarial — vai virar o side input
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
