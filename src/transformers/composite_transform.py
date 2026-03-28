import apache_beam as beam


class ProcessEmployees(beam.PTransform):
    def __init__(self, tax_rate=0.27):
        super().__init__()
        self.tax_rate = tax_rate

    def expand(self, pcollection):
        return (
            pcollection
            | "Normalize" >> beam.Map(lambda e: {**e, "name": e["name"].strip().upper()})
            | "Filter" >> beam.Filter(lambda e: e["salary"] > 0)
            | "Enrich" >> beam.Map(lambda e: {**e, "tax": round(e["salary"] * self.tax_rate, 2)})
        )


employees = [
    {"name": "Alice",   "department": "HR",          "salary": 5000},
    {"name": "  Bob ",  "department": "HR",          "salary": 6000},
    {"name": "Charlie", "department": "HR",          "salary": -100},
]

contractors = [
    {"name": "Dave",  "department": "Engineering", "salary": 8000},
    {"name": "Eve  ", "department": "Engineering", "salary": 9000},
    {"name": "",      "department": "Engineering", "salary": 0},
]


p = beam.Pipeline()

rh_processed = (
    p
    | "RH Employees" >> beam.Create(employees)
    | "Process RH" >> ProcessEmployees(tax_rate=0.27)
    | "Print RH" >> beam.Map(print)
)

eng_processed = (
    p
    | "Eng Employees" >> beam.Create(contractors)
    | "Process Eng" >> ProcessEmployees(tax_rate=0.20)
    | "Print Eng" >> beam.Map(print)
)

p.run().wait_until_finish()

# {'name': 'ALICE',   'department': 'HR',          'salary': 5000, 'tax': 1350.0}
# {'name': 'BOB',     'department': 'HR',          'salary': 6000, 'tax': 1620.0}
# {'name': 'DAVE',    'department': 'Engineering', 'salary': 8000, 'tax': 1600.0}
# {'name': 'EVE',     'department': 'Engineering', 'salary': 9000, 'tax': 1800.0}
