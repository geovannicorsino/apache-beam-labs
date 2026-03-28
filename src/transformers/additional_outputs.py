from apache_beam import pvalue
import apache_beam as beam


# Define a DoFn that validates employee records and routes valid and invalid records to different outputs
VALID = "valid"
INVALID = "invalid"


class ValidateDoFn(beam.DoFn):
    def process(self, element):
        if element["salary"] > 0 and element["name"]:
            yield element  # principal output (default)
        else:
            yield pvalue.TaggedOutput(INVALID, element)  # secondary output


p = beam.Pipeline()
employees_pc = p | beam.Create([
    {"name": "Alice",   "salary": 5000},
    {"name": "",        "salary": 3000},  # invalid
    {"name": "Charlie", "salary": -100},  # invalid
])

results = (
    employees_pc
    | "Validate" >> beam.ParDo(ValidateDoFn()).with_outputs(INVALID, main="valid")
)

valid_employees = results.valid
invalid_employees = results[INVALID]

valid_employees | "Print valid" >> beam.Map(print)
invalid_employees | "Print invalid" >> beam.Map(print)

# valid:   {"name": "Alice", "salary": 5000}
# invalid: {"name": "", "salary": 3000}
# invalid: {"name": "Charlie", "salary": -100}


p.run().wait_until_finish()
