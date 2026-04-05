import apache_beam as beam
from apache_beam.pvalue import TaggedOutput


class RouteRecords(beam.DoFn):
    def process(self, element):
        try:
            if element.get("value") is None:
                yield TaggedOutput("dead_letter", element)
            elif element["value"] > 0:
                yield TaggedOutput("valid", element)
            else:
                yield TaggedOutput("invalid", element)
        except Exception as e:
            yield TaggedOutput("dead_letter", {**element, "_error": str(e)})


with beam.Pipeline() as p:
    records = p | beam.Create([
        {"id": 1, "value": 10},
        {"id": 2, "value": -5},
        {"id": 3, "value": None},
    ])

    results = (
        records
        | "Route" >> beam.ParDo(RouteRecords()).with_outputs(
            "invalid",
            "dead_letter",
            main="valid"
        )
    )

    results["valid"]       | "Write Valid"       >> beam.Map(lambda x: print(f"Write Valid: {x}"))
    results["invalid"]     | "Write Invalid"     >> beam.Map(lambda x: print(f"Write Invalid: {x}"))
    results["dead_letter"] | "Write Dead Letter" >> beam.Map(lambda x: print(f"Write Dead Letter: {x}"))

# Output: 
# Write Invalid: {'id': 2, 'value': -5}
# Write Dead Letter: {'id': 3, 'value': None}