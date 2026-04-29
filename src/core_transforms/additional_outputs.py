"""
Additional Outputs (Tagged Outputs) | Route elements to multiple named output PCollections
from a single DoFn.

Use TaggedOutput inside process() to label each emitted element with a tag, then access each
named stream via results[tag] after calling with_outputs().
Use it to implement dead-letter queues, validation splits, or any multi-path routing logic.

Example input:
    [{'id': 1, 'value': 10}, {'id': 2, 'value': -5}, {'id': 3, 'value': None}]
Example output:
    valid:        {'id': 1, 'value': 10}
    invalid:      {'id': 2, 'value': -5}
    dead_letter:  {'id': 3, 'value': None}
"""
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


if __name__ == '__main__':
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

        results["valid"]       | "Write Valid"       >> beam.Map(lambda x: print(f"Valid: {x}"))
        results["invalid"]     | "Write Invalid"     >> beam.Map(lambda x: print(f"Invalid: {x}"))
        results["dead_letter"] | "Write Dead Letter" >> beam.Map(lambda x: print(f"DLQ: {x}"))
