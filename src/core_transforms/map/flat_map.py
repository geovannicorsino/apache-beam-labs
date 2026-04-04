import apache_beam as beam
import json

events = [
    '{"user": "A", "purchases": ["item1", "item2"]}',
    '{"user": "B", "purchases": ["item3"]}',
]


def explode_purchases(record):
    data = json.loads(record)
    for item in data["purchases"]:
        yield {"user": data["user"], "item": item}


with beam.Pipeline() as p:
    (
        p
        | "Create Events" >> beam.Create(events)
        | "Explode Purchases" >> beam.FlatMap(explode_purchases)
        | "Print Results" >> beam.Map(print)
    )

# {"user": "A", "item": "item1"}
# {"user": "A", "item": "item2"}
# {"user": "B", "item": "item3"}
