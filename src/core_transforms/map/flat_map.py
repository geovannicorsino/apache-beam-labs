"""
FlatMap | Apply a function that returns an iterable and flatten the results into a single PCollection.

Unlike Map (one-to-one), FlatMap allows each element to produce zero or more output elements.
Use it as a concise alternative to ParDo when the expansion logic fits in a single generator function.

Example input:
    ['{"user": "A", "purchases": ["item1", "item2"]}', '{"user": "B", "purchases": ["item3"]}']
Example output:
    {'user': 'A', 'item': 'item1'}
    {'user': 'A', 'item': 'item2'}
    {'user': 'B', 'item': 'item3'}
"""
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

