"""
ParDo (one-to-many) | Apply a DoFn that emits multiple output elements per input element.

Each call to process() can yield zero or more values, expanding the collection size.
Use it when a single record needs to be "exploded" into several downstream elements,
such as flattening a list field or generating multiple events from one source record.

Example input:
    [
        {'user_id': 'u1', 'tags': ['python', 'beam']},
        {'user_id': 'u2', 'tags': ['spark']},
        {'user_id': 'u3', 'tags': ['beam', 'dataflow', 'gcp']},
    ]
Example output:
    ('u1', 'python')
    ('u1', 'beam')
    ('u2', 'spark')
    ('u3', 'beam')
    ('u3', 'dataflow')
    ('u3', 'gcp')
"""
import apache_beam as beam


class ExpandTagsDoFn(beam.DoFn):
    def process(self, element):
        # Emit one (user_id, tag) pair for every tag the user has
        for tag in element["tags"]:
            yield (element["user_id"], tag)


with beam.Pipeline() as p:
    (
        p
        | "Create Users" >> beam.Create([
            {"user_id": "u1", "tags": ["python", "beam"]},
            {"user_id": "u2", "tags": ["spark"]},
            {"user_id": "u3", "tags": ["beam", "dataflow", "gcp"]},
        ])
        | "Expand Tags" >> beam.ParDo(ExpandTagsDoFn())
        | "Print Results" >> beam.Map(print)
    )

