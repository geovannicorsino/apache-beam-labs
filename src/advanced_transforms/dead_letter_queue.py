"""
Dead Letter Queue (DLQ) | Route invalid elements to a separate error output.

In production pipelines, a subset of elements may fail parsing or validation.
Instead of letting a single bad record crash the entire pipeline, the DLQ
pattern routes those records to a side output for inspection or reprocessing
— the main pipeline continues uninterrupted.

This pattern builds on TaggedOutput (see also: additional_outputs.py).

Steps:
  1. A DoFn attempts to parse/validate each element.
  2. Valid records are yielded with the VALID tag.
  3. Invalid records are yielded with the DEAD_LETTER tag, carrying the raw
     data and the error message for debugging/reprocessing.

Example input:
    '{"order_id": "001", "amount": 99.5}'  <- valid JSON
    'this is not json at all'              <- JSON parse error → dead letter
    '{"order_id": "003", "amount": 210.0}' <- valid
    '{"order_id": "004"}'                  <- missing 'amount' → dead letter
    '{"amount": 55.0}'                     <- missing 'order_id' → dead letter

Example output (valid):
    {'order_id': '001', 'amount': 99.5}
    {'order_id': '003', 'amount': 210.0}

Example output (dead letter):
    DLQ | raw: 'this is not json at all'  | error: ...
    DLQ | raw: '{"order_id": "004"}'      | error: 'amount'
    DLQ | raw: '{"amount": 55.0}'         | error: 'order_id'
"""
import json

import apache_beam as beam
from apache_beam import pvalue

VALID = "valid"
DEAD_LETTER = "dead_letter"


class ParseAndValidateDoFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            # Validate that all required fields are present
            _ = record["order_id"]
            _ = record["amount"]
            yield pvalue.TaggedOutput(VALID, record)
        except (json.JSONDecodeError, KeyError) as e:
            yield pvalue.TaggedOutput(DEAD_LETTER, {
                "raw": element,
                "error": str(e),
            })


if __name__ == '__main__':
    raw_events = [
        '{"order_id": "001", "amount": 99.5}',
        'this is not json at all',
        '{"order_id": "003", "amount": 210.0}',
        '{"order_id": "004"}',   # missing 'amount'
        '{"amount": 55.0}',      # missing 'order_id'
    ]

    with beam.Pipeline() as p:
        results = (
            p
            | "Read" >> beam.Create(raw_events)
            | "Parse" >> beam.ParDo(ParseAndValidateDoFn()).with_outputs(
                VALID, DEAD_LETTER
            )
        )

        valid = results[VALID]
        dead_letter = results[DEAD_LETTER]

        valid | "Print Valid" >> beam.Map(
            lambda x: print(f"VALID: {x}")
        )

        dead_letter | "Print DLQ" >> beam.Map(
            lambda x: print(
                f"INVALID: DLQ | raw: {x['raw']!r:<45} | error: {x['error']}")
        )
