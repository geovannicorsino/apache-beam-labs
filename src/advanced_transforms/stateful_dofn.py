"""
Stateful DoFn | Accumulate per-key state between elements.

A regular DoFn is stateless: each element is processed independently with no
memory of previous elements. A Stateful DoFn adds a persistent state cell per
key within a window, surviving across multiple elements with the same key.

Use cases:
  - Deduplication: remember which IDs have already been emitted
  - Rate limiting: cap the number of outputs per key per window
  - Session tracking: update a running state as events arrive

Key Components:
  ReadModifyWriteStateSpec  — a single mutable cell (read → modify → write)
  BagStateSpec              — an append-only collection per key
  beam.DoFn.StateParam      — binds the state cell to a DoFn parameter

Important: Stateful DoFns require keyed data (key, value) pairs and a window.
State is scoped per (key, window) — it resets when a new window begins.

Example scenario: Deduplicate order events — if the same order_id arrives
more than once (e.g. due to retry logic), emit it only the first time.

Example input:
    ('order-1', {'amount': 100.0})
    ('order-2', {'amount': 200.0})
    ('order-1', {'amount': 100.0})  <- duplicate: same key seen before
    ('order-3', {'amount': 150.0})
    ('order-2', {'amount': 200.0})  <- duplicate: same key seen before
Example output:
    ('order-1', {'amount': 100.0})
    ('order-2', {'amount': 200.0})
    ('order-3', {'amount': 150.0})
"""
import apache_beam as beam
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec


class DeduplicateByKeyDoFn(beam.DoFn):
    STATE_SPEC  = ReadModifyWriteStateSpec("num_elements", VarIntCoder())

    def process(self, element, state=beam.DoFn.StateParam(STATE_SPEC )):
        key, value = element

        if state.read() is not None:
            # Key already seen in this window → drop the duplicate
            return

        # First time seeing this key → emit and mark as seen
        state.write(1)
        yield element


with beam.Pipeline() as p:
    (
        p
        | "Events" >> beam.Create([
            ("order-1", {"amount": 100.0}),
            ("order-2", {"amount": 200.0}),
            ("order-1", {"amount": 100.0}),  # duplicate
            ("order-3", {"amount": 150.0}),
            ("order-2", {"amount": 200.0}),  # duplicate
        ])
        | "Deduplicate" >> beam.ParDo(DeduplicateByKeyDoFn())
        | "Print" >> beam.Map(print)
    )
