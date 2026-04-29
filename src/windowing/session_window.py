"""
Session Windowing | User Session Analytics

Gap duration: 30 seconds (if no events arrive within 30s, the session closes)

Each session emits:
  - user_id
  - pages visited
  - session duration (seconds), derived from window metadata

Example input:
    {"user_id": "alice", "page": "/home"}
    {"user_id": "alice", "page": "/products"}

Example output:
  [Session] alice | pages: ["/home", "/products", "/cart"] | duration: 45s
"""

import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import Sessions

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"

GAP_SECONDS = 30


class SessionCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, acc, element):
        acc.append(element["page"])
        return acc

    def merge_accumulators(self, accumulators):
        merged = []
        for acc in accumulators:
            merged.extend(acc)
        return merged

    def extract_output(self, acc):
        return acc


class PrintSessionDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        user_id, pages = element
        duration = round((window.end - window.start).micros / 1e6, 1)
        print(f"[Session] {user_id} | " f"pages: {pages} | " f"duration: {duration}s")
        yield element


def parse_message(message):
    return json.loads(message.decode("utf-8"))


if __name__ == "__main__":
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_message)
            | "ExtractKV" >> beam.Map(lambda e: (e["user_id"], e))
            | "Window" >> beam.WindowInto(Sessions(GAP_SECONDS))
            | "Combine" >> beam.CombinePerKey(SessionCombineFn())
            | "Print" >> beam.ParDo(PrintSessionDoFn())
        )
