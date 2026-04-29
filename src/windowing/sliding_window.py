"""
Sliding Windowing | Rolling Average Latency

Window size:   2 minutes  (how far back we look)
Window period: 30 seconds (how often we emit a new result)

Every 30s, emits the average latency of the last 2 minutes.
Example input:
    {"latency_ms": 142}
    {"latency_ms": 543}

Example output:
  [SlidingWindow] avg_latency: 145.0ms (last 2 min)
  [SlidingWindow] avg_latency: 132.0ms (last 2 min)
"""

import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import SlidingWindows

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"

WINDOW_SIZE_SECONDS = 120
WINDOW_PERIOD_SECONDS = 30


class AvgLatencyCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return {"total_latency": 0.0, "count": 0}

    def add_input(self, acc, element):
        acc["total_latency"] += element["latency_ms"]
        acc["count"] += 1
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["total_latency"] += acc["total_latency"]
            merged["count"] += acc["count"]
        return merged

    def extract_output(self, acc):
        if acc["count"] == 0:
            return None
        return round(acc["total_latency"] / acc["count"], 1)


def parse_message(message):
    return json.loads(message.decode("utf-8"))


def print_avg(element):
    if element is not None:
        print(f"[SlidingWindow] avg_latency: {element}ms (last 2 min)")
    return element


if __name__ == "__main__":
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_message)
            | "Window"
            >> beam.WindowInto(SlidingWindows(WINDOW_SIZE_SECONDS, WINDOW_PERIOD_SECONDS))
            | "Combine" >> beam.CombineGlobally(AvgLatencyCombineFn()).without_defaults()
            | "Print" >> beam.Map(print_avg)
        )
