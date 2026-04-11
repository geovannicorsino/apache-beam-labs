'''
Fixed Windowing | System Metrics per Minute

Each 60-second window emits:
  - total requests
  - average latency (ms)
  - error count

Example input:
    {"latency_ms": 142, "status": 200}
    {"latency_ms": 543, "status": 500}

Example output:
  [14:00–14:01] requests: 3 | avg_latency: 145.0ms | errors: 1
  [14:01–14:02] requests: 2 | avg_latency: 112.5ms | errors: 0
'''
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"

WINDOW_SIZE_SECONDS = 60


class MetricsCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return {"requests": 0, "total_latency": 0.0, "errors": 0}

    def add_input(self, acc, element):
        acc["requests"] += 1
        acc["total_latency"] += element["latency_ms"]
        acc["errors"] += 1 if element["status"] >= 500 else 0
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["requests"] += acc["requests"]
            merged["total_latency"] += acc["total_latency"]
            merged["errors"] += acc["errors"]
        return merged

    def extract_output(self, acc):
        avg_latency = acc["total_latency"] / \
            acc["requests"] if acc["requests"] else 0
        return {
            "requests": acc["requests"],
            "avg_latency": round(avg_latency, 1),
            "errors": acc["errors"],
        }


def parse_message(message):
    return json.loads(message.decode("utf-8"))


def print_metrics(element):
    print(
        f"[FixedWindow] "
        f"requests: {element['requests']} | "
        f"avg_latency: {element['avg_latency']}ms | "
        f"errors: {element['errors']}"
    )
    return element


if __name__ == '__main__':
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_message)
            | "Window" >> beam.WindowInto(FixedWindows(WINDOW_SIZE_SECONDS))
            | "Combine" >> beam.CombineGlobally(MetricsCombineFn()).without_defaults()
            | "Print" >> beam.Map(print_metrics)
        )
