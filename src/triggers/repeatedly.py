"""
Repeatedly Trigger | Fire when a minimum number of elements have been collected.

A data-driven trigger that fires whenever N elements accumulate in a window,
regardless of event time or processing time. Use it when you want results based
on data volume rather than time — for example, processing records in micro-batches.

Example input:
    {"order_id": "001", "amount": 50.0}
    {"order_id": "002", "amount": 30.0}
    {"order_id": "003", "amount": 20.0}  ← 3rd element: trigger fires
Example output:
    total_orders: 3 | revenue: R$ 100.00
"""
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterCount, Repeatedly
from apache_beam.transforms.window import GlobalWindows

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"

# Fires every time 3 elements accumulate in the window
BATCH_SIZE = 3


def parse_message(message):
    return json.loads(message.decode("utf-8"))


def print_result(element):
    print(
        f"total_orders: {element['total_orders']} | "
        f"revenue: R$ {element['revenue']:.2f}"
    )
    return element


class RevenueCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return {"total_orders": 0, "revenue": 0.0}

    def add_input(self, acc, element):
        acc["total_orders"] += 1
        acc["revenue"] += element["amount"]
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["total_orders"] += acc["total_orders"]
            merged["revenue"] += acc["revenue"]
        return merged

    def extract_output(self, acc):
        return {
            "total_orders": acc["total_orders"],
            "revenue": round(acc["revenue"], 2),
        }


if __name__ == '__main__':
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_message)
            | "Window" >> beam.WindowInto(
                GlobalWindows(),
                trigger=Repeatedly(AfterCount(BATCH_SIZE)),
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0,
            )
            | "Combine" >> beam.CombineGlobally(RevenueCombineFn()).without_defaults()
            | "Print" >> beam.Map(print_result)
        )
