"""
Composite Triggers | Combine multiple triggers with AfterFirst (OR) or AfterAll (AND).

AfterFirst — fires when ANY of the triggers fires first (OR logic).
AfterAll   — fires only when ALL triggers have fired (AND logic).

Example input:
    {"order_id": "001", "amount": 50.0}
    {"order_id": "002", "amount": 30.0}
Example output (AfterFirst — fired by AfterCount, 5 orders arrived before 10s):
    total_orders: 5 | revenue: R$ 280.00
Example output (AfterFirst — fired by AfterProcessingTime, only 2 orders in 10s):
    total_orders: 2 | revenue: R$ 90.00
"""

import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterFirst,
    AfterProcessingTime,
    Repeatedly,
)
from apache_beam.transforms.window import GlobalWindows

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"


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


def parse_message(message):
    return json.loads(message.decode("utf-8"))


def print_result(element):
    print(f"total_orders: {element['total_orders']} | " f"revenue: R$ {element['revenue']:.2f}")
    return element


# AfterFirst (OR): fires when 5 elements arrive OR 10s of processing time pass
trigger = Repeatedly(
    AfterFirst(
        AfterCount(5),
        AfterProcessingTime(10),
    )
)


if __name__ == "__main__":
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_message)
            | "Window"
            >> beam.WindowInto(
                GlobalWindows(),
                trigger=trigger,
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0,
            )
            | "Combine" >> beam.CombineGlobally(RevenueCombineFn()).without_defaults()
            | "Print" >> beam.Map(print_result)
        )
