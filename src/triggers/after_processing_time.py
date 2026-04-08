"""
AfterProcessingTime Trigger | Fire based on how much wall-clock time has elapsed.

Fires after a fixed interval of processing time has passed since the first element
arrived in the window. Unlike AfterWatermark, it ignores event timestamps entirely
and fires based on the real clock of the machine running the pipeline.

Use it when you want results at regular real-time intervals regardless of event time,
for example: flush whatever arrived in the last 10 seconds and emit a result.

Example input:
    {"order_id": "001", "amount": 50.0}
    {"order_id": "002", "amount": 30.0}
    ... (events keep arriving)
Example output (every 10 seconds of processing time):
    total_orders: 4 | revenue: R$ 210.00
    total_orders: 2 | revenue: R$ 80.00
"""
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, Repeatedly
from apache_beam.transforms.window import GlobalWindows

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"

# Emit results every 10 seconds of processing time
INTERVAL_SECONDS = 10


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
    print(
        f"total_orders: {element['total_orders']} | "
        f"revenue: R$ {element['revenue']:.2f}"
    )
    return element


options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as p:
    (
        p
        | "Read"    >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
        | "Parse"   >> beam.Map(parse_message)
        | "Window"  >> beam.WindowInto(
            GlobalWindows(),
            # Repeatedly so it keeps firing every INTERVAL_SECONDS forever
            trigger=Repeatedly(AfterProcessingTime(INTERVAL_SECONDS)),
            accumulation_mode=AccumulationMode.DISCARDING,
            allowed_lateness=0,
        )
        | "Combine" >> beam.CombineGlobally(RevenueCombineFn()).without_defaults()
        | "Print"   >> beam.Map(print_result)
    )
