"""
AfterWatermark Trigger | Fire based on event-time progress.

AfterWatermark fires when the watermark passes the end of the window — meaning Beam
estimates all data for that window has arrived. It is the most common trigger for
event-time streaming pipelines because results respect the actual time the events occurred.

It has three firing phases:
  - early:  fires before the window closes (preview of partial results)
  - on-time: fires exactly when the watermark crosses the window end (main result)
  - late:   fires when data arrives after the watermark (corrections)

Example input:
    {"order_id": "001", "amount": 50.0}
    {"order_id": "002", "amount": 120.0}
Example output:
    [EARLY]   total_orders: 3 | revenue: R$ 270.00
    [ON-TIME] total_orders: 5 | revenue: R$ 480.00
    [LATE]    total_orders: 6 | revenue: R$ 530.00  ← late order arrived
"""
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterCount, AfterWatermark, TimestampCombiner
from apache_beam.transforms.window import FixedWindows

PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"

WINDOW_SIZE_SECONDS = 60
# Accept data arriving up to 30s after the watermark passed the window end
ALLOWED_LATENESS_SECONDS = 30


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


if __name__ == '__main__':
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read"    >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse"   >> beam.Map(parse_message)
            | "Window"  >> beam.WindowInto(
                FixedWindows(WINDOW_SIZE_SECONDS),
                trigger=AfterWatermark(
                    # Fire a preview every 3 events before the window closes
                    early=AfterCount(3),
                    # Fire a correction for every late event that arrives
                    late=AfterCount(1),
                ),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
                # OUTPUT_AT_EOW: the output timestamp of each pane is set to the
                # end of the window, regardless of when individual events arrived.
                # This makes downstream time-based operations predictable.
                timestamp_combiner=TimestampCombiner.OUTPUT_AT_EOW,
            )
            | "Combine" >> beam.CombineGlobally(RevenueCombineFn()).without_defaults()
            | "Print"   >> beam.Map(print_result)
        )
