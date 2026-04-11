'''
Global Windowing | Live Scoreboard

Example input:
    {"team": "Brazil"}
    {"team": "Argentina"}

Example output:
    Brazil               2 goal(s)
    Argentina            1 goal(s)
'''
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterCount, Repeatedly
from apache_beam.transforms.window import GlobalWindows


PROJECT = "geovanni-corsino-labs"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/event-topic-sub"


def parse_message(message):
    return json.loads(message.decode("utf-8"))


def print_scoreboard(element):
    team, goals = element
    print(f"  {team:<20} {goals} goal(s)")
    return element


def print_separator(_):
    print("-" * 35)


if __name__ == '__main__':
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        goals = (
            p
            | "Read" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse" >> beam.Map(parse_message)
            | "ExtractTeam" >> beam.Map(lambda e: (e["team"], 1))
            | "Window" >> beam.WindowInto(
                GlobalWindows(),
                trigger=Repeatedly(AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=0,
            )
            | "SumByTeam" >> beam.CombinePerKey(sum)
        )

        goals | "PrintScore" >> beam.Map(print_scoreboard)
        goals | "PrintSeparator" >> beam.Map(print_separator)
