"""
Tests for MetricsCombineFn (fixed_window.py).

Best practice: test CombineFn classes directly via CombineGlobally in a
TestPipeline — this exercises the full accumulator lifecycle (create,
add_input, merge_accumulators, extract_output) just as Beam would in production.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.windowing.fixed_window import MetricsCombineFn


class TestMetricsCombineFn(unittest.TestCase):
    def test_counts_requests_and_errors(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        {"latency_ms": 100, "status": 200},
                        {"latency_ms": 200, "status": 500},
                        {"latency_ms": 300, "status": 200},
                    ]
                )
                | beam.CombineGlobally(MetricsCombineFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        {
                            "requests": 3,
                            "avg_latency": 200.0,
                            "errors": 1,
                        }
                    ]
                ),
            )

    def test_no_errors(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        {"latency_ms": 100, "status": 200},
                        {"latency_ms": 300, "status": 200},
                    ]
                )
                | beam.CombineGlobally(MetricsCombineFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        {
                            "requests": 2,
                            "avg_latency": 200.0,
                            "errors": 0,
                        }
                    ]
                ),
            )

    def test_all_errors(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        {"latency_ms": 500, "status": 500},
                        {"latency_ms": 600, "status": 503},
                    ]
                )
                | beam.CombineGlobally(MetricsCombineFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        {
                            "requests": 2,
                            "avg_latency": 550.0,
                            "errors": 2,
                        }
                    ]
                ),
            )

    def test_single_request(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([{"latency_ms": 142, "status": 200}])
                | beam.CombineGlobally(MetricsCombineFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        {
                            "requests": 1,
                            "avg_latency": 142.0,
                            "errors": 0,
                        }
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
