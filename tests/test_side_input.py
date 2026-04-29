"""
Tests for side input patterns.

Covers:
  - EnrichDoFn with AsSingleton side input (side_input.py)

Best practice: when testing side inputs, pass the side input as a concrete
Python value directly to the DoFn in tests — no need to compute it via a
pipeline branch. This isolates the DoFn logic from the aggregation logic.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.core_transforms.side_input import EnrichDoFn


class TestEnrichDoFn(unittest.TestCase):
    def test_above_average(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([{"name": "Bob", "salary": 8000}])
                # Pass avg_salary as a concrete value — no need for AsSingleton in tests
                | beam.ParDo(EnrichDoFn(), avg_salary=6000.0)
            )
            assert_that(result, equal_to([{"name": "Bob", "salary": 8000, "above_avg": True}]))

    def test_below_average(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([{"name": "Alice", "salary": 5000}])
                | beam.ParDo(EnrichDoFn(), avg_salary=6000.0)
            )
            assert_that(result, equal_to([{"name": "Alice", "salary": 5000, "above_avg": False}]))

    def test_exactly_average_is_not_above(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([{"name": "Charlie", "salary": 6000}])
                | beam.ParDo(EnrichDoFn(), avg_salary=6000.0)
            )
            assert_that(result, equal_to([{"name": "Charlie", "salary": 6000, "above_avg": False}]))

    def test_multiple_employees(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        {"name": "Alice", "salary": 5000},
                        {"name": "Bob", "salary": 8000},
                        {"name": "Charlie", "salary": 6000},
                    ]
                )
                | beam.ParDo(EnrichDoFn(), avg_salary=6333.0)
            )
            assert_that(
                result,
                equal_to(
                    [
                        {"name": "Alice", "salary": 5000, "above_avg": False},
                        {"name": "Bob", "salary": 8000, "above_avg": True},
                        {"name": "Charlie", "salary": 6000, "above_avg": False},
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
