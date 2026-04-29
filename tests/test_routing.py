"""
Tests for element routing patterns.

Covers:
  - RouteRecords DoFn (additional_outputs.py)
  - ParseAndValidateDoFn / DLQ pattern (dead_letter_queue.py)

Best practice: when testing multiple outputs (TaggedOutput), access each
tag separately and assert on each PCollection independently.

Note: when using with_outputs() without main=, all outputs including the
"main" tag are accessed via their explicit tag name.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty

from src.advanced_transforms.dead_letter_queue import DEAD_LETTER, VALID, ParseAndValidateDoFn
from src.core_transforms.additional_outputs import RouteRecords


class TestRouteRecords(unittest.TestCase):
    def _route(self, p, records):
        """Helper: runs RouteRecords and returns results with all tags accessible."""
        return (
            p
            | beam.Create(records)
            | beam.ParDo(RouteRecords()).with_outputs("valid", "invalid", "dead_letter")
        )

    def test_positive_value_goes_to_valid(self):
        with TestPipeline() as p:
            results = self._route(p, [{"id": 1, "value": 10}])
            assert_that(results["valid"], equal_to([{"id": 1, "value": 10}]))
            assert_that(results["invalid"], is_empty())
            assert_that(results["dead_letter"], is_empty())

    def test_negative_value_goes_to_invalid(self):
        with TestPipeline() as p:
            results = self._route(p, [{"id": 2, "value": -5}])
            assert_that(results["valid"], is_empty())
            assert_that(results["invalid"], equal_to([{"id": 2, "value": -5}]))
            assert_that(results["dead_letter"], is_empty())

    def test_none_value_goes_to_dead_letter(self):
        with TestPipeline() as p:
            results = self._route(p, [{"id": 3, "value": None}])
            assert_that(results["valid"], is_empty())
            assert_that(results["invalid"], is_empty())
            assert_that(results["dead_letter"], equal_to([{"id": 3, "value": None}]))

    def test_mixed_records_routed_correctly(self):
        with TestPipeline() as p:
            results = self._route(
                p,
                [
                    {"id": 1, "value": 10},
                    {"id": 2, "value": -5},
                    {"id": 3, "value": None},
                ],
            )
            assert_that(results["valid"], equal_to([{"id": 1, "value": 10}]))
            assert_that(results["invalid"], equal_to([{"id": 2, "value": -5}]))
            assert_that(results["dead_letter"], equal_to([{"id": 3, "value": None}]))


class TestDeadLetterQueue(unittest.TestCase):
    def test_valid_json_with_required_fields(self):
        with TestPipeline() as p:
            results = (
                p
                | beam.Create(['{"order_id": "001", "amount": 99.5}'])
                | beam.ParDo(ParseAndValidateDoFn()).with_outputs(VALID, DEAD_LETTER)
            )
            assert_that(results[VALID], equal_to([{"order_id": "001", "amount": 99.5}]))
            assert_that(results[DEAD_LETTER], is_empty())

    def test_invalid_json_goes_to_dlq(self):
        with TestPipeline() as p:
            results = (
                p
                | beam.Create(["not valid json"])
                | beam.ParDo(ParseAndValidateDoFn()).with_outputs(VALID, DEAD_LETTER)
            )
            assert_that(results[VALID], is_empty())

    def test_missing_field_goes_to_dlq(self):
        with TestPipeline() as p:
            results = (
                p
                | beam.Create(['{"order_id": "004"}'])  # missing amount
                | beam.ParDo(ParseAndValidateDoFn()).with_outputs(VALID, DEAD_LETTER)
            )
            assert_that(results[VALID], is_empty())

    def test_mixed_input(self):
        with TestPipeline() as p:
            results = (
                p
                | beam.Create(
                    [
                        '{"order_id": "001", "amount": 99.5}',
                        "not valid json",
                        '{"order_id": "003", "amount": 210.0}',
                        '{"order_id": "004"}',
                    ]
                )
                | beam.ParDo(ParseAndValidateDoFn()).with_outputs(VALID, DEAD_LETTER)
            )
            assert_that(
                results[VALID],
                equal_to(
                    [
                        {"order_id": "001", "amount": 99.5},
                        {"order_id": "003", "amount": 210.0},
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
