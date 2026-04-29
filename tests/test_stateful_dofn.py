"""
Tests for stateful DoFn patterns.

Covers:
  - DeduplicateByKeyDoFn (stateful_dofn.py)

Best practice: stateful DoFns require keyed (key, value) input and a window.
The DirectRunner handles state correctly in tests, so no special setup is needed.
Assert that duplicates are dropped and only the first occurrence per key is emitted.
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.advanced_transforms.stateful_dofn import DeduplicateByKeyDoFn


class TestDeduplicateByKeyDoFn(unittest.TestCase):
    def test_drops_duplicate_keys(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        ("order-1", {"amount": 100.0}),
                        ("order-2", {"amount": 200.0}),
                        ("order-1", {"amount": 100.0}),  # duplicate
                        ("order-3", {"amount": 150.0}),
                        ("order-2", {"amount": 200.0}),  # duplicate
                    ]
                )
                | beam.ParDo(DeduplicateByKeyDoFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        ("order-1", {"amount": 100.0}),
                        ("order-2", {"amount": 200.0}),
                        ("order-3", {"amount": 150.0}),
                    ]
                ),
            )

    def test_no_duplicates_passes_all(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        ("order-1", {"amount": 100.0}),
                        ("order-2", {"amount": 200.0}),
                    ]
                )
                | beam.ParDo(DeduplicateByKeyDoFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        ("order-1", {"amount": 100.0}),
                        ("order-2", {"amount": 200.0}),
                    ]
                ),
            )

    def test_single_element(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([("order-1", {"amount": 100.0})])
                | beam.ParDo(DeduplicateByKeyDoFn())
            )
            assert_that(
                result,
                equal_to(
                    [
                        ("order-1", {"amount": 100.0}),
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
