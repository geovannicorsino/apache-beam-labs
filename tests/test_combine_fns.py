"""
Tests for custom CombineFn implementations.

Covers:
  - SumEvenFn  (combine_fn.py)
  - SumEvenOrOddFn (combine_by_key.py)

Beam testing best practices:
  - Use TestPipeline instead of beam.Pipeline — it is optimised for tests and
    raises assertion errors instead of silently passing.
  - Use assert_that + equal_to to validate PCollection contents.
    equal_to is order-independent, matching the non-deterministic nature of
    PCollections in distributed runners.
  - Import the class under test directly — do not re-define it in the test file.
"""
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.core_transforms.combine.combine_fn import SumEvenFn
from src.core_transforms.combine.combine_by_key import SumEvenOrOddFn


class TestSumEvenFn(unittest.TestCase):

    def test_sums_only_even_numbers(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([1, 2, 3, 4, 5])
                | beam.CombineGlobally(SumEvenFn())
            )
            # 2 + 4 = 6
            assert_that(result, equal_to([6]))

    def test_all_odd_returns_zero(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([1, 3, 5, 7])
                | beam.CombineGlobally(SumEvenFn())
            )
            assert_that(result, equal_to([0]))

    def test_all_even(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([2, 4, 6])
                | beam.CombineGlobally(SumEvenFn())
            )
            assert_that(result, equal_to([12]))

    def test_single_element(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([4])
                | beam.CombineGlobally(SumEvenFn())
            )
            assert_that(result, equal_to([4]))


class TestSumEvenOrOddFn(unittest.TestCase):

    def test_separates_even_and_odd_per_key(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([
                    ('Ronaldo', 1),
                    ('Messi', 2),
                    ('Messi', 3),
                    ('Neymar', 4),
                    ('Neymar', 5),
                ])
                | beam.CombinePerKey(SumEvenOrOddFn())
            )
            assert_that(result, equal_to([
                ('Ronaldo', {'even': 0, 'odd': 1}),
                ('Messi',   {'even': 2, 'odd': 3}),
                ('Neymar',  {'even': 4, 'odd': 5}),
            ]))

    def test_all_even_values(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([('a', 2), ('a', 4)])
                | beam.CombinePerKey(SumEvenOrOddFn())
            )
            assert_that(result, equal_to([
                ('a', {'even': 6, 'odd': 0}),
            ]))

    def test_all_odd_values(self):
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([('a', 1), ('a', 3)])
                | beam.CombinePerKey(SumEvenOrOddFn())
            )
            assert_that(result, equal_to([
                ('a', {'even': 0, 'odd': 4}),
            ]))


if __name__ == '__main__':
    unittest.main()
