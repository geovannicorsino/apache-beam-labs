"""
Splittable DoFn | Process large elements in parallel by splitting into sub-ranges.

A regular DoFn processes one element at a time in a single worker. A Splittable DoFn
tells Beam how to split a large element (e.g. a file with 1000 lines) into smaller
restrictions (e.g. lines 0-499 and 500-999) so multiple workers can process them
in parallel.

Key components:
  RestrictionProvider — defines how to create, split and measure restrictions
  RestrictionTracker  — tracks progress within a restriction using try_claim()
  try_claim(pos)      — claims the right to process position pos.
                        Returns False if another worker already claimed it.

Example scenario:
  Input: list of sentences (each sentence is a "file" with words as "lines")
  Each sentence is split into two halves processed independently.

Example input:
    'the quick brown fox'
    'jumps over the lazy dog'
Example output:
    (0, 'the'), (1, 'quick')          ← first half of sentence 1
    (2, 'brown'), (3, 'fox')          ← second half of sentence 1
    (0, 'jumps'), (1, 'over')         ← first half of sentence 2
    (2, 'the'), (3, 'lazy'), (4, 'dog') ← second half of sentence 2
"""
import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker


class WordRangeProvider(beam.RestrictionProvider):

    def initial_restriction(self, element):
        """Create the initial restriction covering all words in the element."""
        words = element.split()
        # restriction covers the full range of word indices: 0 to len(words)
        return OffsetRange(0, len(words))

    def create_tracker(self, restriction):
        """Wrap the restriction in a tracker that supports try_claim()."""
        return OffsetRestrictionTracker(restriction)

    def split(self, element, restriction):
        """Split the restriction into two halves for parallel processing."""
        mid = (restriction.start + restriction.stop) // 2
        yield OffsetRange(restriction.start, mid)
        yield OffsetRange(mid, restriction.stop)

    def restriction_size(self, element, restriction):
        """Report the size of the restriction so Beam can balance load across workers."""
        return restriction.stop - restriction.start


class ProcessWordsSplittable(beam.DoFn):
    def process(
        self,
        element,
        restriction_tracker=beam.DoFn.RestrictionParam(WordRangeProvider()),
    ):
        words = element.split()
        cur = restriction_tracker.current_restriction().start

        while restriction_tracker.try_claim(cur):
            # try_claim returns True while this worker owns position cur
            yield (cur, words[cur])
            cur += 1


with beam.Pipeline() as p:
    (
        p
        | 'Create sentences' >> beam.Create([
            'the quick brown fox',
            'jumps over the lazy dog',
        ])
        | 'Process words' >> beam.ParDo(ProcessWordsSplittable())
        | 'Print' >> beam.Map(print)
    )
