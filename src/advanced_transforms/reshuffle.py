"""
Reshuffle | Force redistribution of elements across workers, breaking stage fusion.

Beam automatically fuses consecutive transforms to run them in the same worker
(avoiding serialization overhead). Reshuffle breaks this fusion, useful when:
  - Create() produces all elements on a single worker and downstream transforms are heavy
  - A hot key concentrates too much data on one worker
  - A long-running bundle risks timing out and needs to be split into smaller chunks

Without Reshuffle:
  Create → Map → Map   (all fused, single worker)

With Reshuffle:
  Create → Reshuffle → Map → Map   (Map runs across multiple workers)

Example input:
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
Example output:
    2, 4, 6, 8, 10, 12, 14, 16, 18, 20  (order may vary — different workers)
"""
import apache_beam as beam

with beam.Pipeline() as p:
    (
        p
        | 'Create' >> beam.Create(list(range(1, 11)))
        # Without Reshuffle, all elements stay on the same worker after Create.
        # Reshuffle redistributes them so the heavy Map below can run in parallel.
        | 'Reshuffle' >> beam.Reshuffle()
        | 'Double' >> beam.Map(lambda x: x * 2)
        | 'Print' >> beam.Map(print)
    )
