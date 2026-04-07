"""
Partition | Split a single PCollection into a fixed number of disjoint sub-collections.

A partitioning function maps each element to an integer index in [0, num_partitions).
Use it when different subsets of your data require different processing logic applied in parallel.

Example input:
    [{'name': 'Alice', 'tier': 'gold', ...}, {'name': 'Bob', 'tier': 'bronze', ...}, ...]
Example output:
    bronze partition: {'id': 2, 'name': 'Bob',   'tier': 'bronze', 'spent': 200,   'discount': 0.05}
    silver partition: {'id': 3, 'name': 'Carol',  'tier': 'silver', 'spent': 1500,  'discount': 0.10}
    gold   partition: {'id': 1, 'name': 'Alice',  'tier': 'gold',   'spent': 9500,  'discount': 0.20}
"""
import apache_beam as beam


PARTITIONS = ["bronze", "silver", "gold"]


def partition_by_tier(element, num_partitions):
    """Partition customers by their tier index."""
    tier_index = {"bronze": 0, "silver": 1, "gold": 2}
    index = tier_index.get(element["tier"], 0)
    return min(index, num_partitions - 1)


with beam.Pipeline() as p:
    customers = p | "Customers" >> beam.Create([
        {"id": 1, "name": "Alice",   "tier": "gold",   "spent": 9500},
        {"id": 2, "name": "Bob",     "tier": "bronze", "spent": 200},
        {"id": 3, "name": "Carol",   "tier": "silver", "spent": 1500},
        {"id": 4, "name": "Dan",     "tier": "gold",   "spent": 12000},
        {"id": 5, "name": "Eve",     "tier": "bronze", "spent": 80},
        {"id": 6, "name": "Frank",   "tier": "silver", "spent": 3000},
    ])

    bronze, silver, gold = customers | beam.Partition(partition_by_tier, 3)

    bronze | "Discount Bronze" >> beam.Map(
        lambda x: {**x, "discount": 0.05}
    ) | "Print Bronze" >> beam.Map(print)

    silver | "Discount Silver" >> beam.Map(
        lambda x: {**x, "discount": 0.10}
    ) | "Print Silver" >> beam.Map(print)

    gold | "Discount Gold" >> beam.Map(
        lambda x: {**x, "discount": 0.20}
    ) | "Print Gold" >> beam.Map(print)
