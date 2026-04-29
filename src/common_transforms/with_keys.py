"""
WithKeys | Promote a field from each element into the key of a (key, value) pair.

Wraps every element in a 2-tuple (derived_key, original_element) without modifying the element.
Use it to prepare a flat PCollection for downstream key-aware transforms such as GroupByKey
or CoGroupByKey.

Example input:
    [{'id': '323154564656', 'first_name': 'Alice', 'last_name': 'Smith', 'age': 30}, ...]
Example output:
    ('323154564656', {'id': '323154564656', 'first_name': 'Alice', 'last_name': 'Smith', 'age': 30})
    ('323154564657', {'id': '323154564657', 'first_name': 'Bob', 'last_name': 'Johnson', 'age': 25})
    ('323154564658',
     {'id': '323154564658', 'first_name': 'Charlie', 'last_name': 'Williams', 'age': 35})
"""

import apache_beam as beam

if __name__ == "__main__":
    with beam.Pipeline() as p:
        people = (
            p
            | "Create people with ages"
            >> beam.Create(
                [
                    {"id": "323154564656", "first_name": "Alice", "last_name": "Smith", "age": 30},
                    {"id": "323154564657", "first_name": "Bob", "last_name": "Johnson", "age": 25},
                    {
                        "id": "323154564658",
                        "first_name": "Charlie",
                        "last_name": "Williams",
                        "age": 35,
                    },
                ]
            )
            | "Create key-value pairs" >> beam.WithKeys(lambda x: x["id"])
            | "Print results" >> beam.Map(print)
        )
