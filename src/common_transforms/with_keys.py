import apache_beam as beam

with beam.Pipeline() as p:
    people = (
        p
        | 'Create people with ages' >> beam.Create([
            {
                "id": "323154564656",
                "first_name": "Alice",
                "last_name": "Smith",
                "age": 30
            },
            {
                "id": "323154564657",
                "first_name": "Bob",
                "last_name": "Johnson",
                "age": 25
            },
            {
                "id": "323154564658",
                "first_name": "Charlie",
                "last_name": "Williams",
                "age": 35
            },
        ])
        | "Create key-value pairs" >> beam.WithKeys(lambda x: x["id"])
        | "Print results" >> beam.Map(print)
    )

# ('323154564656', {'id': '323154564656', 'first_name': 'Alice', 'last_name': 'Smith', 'age': 30})
# ('323154564657', {'id': '323154564657', 'first_name': 'Bob', 'last_name': 'Johnson', 'age': 25})
# ('323154564658', {'id': '323154564658', 'first_name': 'Charlie', 'last_name': 'Williams', 'age': 35})
