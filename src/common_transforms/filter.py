import apache_beam as beam

with beam.Pipeline() as p:
    valid_durations = p | 'Valid durations' >> beam.Create([
        'annual',
        'biennial',
        'perennial',
    ])

    valid_plants = (
        p | 'Gardening plants' >> beam.Create([
            {
                'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'name': 'Tomato', 'duration': 'annual'
            },
            {
                'name': 'Potato', 'duration': 'PERENNIAL'
            },
        ])
        | 'Filter valid plants' >> beam.Filter(
            lambda element, valid_durations: element['duration'] in valid_durations, valid_durations=beam.pvalue.AsIter(valid_durations),
        )
        | beam.Map(print))

# {'name': 'Strawberry', 'duration': 'perennial'}
# {'name': 'Carrot', 'duration': 'biennial'}
# {'name': 'Eggplant', 'duration': 'perennial'}
# {'name': 'Tomato', 'duration': 'annual'}
