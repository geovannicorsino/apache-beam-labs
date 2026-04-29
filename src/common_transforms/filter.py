"""
Filter | Keep only elements that satisfy a predicate.

Passes each element through a boolean function and drops those that return False.
Use it to exclude invalid, out-of-range, or unwanted records from a pipeline.
Side inputs (e.g., AsIter) let the predicate reference a dynamic allowlist computed at runtime.

Example input:
    [{'name': 'Strawberry', 'duration': 'perennial'},
     {'name': 'Potato', 'duration': 'PERENNIAL'}, ...]
Example output:
    {'name': 'Strawberry', 'duration': 'perennial'}
    {'name': 'Carrot', 'duration': 'biennial'}
    {'name': 'Eggplant', 'duration': 'perennial'}
    {'name': 'Tomato', 'duration': 'annual'}
"""
import apache_beam as beam

if __name__ == '__main__':
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
                lambda element, valid_durations: element['duration'] in valid_durations,
                valid_durations=beam.pvalue.AsIter(valid_durations),
            )
            | beam.Map(print))
