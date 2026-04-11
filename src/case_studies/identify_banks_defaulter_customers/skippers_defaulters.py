"""
Case Study - Identify Bank's Defaulter Customers
Credit card skippers/defaulters:

 --> Assign 1 point to customer for short payment, where a short payment means when customer fails to clear atleast 70% of its monthly spends.
 --> Assign 1 point to customer where he has spent 100% of his max_limit but did not clear the full amount.
 --> If for any month customer is meeting both the above conditions,assign 1 additional point.
 --> Sum up all the points for a customer and output in file.
"""
import apache_beam as beam


def parse_line(line):
    parts = [p.strip() for p in line.split(',')]
    if len(parts) < 10:
        return None
    try:
        return {
            'customer_id': parts[0],
            'first_name': parts[1],
            'last_name': parts[2],
            'relationship_no': parts[3],
            'card_type': parts[4],
            'max_credit_limit': float(parts[5]),
            'total_spent': float(parts[6]),
            'cash_withdrawn': float(parts[7]),
            'cleared_amount': float(parts[8]),
            'last_date': parts[9],
        }
    except ValueError:
        return None


def calc_points(record):
    spent = record['total_spent']
    cleared = record['cleared_amount']
    limit_ = record['max_credit_limit']

    short_payment = 1 if cleared < 0.7 * spent else 0
    full_limit_unpaid = 1 if spent >= limit_ and cleared < spent else 0
    extra = 1 if short_payment and full_limit_unpaid else 0

    points = short_payment + full_limit_unpaid + extra

    return record['customer_id'], {
        'first_name': record['first_name'],
        'last_name': record['last_name'],
        'points': points,
    }


def sum_points(records):
    print(records)
    records = list(records)
    print(records)
    return {
        'first_name': records[0]['first_name'],
        'last_name': records[0]['last_name'],
        'points': sum(r['points'] for r in records),
    }


def format_result(kv):
    customer_id, info = kv
    return f"{customer_id},{info['first_name']},{info['last_name']},{info['points']}"


if __name__ == '__main__':
    with beam.Pipeline() as p:
        (
            p
            | 'ReadFile' >> beam.io.ReadFromText('data/bank/cards.txt', skip_header_lines=1)
            | 'Parse' >> beam.Map(parse_line)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
            | 'ComputePoints' >> beam.Map(calc_points)
            | 'SumPointsPerCustomer' >> beam.CombinePerKey(sum_points)
            | 'Format' >> beam.Map(format_result)
            | 'Write' >> beam.io.WriteToText(
                'data/test/bank/cards_defaulters_points.txt',
                num_shards=1,
                shard_name_template='',
                header='customer_id,first_name,last_name,total_points'
            )
        )
