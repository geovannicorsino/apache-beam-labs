'''Common Transforms motivating challenge

You are provided with a PCollection created from the array of taxi order prices in a csv file.
Your task is to find how many orders are below $15 and how many are equal to or above $15.
Return it as a map structure (key-value), make above or below the key, and the total dollar value (sum) of orders - the value.
Although there are many ways to do this, try using another transformation presented in this module.
'''
from typing import Dict, List, Any
import apache_beam as beam
import csv

CSV_PATH = 'data\challenge\sample1000.csv'

SCHEMA = {
    "VendorID": int,
    "tpep_pickup_datetime": str,
    "tpep_dropoff_datetime": str,
    "passenger_count": int,
    "trip_distance": float,
    "RatecodeID": int,
    "store_and_fwd_flag": str,
    "PULocationID": int,
    "DOLocationID": int,
    "payment_type": int,
    "fare_amount": float,
    "extra": float,
    "mta_tax": float,
    "tip_amount": float,
    "tolls_amount": float,
    "improvement_surcharge": float,
    "total_amount": float,
}


def get_header(path: str) -> List[str]:
    """
    Get the header of a csv file as a list of strings

    Args:
        path (str): Path to the csv file

    Returns:
        List[str]: List of strings representing the header of the csv file
    """
    with beam.io.filesystems.FileSystems.open(path) as f:
        first_line = f.readline().decode("utf-8").strip()
    return next(csv.reader([first_line]))


def parse_row(line: str, fields: List[str], schema: Dict) -> Dict[str, Any]:
    """
    Parse a line of a csv file into a dictionary, using the provided fields and schema

    Args:
        line (str): A line of a csv file
        fields (List[str]): List of strings representing the column names
        schema (Dict): Dictionary mapping column names to their expected types

    Returns:
        Dict[str, Any]: A dictionary representing the parsed line, with keys as column names and values as the parsed values according to the schema
    """
    values = next(csv.reader([line]))
    return {
        key: schema[key](value) if key in schema else value
        for key, value in zip(fields, values)
    }


with beam.Pipeline() as p:
    taxi_orders = (
        p
        | 'Read Taxi Orders' >> beam.io.ReadFromText(CSV_PATH, skip_header_lines=1)
        | 'Split Line to Dict' >> beam.Map(parse_row, fields=get_header(CSV_PATH), schema=SCHEMA)
        | 'Key by threshold' >> beam.Map(
            lambda x: ("below" if x["total_amount"] <
                       15 else "above", x["total_amount"])
        )
        | 'Sum total amounts by key' >> beam.CombinePerKey(sum)
        | 'Print Orders' >> beam.Map(print)
    )
