"""Classify taxi rides as 'above' or 'below' $15 and total their price and count.

You are provided with a PCollection created from the array of taxi order prices in a csv file.
Your task is to find how many orders are below $15 and how many are equal to or above $15.
Return it as a map structure (key-value), make above or below the key,
and the total dollar value (sum) of orders - the value
"""

import apache_beam as beam

PRICE_THRESHOLD = 15


def classify_ride(element: beam.Row) -> tuple[str, tuple[float, int]]:
    """Key a ride by its price bracket and pair it with (total_amount, ride_count=1).

    Args:
        element: A Row with a total_amount field.

    Returns:
        A (key, (total_amount, 1)) pair, where key is 'above' or 'below'.
    """
    key = 'above' if element.total_amount >= PRICE_THRESHOLD else 'below'
    return key, (element.total_amount, 1)


def to_row(element: tuple[str, tuple[float, int]]) -> tuple[str, beam.Row]:
    """Wrap a (key, (total_amount, total_rides)) pair into (key, Row) with named fields.

    Args:
        element: A (key, (total_amount, total_rides)) pair.

    Returns:
        A (key, Row) pair with total_amount and total_rides as named fields.
    """
    key, (total_amount, total_rides) = element
    return key, beam.Row(total_amount=total_amount, total_rides=total_rides)


with beam.Pipeline() as pipeline:

    data = (
        pipeline
        | 'ReadData' >> beam.io.ReadFromCsv(path='gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv')
    )

    ride_prices = (
        data
        | 'SelectRidePrices' >> beam.Select('total_amount')
    )

    with_keys = (
        ride_prices
        | 'CreateKey' >> beam.Map(classify_ride)
    )

    final = (
        with_keys
        | 'SumByKey' >> beam.CombinePerKey(beam.combiners.TupleCombineFn(sum, sum))
        | 'ToRow' >> beam.Map(to_row)
        | 'Print' >> beam.Map(print)
    )
