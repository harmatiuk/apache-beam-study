import apache_beam as beam
from typing import NamedTuple

from beam_study.transforms.print_output import Output


with beam.Pipeline() as pipeline:

    input = pipeline | 'Input' >> beam.io.ReadFromCsv('gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv')

    taxi_cost = input | 'TaxiCost' >> beam.Select('total_amount')

    sample = taxi_cost | 'SampleData' >> beam.combiners.Sample.FixedSizeGlobally(10)

    sample | 'Print' >> Output()