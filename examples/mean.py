import apache_beam as beam

numbers = [number for number in range(1,11)]

with beam.Pipeline() as pipeline:

    mean = (
        pipeline
        | 'CreateNumberCollection' >> beam.Create(numbers)
        | 'CalculateMean' >> beam.combiners.Mean.Globally()
        | 'PrintMean' >> beam.Map(print)
    )