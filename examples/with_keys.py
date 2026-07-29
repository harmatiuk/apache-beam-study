import apache_beam as beam
import uuid


data = ['apple', 'banana', 'cherry', 'durian', 'guava', 'melon']

with beam.Pipeline() as pipeline:

    collection = (
        pipeline
        | 'CreateCollection' >> beam.Create(data)
        | 'WithKey' >> beam.WithKeys(lambda element: str(uuid.uuid4()))
        | 'Print' >> beam.Map(print)
    )