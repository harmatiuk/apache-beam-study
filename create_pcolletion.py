import apache_beam as beam
from apache_beam.pvalue import PCollection


class Output(beam.PTransform):
    """Print each element of a PCollection, optionally prefixed.

    Example:
        >>> pcoll | Output(prefix="debug: ")
    """

    class _OutputFn(beam.DoFn):
        def __init__(self, prefix: str = "") -> None:
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(f"{self.prefix}{element}")

    def __init__(self, label: str | None = None, prefix: str = "") -> None:
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input: PCollection) -> PCollection:
        return input | beam.ParDo(self._OutputFn(self.prefix))
