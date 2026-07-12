import apache_beam as beam

from src.beam_study.factories.user_factory import create_fake_user


class CreateUsers(beam.PTransform):
    def __init__(self, count: int = 50, label: str | None = None) -> None:
        super().__init__(label)
        self.count = count

    def expand(self, input: beam.Pipeline) -> beam.PCollection:
        users = [create_fake_user() for _ in range(self.count)]

        return input | beam.Create(users)


if __name__ == "__main__":
    from beam_study.transforms.print_output import Output

    with beam.Pipeline() as pipeline:
        pipeline | CreateUsers(count=5) | Output()
