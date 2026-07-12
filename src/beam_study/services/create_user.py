import apache_beam as beam
from faker import Faker

from models.user import User, Address

faker = Faker("pt_BR")

def _faker_user() -> User:
    return User(
        user_id= faker.uuid1(),
        name=faker.name(),
        age=faker.random_int(min=15, max=80),
        address= Address(
            street= faker.street_name(),
            city=faker.city(),
            state=faker.estado_sigla(),
            number=faker.building_number()
        )
    )

class CreateUsers(beam.PTransform):
    def __init__(self, count: int=50, label: str | None=None) -> None:
        super().__init__(label)
        self.count = count
    
    def expand(self, input: beam.Pipeline) -> beam.PCollection:
        users = [_faker_user() for _ in range(self.count)]

        return input | beam.Create(users)


if __name__ == '__main__':
    from utils.print_output import Output

    with beam.Pipeline() as pipeline:
        pipeline | CreateUsers(count=5) | Output()