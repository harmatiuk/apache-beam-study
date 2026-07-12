from faker import Faker

from src.beam_study.models.user import Address, User

faker = Faker("pt_BR")


def create_fake_user() -> User:
    return User(
        user_id=faker.uuid1(),
        name=faker.name(),
        age=faker.random_int(min=15, max=80),
        address=Address(
            street=faker.street_name(),
            city=faker.city(),
            state=faker.estado_sigla(),
            number=faker.building_number(),
        ),
    )
