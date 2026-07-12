import typing

import apache_beam as beam

from utils.print_output import Output


class Address(typing.NamedTuple):
    street: str
    city: str
    state: str
    number: str


class User(typing.NamedTuple):
    user_id: str
    name: str
    age: int
    address: Address


user_1 = User(
    user_id="abcd",
    name="Harmatiuk",
    age=30,
    address=Address(street="Rua test", city="Curitiba", state="PR", number="420"),
)

user_2 = data = User(
    user_id="egfh",
    name="Lucas",
    age=18,
    address=Address(street="Rua estudando sabado a noite", city="Curitiba", state="PR", number="10"),
)

def is_of_age(user: User) -> bool:
    return user.age > 18

with beam.Pipeline() as pipeline:
    my_first_pcolletion = pipeline | "CreateMyFirstPcolletion" >> beam.Create(range(1, 10))

    my_first_pcolletion | "PrintMyFirstPcolletion" >> Output()

    user = pipeline | "User" >> beam.Create([user_1, user_2])

    user | "PrintUser" >> Output()
