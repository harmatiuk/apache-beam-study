import apache_beam as beam

from beam_study.models.user import Address, User
from beam_study.transforms.print_output import Output

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

with beam.Pipeline() as pipeline:

    user = pipeline | "User" >> beam.Create([user_1, user_2])

    user | "PrintUser" >> Output()
