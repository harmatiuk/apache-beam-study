import typing

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