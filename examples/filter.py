import apache_beam as beam

from beam_study.transforms.print_output import Output

plants = [
    {"icon": "🍓", "name": "Strawberry", "duration": "perennial"},
    {"icon": "🥕", "name": "Carrot", "duration": "biennial"},
    {"icon": "🍆", "name": "Eggplant", "duration": "perennial"},
    {"icon": "🍅", "name": "Tomato", "duration": "annual"},
    {"icon": "🥔", "name": "Potato", "duration": "perennial"},
]


def is_perennial(plant: dict) -> bool:
    return plant.get("duration") == "perennial"


def has_duration(plant: dict, duration: str) -> bool:
    return plant.get("duration") == duration


with beam.Pipeline() as pipeline:
    data = pipeline | "CreateData" >> beam.Create(plants)

    # Filter with python function
    perennials = data | "FilterPerennials" >> beam.Filter(is_perennial)

    # Filter with python lambda
    annuals = data | "FilterAnnuals" >> beam.Filter(lambda plant: plant["duration"] == "annual")

    # Filter with multiple arguments
    biennials = data | "FilterBiennials" >> beam.Filter(has_duration, "biennial")

    (
        perennials
        | "CountPerennials" >> beam.combiners.Count.Globally()
        | "PrintCountPerennials" >> Output(prefix="Total perennials: ")
    )

    (
        annuals
        | "CountAnnuals" >> beam.combiners.Count.Globally()
        | "PrintCountAnnuals" >> Output(prefix="Total annuals: ")
    )

    (
        biennials
        | "CountBiennials" >> beam.combiners.Count.Globally()
        | "PrintCountBiennials" >> Output(prefix="Total biennials: ")
    )
