import apache_beam as beam

from beam_study.transforms.print_output import Output

numbers = [number for number in range(100,151)]
itens =[
    ('🥕', 3),
    ('🥕', 2),
    ('🍆', 1),
    ('🍅', 4),
    ('🍅', 5),
    ('🍅', 3),
]

with beam.Pipeline() as pipeline:

    number_collection = pipeline | 'CreateNumberCollection' >> beam.Create(numbers)
    itens_collection = pipeline | 'CreateItensCollection' >> beam.Create(itens)

    (
        number_collection
        | 'MinNumber' >> beam.CombineGlobally(lambda elements: min(elements))
        | 'PrintMinNumber' >> Output(prefix='Min number: ')
    )

    (
        number_collection
        | 'MaxNumber' >> beam.CombineGlobally(lambda elements: max(elements))
        | 'PrintMaxNumber' >> Output(prefix='Max number: ')
    )

    (
        itens_collection
        | 'MinPerKey' >> beam.CombinePerKey(min)
        | 'PrintMinPerKey' >> Output(prefix='Min per key: ')
    )

    (
        itens_collection
        | 'MaxPerKey' >> beam.CombinePerKey(max)
        | 'PrintMaxPerKey' >> Output(prefix='Max per key: ')
    )