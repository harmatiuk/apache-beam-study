import apache_beam as beam


numbers = [number for number in range(1,11)]

itens =[
    ('🥕', 3),
    ('🥕', 2),
    ('🍆', 1),
    ('🍅', 4),
    ('🍅', 5),
    ('🍅', 3),
]

with beam.Pipeline() as pipeline:

    sum_numbers = (
        pipeline
        | 'CreateNumberCollection' >> beam.Create(numbers)
        | 'SumNumbers' >> beam.CombineGlobally(sum)
        | 'PrintTotalSum' >> beam.Map(print) 
    )

    sum_per_key = (
        pipeline
        | 'CreateItensCollection' >> beam.Create(itens)
        | 'SumPerKey' >> beam.CombinePerKey(sum)
        | 'PrintTotalPerKey' >> beam.Map(print)
    )

