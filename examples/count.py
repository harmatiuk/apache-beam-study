import apache_beam as beam

from beam_study.transforms.print_output import Output


icons = ['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽']

icons_per_key = [
    ('spring', '🍓'),
    ('spring', '🥕'),
    ('summer', '🥕'),
    ('fall', '🥕'),
    ('spring', '🍆'),
    ('winter', '🍆'),
    ('spring', '🍅'),
    ('summer', '🍅'),
    ('fall', '🍅'),
    ('summer', '🌽')
]

with beam.Pipeline() as pipeline:

    icons_collection = pipeline | 'CreateIconsPCollection' >> beam.Create(icons)
    
    (
        icons_collection
        | 'CountTotalIcons' >> beam.combiners.Count.Globally()
        | 'PrintCountIcons' >> Output(prefix='Total icons: ')
    )

    (
        icons_collection
        | 'CountPerElement' >> beam.combiners.Count.PerElement()
        | 'PrintCountPerElement' >> Output(prefix='Total per element: ')
    )

    icons_per_key_collection = (
        pipeline
        | 'CreateIconsPerKeyPCollection' >> beam.Create(icons_per_key)
        | 'CountPerKey' >> beam.combiners.Count.PerKey()
        | 'PrintCountPerKey' >> Output(prefix='Total per key: ')
    )
