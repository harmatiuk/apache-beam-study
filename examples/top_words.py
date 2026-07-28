import apache_beam as beam

from beam_study.transforms.print_output import Output

with beam.Pipeline() as pipeline:
    
    input = pipeline | 'ReadText' >> beam.io.ReadFromText('gs://apache-beam-samples/shakespeare/kinglear.txt')
    
    split_words = input | 'Split' >> beam.FlatMap(lambda line: line.split())

    filter = split_words | 'Filter ' >> beam.Filter(lambda word: not word.isspace() or word.isalnum())

    counter = filter | 'Counter' >> beam.combiners.Count.PerElement()
    
    top_count = counter | 'TopList' >> beam.combiners.Top.Largest(n=10, key=lambda pair: pair[1])

    top_count | 'PrintToCount' >> Output()