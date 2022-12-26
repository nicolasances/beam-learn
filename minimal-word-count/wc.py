import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file = 'input.md'
output_file = 'output.txt'

beam_options = PipelineOptions()

with beam.Pipeline(options=beam_options) as pipeline:

    pipeline | beam.io.ReadFromText(input_file)
        | 'ExtractWords' >> beam.FlatMap(lambda x : re.findall(r'[A-Za-z\']+', x))
        | beam.combiners.Count.PerElement()
        | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
        | beam.io.WriteToText(output_path)

    pipeline.run()
