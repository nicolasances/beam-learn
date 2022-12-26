import apache_beam as beam
import re
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import SetupOptions

def main(argv=None, save_main_session=True):

    input_file = 'input.md'
    output_file = 'output.txt'

    beam_options = PipelineOptions()

    with beam.Pipeline(options=beam_options) as pipeline:

        lines = pipeline | beam.io.ReadFromText(input_file)

        counts = (
            lines
            | "Split"  >> (
                beam.FlatMap(
                    lambda x : re.findall(r'[A-Za-z\']+', x)
                ).with_output_types(str)
            )
            | "PairWithOne" >> beam.Map(lambda x : (x, 1))
            | "GroupAndSum" >> beam.CombinePerKey(sum)
        )

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)
        
        output = counts | 'Format' >> beam.Map(format_result)

        output | WriteToText(output_file)

if __name__ == '__main__':
  main()