import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
    '--input_topic', default='projects/case-study-airflow/topics/tweepy_topic_dummy',
    help=('Output PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
    parser.add_argument('--region', default='asia-south1')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'case-study-airflow'
    google_cloud_options.job_name = 'poc'
    google_cloud_options.staging_location = 'gs://docker-airflow-test/staging'
    google_cloud_options.temp_location = 'gs://docker-airflow-test/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(StandardOptions).streaming = 'true'
    p = beam.Pipeline(options=pipeline_options)
    
    # Read from PubSub into a PCollection.
    
    
    messages = (p
            | beam.io.ReadFromPubSub(topic=known_args.input_topic)
            .with_output_types(bytes))
    
    lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
    
    # Count the occurrences of each word.
    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))
    
    counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                        .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | beam.WindowInto(window.FixedWindows(15, 0))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones))
    
    ## Format the counts into a PCollection of strings.
    #def format_result(word_count):
    #  (word, count) = word_count
    #  return '%s: %d' % (word, count)
    #
    #output = (counts
    #          | 'format' >> beam.Map(format_result)
    #          | 'encode' >> beam.Map(lambda x: x.encode('utf-8'))
    #          .with_output_types(bytes))
    #
    ## Write to PubSub.
    ## pylint: disable=expression-not-assigned
    #output | beam.io.WriteToPubSub(known_args.output_topic)
    
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()