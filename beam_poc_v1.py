import argparse
import logging
import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

parser = argparse.ArgumentParser()
parser.add_argument('--topic', default='projects/case-study-airflow/topics/tweepy_topic_dummy')
parser.add_argument('--output', default='gs://docker-airflow-test/output/poc_beam_')
parser.add_argument('--output_table', default='ds_dummy.stock_data')
known_args, pipeline_args = parser.parse_known_args()
print(known_args.topic)
options = PipelineOptions(pipeline_args)

# For Cloud execution, set the Cloud Platform project, job_name,
# staging location, temp_location and specify DataflowRunner.
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'case-study-airflow'
google_cloud_options.job_name = 'poc'
google_cloud_options.staging_location = 'gs://docker-airflow-test/staging'
google_cloud_options.temp_location = 'gs://docker-airflow-test/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(StandardOptions).streaming = 'true'

#TABLE_SCHEMA = ('DATE:DATE, Open:FLOAT, High:FLOAT, Low:FLOAT, Close:FLOAT, Volume:INTEGER, Adj_close:FLOAT')


class ParseEvents(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        import json
        #print(element)
        #return [element]
        logging.info(element[0] + ' : ' + str(window), element[1])
        element_json = json.loads(decoded_data)
        message = element_json.get('messages').split('\n')
        for row in  x[1:]:
            rd = row.split(',')
            yield {'Date':rd[0],
                    'Open':rd[1],
                    'High':rd[2],
                    'Low':rd[3],
                    'Close':rd[4],
                    'Volume':rd[5],
                    'Adj_close':rd[6]}
        
# Create the Pipeline with the specified options.
p = beam.Pipeline(options=options)

read_message = (p | beam.io.gcp.pubsub.ReadFromPubSub(topic=known_args.topic))
               #| beam.ParDo(Split()))
               
parse_elements = (read_message | 'parse_elements'>>beam.ParDo(ParseEvents())
                               | 'FixedWindow' >> beam.WindowInto(beam.window.FixedWindows(120))
                               )
#write_to_gs = (read_message | "write_to_storage">>WriteToText('gs://docker-airflow-test/output/poc_beam_'))

result = p.run()
result.wait_until_finish()
