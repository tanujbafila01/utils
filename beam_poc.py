import argparse
import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

parser = argparse.ArgumentParser()
parser.add_argument('--topic', default='projects/case-study-airflow/topics/tweepy_topic_dummy')
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


# Create the Pipeline with the specified options.
p = beam.Pipeline(options=options)

read_message = (p | beam.io.gcp.pubsub.ReadFromPubSub(topic=known_args.topic))
			   #| beam.ParDo(Split()))
			   
write_to_gs = (read_message | "write_to_storage">>WriteToText('gs://docker-airflow-test/output/poc_beam_'))

result = p.run()
result.wait_until_finish()