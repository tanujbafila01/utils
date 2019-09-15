# This works for storing to gcs and big query
from __future__ import absolute_import
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.trigger import AfterEach, AccumulationMode


TABLE_SCHEMA = ('DATE:DATE, Open:FLOAT, High:FLOAT, Low:FLOAT, Close:FLOAT, Volume:INTEGER, Adj_close:FLOAT')

#class IndexAssigningStatefulDoFn(beam.DoFn):
#  INDEX_STATE = CombiningStateSpec('index', sum)
#
#  def process(self, element, index=DoFn.StateParam(INDEX_STATE)):
#    unused_key, value = element
#    current_index = index.read()
#    yield (value, current_index)
#    index.add(1)


class WriteToGCS(beam.DoFn):
    def __init__(self):
        self.outdir = "gs://docker-airflow-test/output/poc_beam"

    def process(self, element):
        from apache_beam.io.filesystems import FileSystems # needed here
        from datetime import datetime
        import json
        ts =  datetime.now().strftime('%Y%m%d%H%M%S')
        json_data = json.loads(element)
        writer = FileSystems.create(self.outdir + ts + '.csv', 'text/plain')
        mapped = map(lambda x: x.split(','), json_data['messages'].split('\n'))
        for item in list(mapped):
            writer.write(str(','.join(list(item))))
            writer.write('\n')
        writer.close()


class ParseEvents(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # element is of dict type
        import json
        data = []
        logging.info(element)
        element = json.loads(element)
        message = element['messages'].split('\n')
        logging.info(message)
        for row in  message[1:]:
            rd = row.split(',')
            data.append({'Date':rd[0],
                    'Open':rd[1],
                    'High':rd[2],
                    'Low':rd[3],
                    'Close':rd[4],
                    'Volume':rd[5],
                    'Adj_close':rd[6]})
        return data

def run(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--output', default='./output/poc_beam_')
    parser.add_argument('--topic', default='projects/case-study-airflow/topics/tweepy_topic_dummy')
    parser.add_argument('--output_table', default='case-study-airflow:ds_dummy.stock_data')
    args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'case-study-airflow'
    google_cloud_options.job_name = 'poc-1'
    google_cloud_options.staging_location = 'gs://docker-airflow-test/staging'
    google_cloud_options.temp_location = 'gs://docker-airflow-test/temp'
    
    p = beam.Pipeline(options=options)
    read_events = (p|'Read from PubSub' >> beam.io.ReadFromPubSub(topic=args.topic))     # list of dictionaries
    
    write_bq = (read_events |'parse input' >> beam.ParDo(ParseEvents())
                            |'write to bq' >> beam.io.WriteToBigQuery(
                                                args.output_table,
                                                schema=TABLE_SCHEMA,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    
    write_gs = (read_events |'write to gs' >> beam.ParDo(WriteToGCS()))
    
    result = p.run()
    result.wait_until_finish()
    

if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()