import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# To create a pipeline, we need to instantiate the pipeline object, eventually pass some options, 
# and declaring the steps/transforms of the pipeline

# custom options
class MyOptions(PipelineOptions):
	@classmethod
	def _add_argparse_args(cls, parser):
		parser.add_argument('--input',
							help='Input for the pipeline',
							default='./data/')
		parser.add_argument('--output',
							help='Output for the pipeline',
						default='./output/')
						
class Split(beam.DoFn):
    def process(self, element):
        Date,Open,High,Low,Close,Volume, Adj_Close = element.split(",")
        return [{
            'Open': float(Open),
            'Close': float(Close),
        }]
		
# represent the data as a tuple so we can group by a key and then feed CombineValues with what it expects. 
# To do that we use a custom function “CollectOpen()” which returns a list of tuples containing (1, <open_value>)
class CollectOpen(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing Date and Open value
        result = [(1, element['Open'])]
        return result
	

def run(argv=None):
	parser = argparse.ArgumentParser()
	parser.add_argument('--input',
					  dest='input',
						default='./data/',
					  help='Input file to process.')
	parser.add_argument('--output',
					  dest='output',
					  # CHANGE 1/5: The Google Cloud Storage path is required
					  # for outputting the results.
					  default='./output/',
					  help='Output file to write results to.')
	known_args, pipeline_args = parser.parse_known_args(argv)
	pipeline_args.extend([
	  # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
	  # run your pipeline on the Google Cloud Dataflow Service.
	  '--runner=DirectRunner',
	  # CHANGE 3/5: Your project ID is required in order to run your pipeline on
	  # the Google Cloud Dataflow Service.
	  '--project=case-study-airflow',
	  # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
	  # files.
	  '--staging_location=gs://docker-airflow-test/test_beam_stg',
	  # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
	  # files.
	  '--temp_location=./data/tmp/',
	  '--job_name=beam_example',
	])	
	options = PipelineOptions()
	p = beam.Pipeline(options=options)

	# in beam, data is represented as pcollection object. so to start ingesting data, we need to read from the csv and store this as pcollection 
	# to which we can apply transformation. 
	# The read operation is considered as a transformation and follows the syntax of all transformations.
	# [Output PCollection] = [Input PCollection] | [Transform]

	# using default filepath
	#csv_lines = (p | beam.io.textio.ReadFromText('apl_stk_small.csv', skip_header_lines=1)
	#			# Beam has core methods (ParDo, Combine) that allows to apply a custom transform , but also has pre written transforms called composite transforms. 
	#			# In our example we will use the ParDo transform to apply our own functions.
	#			| beam.ParDo(Split())
	#			#Now that we have the data we need, we can use one of the standard combiners to calculate the mean over the entire PCollection.
	#			| beam.ParDo(CollectOpen())
	#			| "Grouping keys open" >> beam.GroupByKey()
	#			| "Calculating mean for open" >> beam.CombineValues(beam.combiners.MeanCombineFn())
	#			| beam.textio.WriteToText(file_name_suffix='apl_stk_small', header='Date,Open,High,Low,Close,Volume,Adj Close'))
	# 

	# chain everything together 
	# csv_lines = (
	#    p | beam.io.ReadFromText(input_filename) | 
	#    beam.ParDo(Split()) |
	#    beam.ParDo(CollectOpen()) |
	#    "Grouping keys Open" >> beam.GroupByKey() |
	#    "Calculating mean" >> beam.CombineValues(
	#        beam.combiners.MeanCombineFn()
	#    ) | beam.io.WriteToText(output_filename)
	#)

	csv_lines = (p | beam.io.textio.ReadFromText('./data/apl_stk_small.csv', skip_header_lines=1)
				# Beam has core methods (ParDo, Combine) that allows to apply a custom transform , but also has pre written transforms called composite transforms. 
				# In our example we will use the ParDo transform to apply our own functions.
				| beam.ParDo(Split()))

	mean_open = (csv_lines | beam.ParDo(CollectOpen())
				| "Grouping keys Open" >> beam.GroupByKey() 
				| "Calculating mean" >> beam.CombineValues(beam.combiners.MeanCombineFn())
				)
			
	output = (mean_open | beam.io.textio.WriteToText("./output/beam_test"))
	p.run()
	
if __name__=='__main__':
	run()