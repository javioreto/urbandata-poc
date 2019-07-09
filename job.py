import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

project_id = 'webs-clientes-1537993794326'
bigquery_dataset_name = 'dation_data_poc' # needs to exist 
table_name = 'test_urban'
bucket_name = 'urbandata-poc'
json_file_gcs_path = 'gs://urbandata-poc/business.json'
schema = "business_id:STRING,name:STRING"

def json_processor(row):
    import json
    d = json.loads(row)
    return {'business_id': d['business_id'], 'name': d['name']}

options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = "myjob1"
google_cloud_options.staging_location = 'gs://{}/binaries'.format(bucket_name)
google_cloud_options.temp_location = 'gs://{}/temp'.format(bucket_name)
options.view_as(StandardOptions).runner = 'DataflowRunner'
google_cloud_options.region = "europe-west1"

p = beam.Pipeline(options=options)

(p | "read_from_gcs" >> beam.io.ReadFromText(json_file_gcs_path)
   | "json_processor" >> beam.Map(json_processor)
   | "write_to_bq" >> beam.io.Write(beam.io.gcp.bigquery.WriteToBigQuery(table=table_name, 
                                                       dataset=bigquery_dataset_name, 
                                                       project=project_id, 
                                                       schema=schema, 
                                                       create_disposition='CREATE_IF_NEEDED',
                                                       write_disposition='WRITE_EMPTY'))
)

p.run()