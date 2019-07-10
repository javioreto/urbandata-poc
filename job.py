import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

project_id = 'webs-clientes-1537993794326'
bigquery_dataset_name = 'dation_data_poc'
table_name = 'test_urban2'
bucket_name = 'urbandata-poc'
json_file_gcs_path = 'gs://urbandata-poc/business_temp.json'
schema = { "fields": [
 { "name": "business_id", "type": "STRING" },
 { "name": "name", "type": "STRING" }, 
 { "name": "address", "type": "STRING" },
 { "name": "city", "type": "STRING" },
 { "name": "state", "type": "STRING" },
 { "name": "postal_code", "type": "INTEGER" },
 { "name": "is_open", "type": "BOOLEAN" },
 { "name": "hours", "type": "RECORD", "fields" : [
    { "name": "Monday", "type": "STRING" },
    { "name": "Tuesday", "type": "STRING" },
    { "name": "Wednesday", "type": "STRING" },
    { "name": "Thursday", "type": "STRING" },
    { "name": "Friday", "type": "STRING" },
    { "name": "Saturday", "type": "STRING" },
    { "name": "Sunday", "type": "STRING" }
    ] }  
] }  

def json_processor(row):
    import json
    d = json.loads(row)
    return {'business_id': d['business_id'], 'name': d['name'], 'address':d['address'],
     'city':d['city'], 'state':d['state'], 'postal_code':d['postal_code'], 'is_open':d['is_open'], 
     'hours.Monday':d['hours']['Monday'], 'hours.Tuesday':d['hours']['Tuesday'], 'hours.Wednesday':d['hours']['Wednesday'], 
     'hours.Thursday':d['hours']['Thursday'], 'hours.Friday':d['hours']['Friday'], 'hours.Saturday':d['hours']['Saturday'],
     'hours.Sunday':d['hours']['Sunday']}

class Printer(beam.DoFn):
    def process(self, element):
        print element


options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = "myjob11"
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
                                                       write_disposition='WRITE_TRUNCATE'))
)

x = beam.Pipeline(options=options)

(x  | 'QueryTableStdSQL' >> beam.io.Read(beam.io.BigQuerySource(
        query='SELECT city FROM '\
              '`webs-clientes-1537993794326.dation_data_poc.test_urban2` GROUP BY 1',
        use_standard_sql=True))
    # Each row is a dictionary where the keys are the BigQuery columns
    | "Print for now" >> beam.ParDo(Printer())
)

result = p.run()
result.wait_until_finish()

google_cloud_options.job_name = "myjob33"
x.run()