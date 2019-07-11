import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
import config as cfg
import random

job_name_unique = '{}-{}'.format(cfg.job_name_review, random.randint(1,100))

def json_processor(row):
    import json
    d = json.loads(row)
    return {'business_id': d['business_id'], 'review_id': d['review_id'], 'stars':d['stars'],
     'date':d['date'], 'cool':d['cool']}

options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = cfg.project_id
google_cloud_options.job_name = job_name_unique
google_cloud_options.staging_location = 'gs://{}/binaries'.format(cfg.bucket_name)
google_cloud_options.temp_location = 'gs://{}/temp'.format(cfg.bucket_name)
options.view_as(StandardOptions).runner = 'DataflowRunner'
google_cloud_options.region = cfg.region

p = beam.Pipeline(options=options)

(p | "read_from_gcs" >> beam.io.ReadFromText('gs://{}/{}'.format(cfg.bucket_name, cfg.review_json))
   | "json_processor" >> beam.Map(json_processor)
   | "write_to_bq" >> beam.io.Write(beam.io.gcp.bigquery.WriteToBigQuery(table=cfg.table_review, 
                                                       dataset=cfg.bigquery_dataset_name, 
                                                       project=cfg.project_id, 
                                                       schema=cfg.schema_review, 
                                                       create_disposition='CREATE_IF_NEEDED',
                                                       write_disposition='WRITE_TRUNCATE'))
)

p.run()