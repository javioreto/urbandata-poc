import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
import config as cfg
import random

job_name_unique = '{}-{}'.format(cfg.job_name_analytics, random.randint(1,100))

class Printer(beam.DoFn):
    def process(self, element):
        print element

options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = cfg.project_id
google_cloud_options.job_name = job_name_unique
google_cloud_options.staging_location = 'gs://{}/binaries'.format(cfg.bucket_name)
google_cloud_options.temp_location = 'gs://{}/temp'.format(cfg.bucket_name)
options.view_as(StandardOptions).runner = 'DataflowRunner'
google_cloud_options.region = cfg.region

p = beam.Pipeline(options=options)

(p  | 'Businesses open past 21' >> beam.io.Read(beam.io.BigQuerySource(
        query='SELECT city, state, COUNT(*) AS total FROM `webs-clientes-1537993794326.dation_data_poc.urban_data_poc` '\
               'WHERE is_open = 1 AND '\
               'hours.Monday is not null AND CAST(SPLIT(SPLIT(hours.Monday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 AND '\
               'hours.Tuesday is not null AND CAST(SPLIT(SPLIT(hours.Tuesday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 AND '\
               'hours.Wednesday is not null AND CAST(SPLIT(SPLIT(hours.Wednesday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 AND '\
               'hours.Thursday is not null AND CAST(SPLIT(SPLIT(hours.Thursday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 AND '\
               'hours.Friday is not null AND CAST(SPLIT(SPLIT(hours.Friday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 AND '\
               'hours.Saturday is not null AND CAST(SPLIT(SPLIT(hours.Saturday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 AND '\
               'hours.Sunday is not null AND CAST(SPLIT(SPLIT(hours.Sunday,"-")[OFFSET(1)],":")[OFFSET(0)] AS INT64) > 21 '\
               'GROUP BY city, state',
        use_standard_sql=True))
    | 'Write output to file' >> WriteToText(file_path_prefix='gs://{}/{}'.format(cfg.bucket_name, cfg.output1),
                                             num_shards=1,
                                             header='Businesses open past 21')
)

p.run()
