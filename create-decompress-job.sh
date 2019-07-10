# Copy from local path dataset to gcp bucket
gsutil mv yelp_dataset.tar.gz gs://urbandata-poc

# Create decompress distributed job
gcloud dataflow jobs run decompress-job \
    --gcs-location gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files \
    --parameters \
inputFilePattern=gs://urbandata-poc/yelp_dataset.tar.gz,\
outputDirectory=gs://urbandata-poc/,\
outputFailureFile=gs://urbandata-poc/failed.csv