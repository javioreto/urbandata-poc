project_id = 'webs-clientes-1537993794326'
bigquery_dataset_name = 'dation_data_poc'
table_business = 'urban_business'
table_review = 'urban_review'
bucket_name = 'urbandata-poc'
business_json = 'business_temp.json'
review_json = 'review_temp.json'
region = 'europe-west1'

job_name_business = 'job-business'
job_name_review = 'job-review'
job_name_analytics = 'job-analytics'

output1 = 'output1.json'
output2 = 'output2.json'
output3 = 'output3.json'
output4 = 'output4.json'

schema_business = { "fields": [
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


schema_review = { "fields": [
 { "name": "review_id", "type": "STRING" },
 { "name": "business_id", "type": "STRING" },
 { "name": "stars", "type": "INTEGER" },
 { "name": "date", "type": "STRING" },
 { "name": "cool", "type": "INTEGER" }
] }  
