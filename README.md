Urban Data Analytics

Getting started

Clone repository and move to main directory of project.
Install and configure Google Cloud SDK.
Take example data, decompress and upload to google cloud storage (change the ame of project, buckect, etc. in config.py file)


To run in localhost:

Install virtual env:
pip install virtualenv

Create virtual env:
virtualenv  ./env

Activate virtual env:
source env/bin/activate

Install python requierements:
pip install -r requirements.txt

Run scripts one by one:
To load in bigquery business file data.
python job_business.py 

To load in bigquery review file data.
python job_review.py

To execute analysis and output in result files.
python job_analysis.py

To deploy it in GAE Flex:

In the project main directory run this commando to deploy app y GAE Flex:
gcloud app deploy -> yes

Then run Cron scheduling file:

gcloud app deploy cron.yaml