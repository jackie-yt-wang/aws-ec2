import requests
import pandas as pd
import os
from datetime import datetime
import re
import boto3
import toml


#############
app_config = toml.load('config.toml')
url = app_config['api']['url']
filename = app_config['run']['filename']
bucketname= app_config['aws']['bucketname']
foldername= app_config['aws']['foldername']

#############

session = boto3.Session()
s3 = session.resource('s3')
s3_client = session.client('s3')

response = requests.request("GET", url)
results = response.json()['results']
resultsList=[]
for post in results:
    companyname = post['company']['name']
    try:
        city = post['locations'][0]['name'].split(',')[0]
        country = post['locations'][0]['name'].split(',')[1]
    except:
        cityy=''
        country=''
    jobname = post['name']
    jobtype = post['type']
    pubdate = pd.to_datetime(post['publication_date']).strftime('%Y-%m-%d')

    resultsList.append({'Publication Date':pubdate,'Company Name':companyname,'Country':country,'City':city
     ,'Job Name':jobname,'Job Type':jobtype})

resultsDF = pd.DataFrame(resultsList)
os.mkdir('output')
resultsDF.to_csv('output/'+filename,index=False)
s3_client.upload_file(filename, bucketname, foldername+filename)
