#!pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
from google.cloud import bigquery
import json
import requests
from requests.auth import HTTPBasicAuth
from oauth2client.service_account import ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow

from __future__ import print_function
from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools

import pandas as pd
import hashlib

##SCOPES = "https://www.googleapis.com/auth/forms.body.readonly"
##DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"
SCOPES = "https://www.googleapis.com/auth/forms.responses.readonly"
DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

form_id = '1fEjjhj9cdm9BODlpScT8ElhotfbHWKrvnXA5BE0bZEQ'

store = file.Storage('token.json')
creds = None

if not creds or not creds.valid:
  if creds and creds.expired and creds.refresh_token:
    creds.refresh(Request())
  else:
    flow = InstalledAppFlow.from_client_secrets_file('client_secrets.json', SCOPES)
    creds = flow.run_console()

service = discovery.build('forms', 'v1', credentials=creds)

form_id = '1fEjjhj9cdm9BODlpScT8ElhotfbHWKrvnXA5BE0bZEQ'
# Return form response
result = service.forms().responses().list(formId=form_id).execute()
responses = result["responses"]
#respondId = responses["responseId"]
print(responses)

# Create the dataframe
df = pd.DataFrame(responses)
df.head()

c=df['answers']
e=pd.json_normalize(c)
final_dataset = df.join(e).drop(columns='answers',axis=1)
final_dataset.head()

a = df["responseId"]
b = df["lastSubmittedTime"]

id = b+a
unique_id = pd.DataFrame(id.apply(lambda x: hashlib.md5(x.encode()).hexdigest()))
print(unique_id)

final_dataset =final_dataset.join(unique_id).rename(columns={0:'unique_id'})

project_id = 'pacc-raw-data'
##table_id = 'test1'
table_id = 'pacc-raw-data.test1.test_form_integration'
my_table = 'test_form_integration'
json_key = 'key.json'
 
client = bigquery.Client.from_service_account_json(json_key)

query_string = 'SELECT distinct unique_id FROM `pacc-raw-data.test1.test_form_integration` where outlet_code is not null;'
dfss = pd.read_gbq(query_string, project_id=project_id)

check_update = dfss["unique_id"]
check_update.head()

final_dataset=final_dataset[~final_dataset.unique_id.isin(check_update)]

convert_dict = {"41198605.textAnswers.answers": str
                }
 
final_dataset = final_dataset.astype(convert_dict)
dataset_to_choose = final_dataset[['responseId','lastSubmittedTime','41198605.textAnswers.answers','unique_id']].rename(columns={"41198605.textAnswers.answers": "outlet_code"})
rows_to_insert = dataset_to_choose.to_dict('records')
print(rows_to_insert)

if rows_to_insert == []:
  print('stop')
else:
  errors = client.insert_rows_json(table_id, rows_to_insert)   # Make an API request
  if errors == []:
      print("New rows have been added.")
  else:
      print("Encountered errors while inserting rows: {}".format(errors))
