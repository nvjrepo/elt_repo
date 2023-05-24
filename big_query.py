
from google.cloud import bigquery
import pandas_gbq
import pandas as pd
import numpy as np
from google.oauth2 import service_account
import os

#Setting BQ credential in environment
os.environ.setdefault("GCLOUD_PROJECT", 'your-project')
service_account_file_path=os.environ.get("YOUR_PROJECT")

def connect_to_bq():
    client = bigquery.Client.from_service_account_json(service_account_file_path)
    return client

def bq_delete(schema,table_id,condition=''):
    client=connect_to_bq()
    if condition=='':
        print('no columns is removed')
    else:
        query = 'Delete from `pacc-raw-data.'+schema+'.'+table_id+'` where '+condition
        query_job = client.query(query)
        results = query_job.result()
        print(results)

def bq_query(query_string):
    client=connect_to_bq()
    query_job = client.query(query=query_string)
    results = query_job.result()
    print(results)

def bq_insert(schema,table_id,dataframe,condition='',unique_key='',job_config=bigquery.LoadJobConfig()):
    client=connect_to_bq()
    table_id_full = 'pacc-raw-data.'+schema+'.'+table_id
    job_config = job_config
    job_config._properties['load']['schemaUpdateOptions'] = ['ALLOW_FIELD_ADDITION']

    if dataframe.to_dict('records')==[]:
        print('No Insert')
    else:
        print('continue')
        
        #deduplication
        if unique_key=='':
            print('No dedup')
        else:
            dataframe=dataframe.drop_duplicates(subset=unique_key)
            print('Dedup completed')

        #load
        job = client.load_table_from_dataframe(
            dataframe, table_id_full, job_config=job_config
        )

        #remove column with id matches the inserted rows
        bq_delete(schema,table_id,condition=condition)
        job.result()
        table=client.get_table(table_id_full)
        print(str(
                "{} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        ))
    
    
def bq_pandas(query_string):
    credentials = service_account.Credentials.from_service_account_file(service_account_file_path)
    querry_bq=pandas_gbq.read_gbq(query_string, project_id="pacc-raw-data", credentials=credentials)
    return querry_bq

#BQ insert
def bq_insert_streaming(rows_to_insert,table_id,object):
    client=connect_to_bq()
    if rows_to_insert == []:
      print('stop')
    else:
      errors = client.insert_rows_json(table_id, rows_to_insert)   # Make an API request
      if errors == []:
          print("New rows have been added.")
      else:
          print("Encountered errors while inserting rows: {}".format(errors))
          f = open('script_ipos_crm/errors.txt', 'a')
          f.write(f"{object}\n")
          f.write(f"{errors}\n")
          f.close()
          print("data written")
          

def bq_latest_date(date_schema,schema,table_id):
    #finding latest date from BQ table
    df=bq_pandas(query_string='select max(cast('+date_schema+' as date)) as '+date_schema+' from `pacc-raw-data.'+schema+'.'+table_id+'`')
    print(df)
    if df[date_schema].astype(str).to_list()[0]=='NaT':
        recent_loaded_date='1970-01-01'
    else:
        recent_loaded_date=df[date_schema].astype(str).to_list()[0]
    return recent_loaded_date

def append_tables(schema_to_append,schema_appended,table_id):
    query_string='''
        SELECT column_name FROM pacc-raw-data.'''+schema_to_append+'''.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '''+"'"+table_id+"'"

    #Get columns name    
    df=bq_pandas(query_string=query_string)
    column_lists=df['column_name'].astype(str).to_list()
    columns=",\n".join(column_lists)
    
    #Insert Query
    query_string='''
      insert into `pacc-raw-data.'''+schema_appended+'''.'''+table_id+'''` (
        '''+columns+'''
      )
    
    select
        '''+columns+'''
    from
      `pacc-raw-data.'''+schema_to_append+'''.'''+table_id+'''`
    '''
    print(query_string)
    
    #BQ insert
    bq_query(query_string)
