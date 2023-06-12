from big_query import bq_insert_streaming,bq_query,bq_pandas,bq_insert,bq_delete
from mssql import mssql_query_pd
import requests
import pandas as pd
import datetime

def membership_data(brand,condition=''):
    database_name='IPOSS'+brand
    query_string='select distinct '+condition+' membership_id from '+database_name+'.dbo.sale'
    membership_data=mssql_query_pd(query_string)
    print('Total members are '+str(len(membership_data.index)))
    return membership_data

def crm_api(brand,user_id,table,page=0):
    p = {
      'access_token': 'YOUR_KEY',
      'pos_parent':brand,
      'user_id':user_id,
      'page':page
    }
    url="https://api.foodbook.vn/ipos/ws/xpartner/"+table+"?"

    #call API
    try:
        r = requests.get(url, params=p).json()['data']
    except:
        print('error')
        r=0
    
    return r

def crm_get_full_list(brand,table,page=0):
    df = membership_data(brand)
    user_id_list=df['membership_id'].to_list()
    
    raw_output=[]
    for user_id in user_id_list:
        print('get data for member_id:'+user_id)
        raw_output_member=crm_api(brand,user_id,table,page=0)
        raw_output_member['membership_id']=user_id

        if raw_output_member==0:
            print('no data')
        else:
            if type(raw_output_member) is dict:
                raw_output.append(raw_output_member)
            else:
                raw_output=raw_output+raw_output_member   

        for raw_output_dict in raw_output:
            raw_output_dict['loaded_date']=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'

    return raw_output
