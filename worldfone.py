import requests
import time
import pandas as pd
from datetime import datetime
from big_query import bq_insert,bq_delete,bq_pandas
from yml_extract import etract_variable_yml_string
from time_function import last_unix_t_of_month,convert_unix_to_date,unix_month_no,last_date_of_month


def get_worldfone_api(startdate,enddate,page=1):
    worldfone_key='yourkey'
    url='https://apps.worldfone.vn/externalcrm/getcdrs.php?secret='+str(worldfone_key)+'&startdate='+str(startdate)+'&enddate='+str(enddate)+'&page='+str(page)+'&pageSize=100'
    print(url)
    raw_output = requests.post(url).json()
    
    return raw_output

def worldfone_pd(start_date,end_date):

    #getting total pages
    data_to_insert=pd.DataFrame()
    raw_output = get_worldfone_api(startdate=start_date,enddate=end_date)
    total_pages=raw_output['max_page']+1
    print('Total page is '+str(total_pages))

    #concat df to all pages data
    for page in range(1,total_pages):
        raw_output = get_worldfone_api(startdate=start_date,enddate=end_date,page=page)
        df=pd.DataFrame(raw_output['data'])
        data_to_insert=pd.concat([data_to_insert,df])
        data_to_insert['loaded_date'] = pd.to_datetime('today')
    
    return data_to_insert

def worldfone_bq(schema,table_id):
    query_string="select max(unix_seconds(timestamp(calldate || ' UTC+7'))) as calldate FROM "+'`your_project.'+schema+'.'+table_id+'`'
    start_date=bq_pandas(query_string)['calldate'].astype(int).to_list()[0] + 1
    end_date = int(time.mktime(datetime.today().timetuple()))

    if unix_month_no(end_date)==unix_month_no(start_date):
        end_date=end_date
    else:
        end_date=last_unix_t_of_month(start_date)

    data_to_insert=worldfone_pd(start_date,end_date=end_date)

    if data_to_insert.to_dict('records')==[]:
        
        
        if last_date_of_month(start_date)==convert_unix_to_date(start_date) and end_date==last_unix_t_of_month(start_date):
                
                #if last date and end date fall into last date of month => switch range to next date of month
                start_date=end_date+1
                end_date=last_unix_t_of_month(start_date)
                data_to_insert=worldfone_pd(start_date,end_date=end_date)

                if data_to_insert.to_dict('records')==[]:
                    result='No Insert'
                    print('end')
                
                else:
                    print('continue')
                    #remove column with id matches the inserted rows from basevn
                    unique_key=data_to_insert['uniqueid']+data_to_insert['direction']
                    row_to_exclude="('"+"','".join(unique_key.to_list())+"')"
                    condition='concat(uniqueid,direction) in'+row_to_exclude
            
                    result=bq_insert(
                                schema,
                                table_id,
                                dataframe=data_to_insert,
                                condition=condition
                            )
                    print('end')

        else:
            result='No Insert'
            print('end')
        
    else:
        print('continue')
        #remove column with id matches the inserted rows from basevn
        unique_key=data_to_insert['uniqueid']+data_to_insert['direction']
        row_to_exclude="('"+"','".join(unique_key.to_list())+"')"
        condition='concat(uniqueid,direction) in'+row_to_exclude

        result=bq_insert(
                    schema,
                    table_id,
                    dataframe=data_to_insert,
                    condition=condition
                )
        print('end')

    print(start_date)
    print(end_date)
    print(result)
    return result

def worldfone_bq_historical(schema,table_id):
    today_unix = int(time.mktime(datetime.today().timetuple()))
    start_date=1614531599
    end_date=1614531600
    order=0
    while start_date <= today_unix:
        order=order+1
        print(order)
        start_date=end_date+1
        print('first timestamp of month is: '+str(start_date))
        end_date=last_unix_t_of_month(start_date)
        
        worldfone_bq(start_date=start_date,end_date=end_date,schema=schema,table_id=table_id)
        print(' ')
