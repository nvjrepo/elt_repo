from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
import dateutil.relativedelta

def convert_unix_to_date(unix_dt):
    dt = datetime.fromtimestamp(unix_dt)
    date_dt=dt.date()
    return date_dt

def last_date_of_month(unix_dt):
    date_dt=convert_unix_to_date(unix_dt)
    last_date=date_dt+ relativedelta(day=31)
    return last_date

def unix_month_no(unix_dt):
    date_dt=convert_unix_to_date(unix_dt)
    month_number = date_dt.month
    return month_number

def last_unix_t_of_month(unix_dt):
    last_date=last_date_of_month(unix_dt)
    last_date_dt=datetime.strptime(str(last_date)+' 23:59:59', "%Y-%m-%d %H:%M:%S")
    
    last_date_unix= int(time.mktime(last_date_dt.timetuple()))
    print('last timestamp of month is: '+str(last_date_unix))
    return last_date_unix

def first_unix_t_of_last_month(unix_dt):
    date_dt=convert_unix_to_date(unix_dt)
    first_date=date_dt.replace(day=1)
    last_month_date = first_date + dateutil.relativedelta.relativedelta(months=-1)
    last_month_date_dt=datetime.strptime(str(last_month_date)+' 00:00:00', "%Y-%m-%d %H:%M:%S")
    first_date_unix= int(time.mktime(last_month_date_dt.timetuple()))
    print('first timestamp of last month is: '+str(first_date_unix))
    return first_date_unix
