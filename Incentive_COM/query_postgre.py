import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import os
import pandas.io.sql as sqlio
import psycopg2
import calendar
import json

# read config
with open('config.json') as config_file:
  config = json.load(config_file)

# Read the excel file name to extract date
file_name = os.listdir('Incentive_CAL')[0]
month = file_name.split('_')[-2]
year = file_name.split('_')[-1]

end_date_of_month = calendar.monthrange(int(month), int(year))[1]

start_date = f"{year}-{month}-01"
end_date = f"{year}-{month}-{end_date_of_month}"

# Get start date next month for SLOB
month = "01"
next_month = "0" + str(int(month[1]) + 1)
next_month_start_date = f"{year}-{next_month}-01"

# Get config
postgre_config = config["postgre_db"]
host = postgre_config[host]
port = postgre_config[port]
dbname = postgre_config[dbname]
user = postgre_config[user]
password = postgre_config[password]

# Query NMV
def query_nmv(start_date, end_date):
    with psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, user, password)) as conn:
        sql_nmv = f"""
        SELECT groupbrand, sum(NMV)
        FROM sales.v_sales_mtd_v2
        WHERE date(orderdate + interval '7 hours') between '{start_date}'::date AND '{end_date}'::date;
        """
        df_nmv = pd.read_sql_query(sql_nmv, conn)
        return df_nmv

# Query SLOB
def query_slob(date):
    with psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, user, password)) as conn:
        sql_slob = f"""
        
        """
        df_slob = pd.read_sql_query(sql_slob, conn)
        return df_slob

def query_slob_start:
  return query_slob(start_date)

def query_slob_end:
  return query_slob(next_month_start_date)