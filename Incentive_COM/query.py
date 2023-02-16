import pandas as pd
import numpy as np
import os
import pandas.io.sql as sqlio
import psycopg2
import calendar
import json


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