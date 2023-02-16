# Import library
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import os
import pandas.io.sql as sqlio
# import psycopg2
import calendar
from query import *

# read config
configure_path = os.path.join("config", "configure.json")
with open(configure_path) as config_file:
  config = json.load(config_file)

# Read the excel file name to extract date
file_name = os.listdir('Incentive_CAL')[0]
month = file_name.split('_')[-2]
year = file_name.split('_')[-1]

end_date_of_month = calendar.monthrange(int(month), int(year))[1]

start_date = f"{year}-{month}-01"
end_date = f"{year}-{month}-{end_date_of_month}"

# Get start date next month for SLOB
next_month = "0" + str(int(month[1]) + 1)
next_month_start_date = f"{year}-{next_month}-01"

# Get config
biwarehouse = config["biwarehouse"]
host = biwarehouse[host]
port = biwarehouse[port]
dbname = biwarehouse[dbname]
user = biwarehouse[user]
password = biwarehouse[password]

# get nmv and slob
df_nmv = query_nmv(start_date, end_date)
print(df_nmv)