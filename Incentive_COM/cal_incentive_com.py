# Import library
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import os
import pandas.io.sql as sqlio
import calendar
from query import *

# read config
configure_path = os.path.join("config", "configure.json")
with open(configure_path) as config_file:
  config = json.load(config_file)

# Read the excel file name to extract date
file_name = os.listdir('com_file')[0]
month = file_name.split('_')[-2]
year = file_name.split('_')[-1].split(".")[0]

end_date_of_month = calendar.monthrange(int(year), int(month))[1]

start_date = f"{year}-{month}-01"
end_date = f"{year}-{month}-{end_date_of_month}"

# Get start date next month for SLOB
next_month = "0" + str(int(month[1]) + 1)
next_month_start_date = f"{year}-{next_month}-01"

# Get config
biwarehouse = config["biwarehouse"]
# host = biwarehouse["host"]
# port = biwarehouse["port"]
# dbname = biwarehouse["dbname"]
# user = biwarehouse["user"]
# password = biwarehouse["password"]

# get nmv and slob
df_nmv = query_nmv(start_date = start_date, end_date = end_date, db = biwarehouse)

df_slob_before = query_slob(date = start_date, db = biwarehouse)
df_slob_before = df_slob_before.rename(columns={'slob': 'slob_before'})

df_slob_after = query_slob(date = next_month_start_date, db = biwarehouse)
df_slob_after = df_slob_after.rename(columns={'slob': 'slob_before'})

df_slob = df_slob_after.merge(df_slob_after, how = "outer", on = "groupbrand")
df_slob.fillna(0)
df_slob["slob"] = df_slob["slob_after"] - df_slob["slob_before"]
df_slob = df_slob.drop(columns=['slob_before', 'slob_after'])

# read the excel file and add nmv and slob column
directory = "com_file"
path = os.path.join(directory, file_name)
df = pd.read_excel(path, "Detail")
df = df.iloc[9:,1:]

new_header = df.iloc[0] #grab the first row for the header
df = df[1:] #take the data less the header row
df.columns = new_header #set the header row as the df header

#take rows that contain specific 'Onboarded' in 'Store status'
df = df[df["Store status"] == "Onboarded"]

# Rename the cols
df.rename(columns={'Target NMV after': 'Target NMV','GP actual (Simulated)': 'GP actual', 'Target GP after (Simulated)': 'Target GP'}, inplace=True)