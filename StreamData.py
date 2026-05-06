import os
import pandas as pd
from sodapy import Socrata
from datetime import datetime, timedelta
from google.cloud import bigquery
import urllib.parse

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "adta5240f22pam-f3b757ff53fa.json"
bq_client = bigquery.Client()

table_id = "adta5240f22pam.adta_5240_project.dallas_arrest_data"

client = Socrata("www.dallasopendata.com", None)

latest_query = f"""
    SELECT MAX(upzdate) as latest_upzdate
    FROM `{table_id}`
"""
latest_result = bq_client.query(latest_query).result()
latest_datetime = [row.latest_upzdate for row in latest_result][0]
f_datetime = latest_datetime.strftime('%Y-%m-%d %H:%M:%S')
f_datetime

print(f"Latest upzdate in BigQuery: {f_datetime}")

# Define the SQL query with the date injected
query = f"""
    SELECT age,
           ageatarresttime,
           araction,
           aradow,
           ararrestdate,
           ararresttime,
           arbkdate,
           arcurrloc,
           arladdress,
           arlcity,
           arlcounty,
           arldistrict,
           arlzip,
           arpremises,
           arrestyr,
           arstate,
           arweapon,
           birthplace,
           drug,
           drugrelated,
           drugtype,
           employer,
           ethnic,
           eyes,
           hair,
           hcity,
           height,
           hstate,
           hzip,
           nickname,
           occupation,
           race,
           sex,
           tatoo,
           upzdate,
           weight
    WHERE upzdate > '{f_datetime}'
"""

# Encode the query string
safe_string = urllib.parse.quote_plus(query)

# Construct the full URL
url = f'https://www.dallasopendata.com/resource/sdr7-6v3j.csv?$query={safe_string}'

df = pd.read_csv(url)

# Clean and convert height (e.g., 5-11 → 5.11)
df['height'] = df['height'].astype(str).str.replace('-', ".", regex=False)
df['height'] = pd.to_numeric(df['height'], errors='coerce')

# Numeric fields (coerce invalids, fill NA if necessary)
df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)

# Boolean fields
for col in ['drugrelated', 'drug', 'nickname']:
    df[col] = df[col].astype(str).str.lower().map({'true': True, 'false': False})
    df[col] = df[col].fillna(False).astype(bool)

# Datetime fields with coercion
for col in ['upzdate', 'arbkdate', 'ararrestdate']:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Columns that should always be treated as string — even if they contain numbers
string_fields = [
    'arldistrict', 'arpremises', 'birthplace', 'drugtype', 'occupation',
    'tatoo', 'sex', 'arcurrloc', 'araction', 'employer', 'aradow', 'hstate',
    'hcity', 'ethnic', 'race', 'eyes', 'hair', 'arweapon', 'arlcounty',
    'arstate', 'arlcity', 'arladdress', 'ararresttime'
]

for col in string_fields:
    df[col] = df[col].astype(str)

# Ensure any remaining object-type columns are stringified
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].astype(str)


job = bq_client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"  # Ensures new data is appended
))

print("Data loaded to BigQuery successfully. ", df.shape)