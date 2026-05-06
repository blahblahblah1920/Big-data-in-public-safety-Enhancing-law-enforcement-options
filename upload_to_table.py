from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
import pandas as pd
from sodapy import Socrata
import urllib
from pyspark.sql.types import *
from datetime import datetime

# Create Spark session
spark = SparkSession.builder \
    .appName("Update arrest_data_dallas") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Step 1: Get the max upzdate from existing table
    df_existing = spark.table("arrest_data_dallas")
    max_upzdate = df_existing.agg(max(col("upzdate"))).collect()[0][0]
    
    # Convert max_upzdate to datetime if it's not already
    if isinstance(max_upzdate, str):
        max_upzdate = datetime.strptime(max_upzdate, '%Y-%m-%d %H:%M:%S')
    
    # Format date for the query
    date_for_query = max_upzdate.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Maximum upzdate in table: {date_for_query}")
    
    # Step 2: Query new data from Dallas Open Data API
    client = Socrata("www.dallasopendata.com", None)
    
    # Define the SQL query with the date injected
    query = f"""
        SELECT age,
               occupation,
               drugrelated,
               tatoo,
               birthplace,
               drugtype,
               drug,
               sex,
               height,
               nickname,
               arcurrloc,
               hzip,
               upzdate,
               araction,
               employer,
               arpremises,
               aradow,
               arldistrict,
               arrestyr,
               hstate,
               hcity,
               ethnic,
               race,
               eyes,
               hair,
               ageatarresttime,
               arweapon,
               arlcounty,
               arstate,
               arlcity,
               arlzip,
               arladdress,
               arbkdate,
               ararresttime,
               ararrestdate,
               weight
        WHERE upzdate > '{date_for_query}'
    """
    
    # Encode the query string
    safe_string = urllib.parse.quote_plus(query)
    
    # Construct the full URL
    url = f'https://www.dallasopendata.com/resource/sdr7-6v3j.csv?$query={safe_string}'
    
    # Read data using pandas
    df_pandas = pd.read_csv(url)
    df_pandas = df_pandas.where(pd.notnull(df_pandas), None)
    
    if len(df_pandas) > 0:
        # Step 3: Clean and transform the pandas dataframe

        # Clean and convert height (e.g., 5-11 → 5.11)
        df_pandas['height'] = df_pandas['height'].astype(str).str.replace('-', ".", regex=False)
        df_pandas['height'] = pd.to_numeric(df_pandas['height'], errors='coerce')

        # Numeric fields (coerce invalids, fill NA if necessary)
        df_pandas['age'] = pd.to_numeric(df_pandas['age'], errors='coerce').fillna(0).astype(int)
        df_pandas['weight'] = pd.to_numeric(df_pandas['weight'], errors='coerce').fillna(0).astype(float)
        df_pandas['ageatarresttime'] = pd.to_numeric(df_pandas['ageatarresttime'], errors='coerce').fillna(0).astype(int)
        df_pandas['arrestyr'] = pd.to_numeric(df_pandas['arrestyr'], errors='coerce').fillna(0).astype(int)
        df_pandas['arlzip'] = pd.to_numeric(df_pandas['arlzip'], errors='coerce').fillna(0).astype(int)
        df_pandas['hzip'] = pd.to_numeric(df_pandas['hzip'], errors='coerce').fillna(0).astype(int)

        # Boolean fields
        for col in ['drugrelated', 'drug', 'nickname']:
            df_pandas[col] = df_pandas[col].astype(str).str.lower().map({'true': True, 'false': False})
            df_pandas[col] = df_pandas[col].fillna(False).astype(bool)

        # Datetime fields with coercion
        for col in ['upzdate', 'arbkdate', 'ararrestdate']:
            df_pandas[col] = pd.to_datetime(df_pandas[col], errors='coerce')

        # Columns that should always be treated as string — even if they contain numbers
        string_fields = [
            'arldistrict', 'arpremises', 'birthplace', 'drugtype', 'occupation',
            'tatoo', 'sex', 'arcurrloc', 'araction', 'employer', 'aradow', 'hstate',
            'hcity', 'ethnic', 'race', 'eyes', 'hair', 'arweapon', 'arlcounty',
            'arstate', 'arlcity', 'arladdress', 'ararresttime'
        ]

        for col in string_fields:
            df_pandas[col] = df_pandas[col].astype(str)

        # Ensure any remaining object-type columns are stringified
        for col in df_pandas.select_dtypes(include='object').columns:
            df_pandas[col] = df_pandas[col].astype(str)

        
        # df_pandas['height'] = df_pandas['height'].str.replace('-', ".")
        # df_pandas['height'] = df_pandas['height'].astype(float)
        # df_pandas['age'] = df_pandas['age'].astype(int)
        # df_pandas['drugrelated'] = df_pandas['drugrelated'].astype(bool)
        # df_pandas['drug'] = df_pandas['drug'].astype(bool)
        # df_pandas['nickname'] = df_pandas['nickname'].astype(bool)
        # df_pandas['upzdate'] = pd.to_datetime(df_pandas['upzdate'])
        # df_pandas['arbkdate'] = pd.to_datetime(df_pandas['arbkdate'])
        # df_pandas['ararrestdate'] = pd.to_datetime(df_pandas['ararrestdate'])
        # df_pandas['arldistrict'] = df_pandas['arldistrict'].astype(str)
        # df_pandas['arpremises'] = df_pandas['arpremises'].astype(str)
        # df_pandas['birthplace'] = df_pandas['birthplace'].astype(str)
        # df_pandas['drugtype'] = df_pandas['drugtype'].astype(str)
        # df_pandas['occupation'] = df_pandas['occupation'].astype(str)
        # df_pandas['tatoo'] = df_pandas['tatoo'].astype(str)
        # df_pandas['sex'] = df_pandas['sex'].astype(str)
        # df_pandas['arcurrloc'] = df_pandas['arcurrloc'].astype(str)
        # df_pandas['araction'] = df_pandas['araction'].astype(str)
        # df_pandas['employer'] = df_pandas['employer'].astype(str)
        # df_pandas['aradow'] = df_pandas['aradow'].astype(str)
        # df_pandas['arldistrict'] = df_pandas['arldistrict'].astype(str)
        # df_pandas['hstate'] = df_pandas['hstate'].astype(str)
        # df_pandas['hcity'] = df_pandas['hcity'].astype(str)
        # df_pandas['ethnic'] = df_pandas['ethnic'].astype(str)
        # df_pandas['race'] = df_pandas['race'].astype(str)
        # df_pandas['eyes'] = df_pandas['eyes'].astype(str)
        # df_pandas['hair'] = df_pandas['hair'].astype(str)
        # df_pandas['arweapon'] = df_pandas['arweapon'].astype(str)
        # df_pandas['arlcounty'] = df_pandas['arlcounty'].astype(str)
        # df_pandas['arstate'] = df_pandas['arstate'].astype(str)
        # df_pandas['arlcity'] = df_pandas['arlcity'].astype(str)
        # df_pandas['arladdress'] = df_pandas['arladdress'].astype(str)
        # df_pandas['ararresttime'] = df_pandas['ararresttime'].astype(str)
        
        # Convert pandas dataframe to Spark dataframe
        spark_df = spark.createDataFrame(df_pandas)
        
        # Step 4: Append new data to the existing table
        # The insertInto method appends data to an existing table
        spark_df.write.mode("append").insertInto("arrest_data_dallas")
        
        rows_updated = spark_df.count()
        print(f"Number of rows updated: {rows_updated}")
        
        # Verify the update
        new_max_upzdate = str(spark.table("arrest_data_dallas").agg(max(col("upzdate"))).collect()[0][0])
        print(f"New maximum upzdate after update: {new_max_upzdate}")
        
    else:
        print("No new data found to update.")
        
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    spark.stop()