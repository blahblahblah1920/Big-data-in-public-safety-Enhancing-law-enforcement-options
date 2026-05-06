from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# Create Spark session
spark = SparkSession.builder \
    .appName("Find Max Upzdate from arrest_data_dallas") \
    .getOrCreate()

try:
    # Access the table directly by name
    df = spark.table("arrest_data_dallas")
    
    # Find the maximum value of the upzdate column
    max_upzdate = df.agg(max(col("upzdate"))).collect()[0][0]
    print(f"Maximum upzdate value: {max_upzdate}")

    
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    spark.stop()