from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkFiles
from datetime import date
import pandas as pd


today = date.today()
S3_DATA_SOURCE_PATH = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'+ today.strftime('%d-%m-%Y')+'.csv'
S3_DATA_OUTPUT_PATH = 's3://thach-bucket-123/covid19/' + today.strftime('%d-%m-%Y')
schema = StructType([ 
    StructField("FIPS", StringType(), True),
    StructField("Admin2", StringType(), True),
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Last_Update", StringType(), True),
    StructField("Lat", StringType(), True),
    StructField("Long_", StringType(), True),
    StructField("Confirmed", StringType(), True),
    StructField("Deaths", StringType(), True),
    StructField("Recovered", StringType(), True),
    StructField("Active", StringType(), True),
    StructField("Combined_Key", StringType(), True),
    StructField("Incident_Rate", StringType(), True),
    StructField("Case_Fatality_Ratio", StringType(), True),
 ])

def main():
    spark = SparkSession.builder.appName('Thach-demo-App').getOrCreate()
    df = pd.read_csv(S3_DATA_SOURCE_PATH).fillna('')
    all_data = spark.createDataFrame(df,schema=schema)
    all_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)

if __name__ == "__main__":
    main()
