from pyspark.sql import SparkSession
from datetime import date


today = date.today()
S3_DATA_SOURCE_PATH = 's3://thach-bucket-123/covid19/' + today.strftime('%d-%m-%Y')

def main():
    spark = SparkSession.builder.appName('Thach-read-data').getOrCreate()
    df1 = spark.read.parquet(S3_DATA_SOURCE_PATH)
    df1.show(truncate=False)
    rows = df1.count()
    print(f"DataFrame Rows count : {rows}")

    # Get columns count
    cols = len(df1.columns)
    print(f"DataFrame Columns count : {cols}")

if __name__ == "__main__":
    main()
