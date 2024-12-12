from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, trim

from dotenv import load_dotenv
from gcsfs import GCSFileSystem
from os import getenv, path
from dotenv import load_dotenv


def clean_data_tables():
    """Clean all string columns in bucket by iterating over all csv files"""
    dotenv_path = path.abspath(path.join(path.dirname(__file__), '../..', '.env'))
    load_dotenv(dotenv_path)
    bucket = getenv('BUCKET_NAME')

    spark_csv = SparkSession.builder\
                        .appName("Clean DSV Data")\
                        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
                        .getOrCreate()

    # Load all files
    files = GCSFileSystem().ls(f"gs://{bucket}/")
    csv_files_paths = [f"gs://{file}" for file in files if file.endswith(".csv")]

    if not csv_files_paths:
        raise ValueError("No CSV files found in the bucket.")
    
    print("Succesfully read files:")
    for i, name in enumerate(csv_files_paths):
        print(f"{i}. {name}")

    # Passing a list to read.csv reads them in parallel
    df = spark_csv.read.csv(csv_files_paths, header=True, inferSchema=True)

    for column_name, column_type in df.dtypes:
        if column_type == "string":
            df = df.withColumn(column_name, trim(col(column_name)))

    print(df.take(2))

if __name__ == '__main__':
    clean_data_tables()