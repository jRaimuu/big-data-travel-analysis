from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, trim

from dotenv import load_dotenv
from gcsfs import GCSFileSystem
from os import getenv


def load_bucket_files(bucket):
    """Takes a bucket name and loads all the files in a google cloud bucket"""
    return GCSFileSystem().ls(bucket)


def process_file(file_path):
    """Takes a table based csv file and cleans the string based columns"""
    print(f"Processing file: {file_path}")
    df = spark.read.csv(f"gs://{file_path}", header=True, inferSchema=True)
    
    for column_name, column_type in df.dtypes:
        if column_type == "string":
            df = df.withColumn(column_name, trim(col(column_name)))

    """Writing the file to an output would go here (df.write.csv)"""
    print(f"Processed and saved cleaned file: {file_path}")


def clean_data_tables():
    """Clean all string columns in bucket by iterating over all csv files"""
    spark = SparkSession.builder.appName("PublicGCSRead").getOrCreate()
    bucket = getenv('BUCKET_NAME')

    # Load all files
    files = load_bucket_files(bucket)
    csv_files = [file for file in files if file.endswith(".csv")]

    if not csv_files:
        raise ValueError("No CSV files found in the bucket.")

    # Parallelizes the files and processes them
    rdd_files = spark.sparkContext.parallelize(csv_files)
    rdd_files.foreach(process_files)

    print("All files processed successfully")

