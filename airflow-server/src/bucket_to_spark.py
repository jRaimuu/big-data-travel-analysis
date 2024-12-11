from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

'''
To run this file locally, enter this in the command line:
spark-submit \
    --jars gcs-connector-hadoop2-2.2.5-shaded.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    bucket_to_spark.py
'''
load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
FILE_NAME = os.getenv("FILE_NAME")

spark = SparkSession.builder \
    .appName('GCSFilesRead') \
    .getOrCreate()

# load data from the public GCS bucket
df = spark.read.csv(f"gs://{BUCKET_NAME}/{FILE_NAME}", header=True, inferSchema=True)

df.printSchema()
df.show()
