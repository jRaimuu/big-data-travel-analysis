from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


'''
To run this file locally, enter this in the command line:
spark-submit \
    --jars gcs-connector-hadoop2-2.2.5-shaded.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    bucket_to_spark.py
'''

def bucket_sparkdf():
    """
    serves to demonstrate how to load data from a gcs bucket into a spark dataframe
    """

    BUCKET_NAME = "travel-analysis-bucket"
    INPUT_PATH  = f"gs://{BUCKET_NAME}/aggregated"

    spark = SparkSession.builder \
        .appName('GCSFilesRead') \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
        .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
        .getOrCreate()



    # load data from the public GCS bucket
    climate_year=spark.read.csv(INPUT_PATH + "/climate_year", header=True, inferSchema=True)
    health_econ_year=spark.read.csv(INPUT_PATH + "/health_econ_year", header=True, inferSchema=True)
    tourism_year=spark.read.csv(INPUT_PATH + "/tourism_year", header=True, inferSchema=True)    #join cont + year^
    cultural=spark.read.csv(INPUT_PATH + "/cultural", header=True, inferSchema=True)        #Join my country
    
    # Use inner joins
    ml_features = climate_year\
            .join(
                health_econ_year,
                (climate_year.name == health_econ_year.name) & (climate_year.year == health_econ_year.year), 
                'inner')\
            .drop(climate_year.country_code, health_econ_year.name, health_econ_year.country_code, health_econ_year.year) \
            .join(
                tourism_year,
                (climate_year.name == tourism_year.name) & (climate_year.year == tourism_year.year), 
                'inner')\
            .drop(tourism_year.name, tourism_year.country_code, tourism_year.year) \
            .join(
                cultural,
                (climate_year.name == cultural.name), 
                'inner')\
            .drop(cultural.name)

    print(ml_features.show())
    
    ml_features.write.csv(INPUT_PATH + "/ml_features", header=True, mode="overwrite")


bucket_sparkdf()
