from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

def bucket_sparkdf():
    """
    serves to demonstrate how to load data from a gcs bucket into a spark dataframe
    """

    BUCKET_NAME = "travel-analysis-bucket"
    INPUT_PATH  = f"gs://{BUCKET_NAME}/aggregated"

    spark = SparkSession.builder \
        .appName('GCSFilesRead') \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()



    # load data from the public GCS bucket
    climate_year=spark.read.csv(INPUT_PATH + "/climate_year", header=True, inferSchema=True)
    health_econ_year=spark.read.csv(INPUT_PATH + "/health_econ_year", header=True, mode="overwrite")
    tourism_year=spark.read.csv(INPUT_PATH + "/tourism_year", header=True, mode="overwrite")
    cultural=spark.read.csv(INPUT_PATH + "/cultural", header=True, mode="overwrite")
    spark_df = spark.read.csv(f"gs://{BUCKET_NAME}/{FILE_NAME}", header=True, inferSchema=True)
    # spark_df.printSchema()
    spark_df.show()

    # rename column
    spark_df = spark_df.withColumnRenamed("Annual COâ\x82\x82 emissions", "Annual_CO2_emissions")

    df_average_co2 = (
        spark_df
        .groupBy("Entity")
        .agg(F.max("Annual_CO2_emissions").alias("max_CO2_emissions"))
        .sort("max_CO2_emissions")
    )
    print(df_average_co2.show())

    # writing to GCS
    df_average_co2.write.csv(INPUT_PATH, header=True, mode="overwrite")


bucket_sparkdf()