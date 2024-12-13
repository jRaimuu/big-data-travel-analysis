from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    FILE_NAME = "annual_co2_emissions.csv"
    OUTPUT_PATH  = f"gs://{BUCKET_NAME}/test_dataproc"

    spark = SparkSession.builder \
        .appName('GCSFilesRead') \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()


    # load data from the public GCS bucket
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
    df_average_co2.write.csv(OUTPUT_PATH, header=True, mode="overwrite")


bucket_sparkdf()