from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType
from gcsfs import GCSFileSystem

def create_or_replace_table():
    spark = SparkSession.builder\
                        .appName("Upload to BigQuery")\
                        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
                        .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
                        .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.1') \
                        .config("parentProject", "phonic-sunbeam-443308-r6") \
                        .getOrCreate()
    
    BUCKET_NAME = "travel-analysis-bucket"
    # INPUT_PATH  = f"gs://{BUCKET_NAME}/aggregated/"
    REQUIRED_COLUMNS = set(["name", "year"])

    files = GCSFileSystem().ls("gs://travel-analysis-bucket/aggregated/")
    files_paths = [f"gs://{file}" for file in files if not file.endswith('/')]

    project_id = ''
    dataset_id = ''
    
    for file in files_paths:
        print(file)
        table_id = file.replace('gs://travel-analysis-bucket/aggregated/', '')
        print(table_id)
        df = spark.read.csv(file, header=True, inferSchema=True)
        
        schema = df.schema
        updated_fields = []
        for field in schema.fields:
            if field.name in REQUIRED_COLUMNS:
                updated_fields.append(StructField(field.name, field.dataType, nullable=False))
            else:
                updated_fields.append(StructField(field.name, field.dataType, nullable=True))
        
        df = spark.read.csv(file, header=True, schema=StructType(updated_fields))

        df.write\
            .format("bigquery") \
            .option("table", f"{project_id}:{dataset_id}.{table_id}") \
            .option("temporaryGcsBucket", BUCKET_NAME + "/tmp/") \
            .mode("overwrite") \
            .save()

        print(f"Table {table_id} created in BigQuery")

    spark.stop()


if __name__ == '__main__':
    create_or_replace_table()