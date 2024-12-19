from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType
from gcsfs import GCSFileSystem

def create_or_replace_table():
    PARENT_PROJECT = ""

    spark = SparkSession.builder\
                        .appName("Upload to BigQuery")\
                        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
                        .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
                        .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.1') \
                        .config("parentProject", PARENT_PROJECT) \
                        .getOrCreate()
    
    BUCKET_NAME = "travel-analysis-bucket"
    INPUT_PATH = f"gs://{BUCKET_NAME}/aggregated/"
    REQUIRED_COLUMNS = set(["name", "year"])

    files = GCSFileSystem().ls(INPUT_PATH)

    # Create a list of the files that are not the main directory, add the gs prefix
    files_paths = [f"gs://{file}" for file in files if not file.endswith('/')]

    # Insert the private project and dataset ids
    project_id = PARENT_PROJECT
    dataset_id = ''
    
    for file in files_paths:
        df = spark.read.csv(file, header=True, inferSchema=True)
        
        # Create custom schema to make fields nullable
        schema = df.schema
        updated_fields = []
        for field in schema.fields:
            if field.name in REQUIRED_COLUMNS:
                updated_fields.append(StructField(field.name, field.dataType, nullable=False))
            else:
                updated_fields.append(StructField(field.name, field.dataType, nullable=True))
        
        updated_schema = StructType(updated_fields)
        df = spark.read.csv(file, header=True, schema=updated_schema)
        
        table_id = file.replace(INPUT_PATH, '')

        df.write\
            .format("bigquery") \
            .option("table", f"{project_id}:{dataset_id}.{table_id}") \
            .option("temporaryGcsBucket", BUCKET_NAME + "/tmp/") \
            .mode("overwrite") \
            .save()

        # Print for loging purposes
        print(f"Table {table_id} created in BigQuery")

    spark.stop()


if __name__ == '__main__':
    create_or_replace_table()