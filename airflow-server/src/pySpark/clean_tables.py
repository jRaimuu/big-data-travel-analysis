from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from gcsfs import GCSFileSystem

'''
To run this file locally, enter this in the command line:
spark-submit \
    --jars gcs-connector-hadoop2-2.2.5-shaded.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    clean_tables.py
'''

def read_and_clean_csv(spark, file_path, output_path):
    """Reads CSVs, cleans them, and exports them"""
    print(f"Reading and cleaning {file_path}")
    df  = spark.read.csv(file_path, header=True, inferSchema=True)

    for column_name, column_type in df.dtypes:
        if column_type == "string":
            df = df.withColumn(column_name, trim(col(column_name)))

    output_file_path = f"{output_path}/{file_path.split('/')[-1].replace('.csv', '')}"
    
    df.write.csv(output_file_path, header=True, mode='overwrite')
    print(f'Cleaned all string type data for {file_path}, and exported to {output_file_path}')


def clean_data_tables():
    """Reads all CSVs from bucket to then get cleaned and exported"""
    BUCKET = "travel-analysis-bucket"
    spark = SparkSession.builder\
                        .appName("Clean CSV Data")\
                        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
                        .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
                        .getOrCreate()
    
    files = GCSFileSystem().ls(f"gs://{BUCKET}/source_data")
    csv_files_paths = [f"gs://{file}" for file in files if file.endswith(".csv")]
    output_path = f"gs://{BUCKET}/cleaned"

    for file_path in csv_files_paths:
        read_and_clean_csv(spark, file_path, output_path)

if __name__ == '__main__':
    clean_data_tables()