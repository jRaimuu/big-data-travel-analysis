from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, trim

from dotenv import load_dotenv
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
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
                        .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
                        .getOrCreate()

    # Read the data from our wolrd in data as it all has the same schema
    annual_co2 = spark_csv.read.csv(f"gs://{bucket}/annual_co2_emissions.csv", header=True, inferSchema=True)
    annual_ghe = spark_csv.read.csv(f"gs://{bucket}/annual_ghe.csv", header=True, inferSchema=True)
    annual_deforest = spark_csv.read.csv(f"gs://{bucket}/annual_deforest.csv", header=True, inferSchema=True)
    tree_cover_loss_wildfires = spark_csv.read.csv(f"gs://{bucket}/tree_cover_loss_wildfires.csv", header=True, inferSchema=True)
    energy_consumption = spark_csv.read.csv(f"gs://{bucket}/energy_consumption.csv", header=True, inferSchema=True)
    average_precipitation = spark_csv.read.csv(f"gs://{bucket}/average_precipitation.csv", header=True, inferSchema=True)
    gdp_ppp_per_capita = spark_csv.read.csv(f"gs://{bucket}/gdp_ppp_per_capita.csv", header=True, inferSchema=True)
    gdp_nominal_per_capita = spark_csv.read.csv(f"gs://{bucket}/gdp_nominal_per_capita.csv", header=True, inferSchema=True)
    inflation_rate = spark_csv.read.csv(f"gs://{bucket}/inflation_rate.csv", header=True, inferSchema=True)
    crime_rate = spark_csv.read.csv(f"gs://{bucket}/crime_rate.csv", header=True, inferSchema=True)
    intl_tourist_spending = spark_csv.read.csv(f"gs://{bucket}/intl_tourist_spending.csv", header=True, inferSchema=True)
    natural_disaster_death = spark_csv.read.csv(f"gs://{bucket}/natural_disaster_death.csv", header=True, inferSchema=True)

    # Join all the Our World in Data Tables (OWID) (Drop the duplicate columns)
    country_stats_owid = annual_co2 \
            .join(annual_ghe, (annual_co2.Entity == annual_ghe.Entity) & (annual_co2.Year == annual_ghe.Year), 'left')\
            .drop(annual_ghe.Entity, annual_ghe.Year, annual_ghe.Code) \
            .join(annual_deforest, (annual_co2.Entity == annual_deforest.Entity) & (annual_co2.Year == annual_deforest.Year), 'left')\
            .drop(annual_deforest.Entity, annual_deforest.Year, annual_deforest.Code) \
            .join(tree_cover_loss_wildfires, (annual_co2.Entity == tree_cover_loss_wildfires.Entity) & (annual_co2.Year == tree_cover_loss_wildfires.Year), 'left')\
            .drop(tree_cover_loss_wildfires.Entity, tree_cover_loss_wildfires.Year, tree_cover_loss_wildfires.Code) \
            .join(energy_consumption, (annual_co2.Entity == energy_consumption.Entity) & (annual_co2.Year == energy_consumption.Year), 'left')\
            .drop(energy_consumption.Entity, energy_consumption.Year, energy_consumption.Code) \
            .join(average_precipitation, (annual_co2.Entity == average_precipitation.Entity) & (annual_co2.Year == average_precipitation.Year), 'left')\
            .drop(average_precipitation.Entity, average_precipitation.Year, average_precipitation.Code) \
            .join(gdp_ppp_per_capita, (annual_co2.Entity == gdp_ppp_per_capita.Entity) & (annual_co2.Year == gdp_ppp_per_capita.Year), 'left')\
            .drop(gdp_ppp_per_capita.Entity, gdp_ppp_per_capita.Year, gdp_ppp_per_capita.Code) \
            .join(gdp_nominal_per_capita, (annual_co2.Entity == gdp_nominal_per_capita.Entity) & (annual_co2.Year == gdp_nominal_per_capita.Year), 'left')\
            .drop(gdp_nominal_per_capita.Entity, gdp_nominal_per_capita.Year, gdp_nominal_per_capita.Code) \
            .join(inflation_rate, (annual_co2.Entity == inflation_rate.Entity) & (annual_co2.Year == inflation_rate.Year), 'left')\
            .drop(inflation_rate.Entity, inflation_rate.Year, inflation_rate.Code) \
            .join(crime_rate, (annual_co2.Entity == crime_rate.Entity) & (annual_co2.Year == crime_rate.Year), 'left')\
            .drop(crime_rate.Entity, crime_rate.Year, crime_rate.Code) \
            .join(intl_tourist_spending, (annual_co2.Entity == intl_tourist_spending.Entity) & (annual_co2.Year == intl_tourist_spending.Year), 'left')\
            .drop(intl_tourist_spending.Entity, intl_tourist_spending.Year, intl_tourist_spending.Code) \
            .join(natural_disaster_death, (annual_co2.Entity == natural_disaster_death.Entity) & (annual_co2.Year == natural_disaster_death.Year), 'left')\
            .drop(natural_disaster_death.Entity, natural_disaster_death.Year, natural_disaster_death.Code)

    # Rename columns
    new_column_names = {
        'Entity': 'name', 
        'Code': 'country_code', 
        'Year': 'year', 
        'Annual COâ emissions': 'co2 emissions', 
        'Annual greenhouse gas emissions in COâ equivalents': 'ghe_co_equivalent', 
        'Deforestation as share of forest area': 'deforestation_by_forest_area', 
        'wildfire': 'tree_loss_from_wildfires',
        'ny_gdp_pcap_pp_kd': 'gdp_ppp_per_capita', 
        'ny_gdp_pcap_kd': 'nominal_gdp_ppp_per_capita',
        'fp_cpi_totl_zg': 'inflation_rate_cpi_based', 
        'value__category_total__sex_total__age_total__unit_of_measurement_rate_per_100_000_population': 'crime_rate', 
        'outbound_exp_us_cpi_adjust': 'intl_tourist_spending', 
        'death_count__age_group_allages__sex_both_sexes__cause_natural_disasters': 'natural_disaster_deaths',
    }
    
    for old_col, new_col in new_column_names.items():
        country_stats_owid = country_stats_owid.withColumnRenamed(old_col, new_col)

    world_heritage_sites = spark_csv.read.csv(f"gs://{bucket}/unesco_whs_by_country.csv", header=True, inferSchema=True)
    

    # Show the result
    print(country_stats_owid.head(1))

    """
        3. average_monthly_surface_temp.csv
        7. disease_death.csv
        12. infrastructure.csv
        13. international_tourist_trips.csv
        14. internet_penetration_rate.csv
    """

    # Load all files
    # files = GCSFileSystem().ls(f"gs://{bucket}/")
    # csv_files_paths = [f"gs://{file}" for file in files if file.endswith(".csv")]

    # if not csv_files_paths:
    #     raise ValueError("No CSV files found in the bucket.")
    
    # print("Succesfully read files:")
    # for i, name in enumerate(csv_files_paths):
    #     print(f"{i}. {name}")

    # # Passing a list to read.csv reads them in parallel
    # df = spark_csv.read.csv(csv_files_paths, header=True, inferSchema=True)

    # for column_name, column_type in df.dtypes:
    #     if column_type == "string":
    #         df = df.withColumn(column_name, trim(col(column_name)))

if __name__ == '__main__':
    clean_data_tables()