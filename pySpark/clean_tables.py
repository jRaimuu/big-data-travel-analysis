from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import month

from dotenv import load_dotenv
from os import getenv, path
from dotenv import load_dotenv


def clean_data_tables():
    """Clean all string columns in bucket by iterating over all csv files"""
    dotenv_path = path.abspath(path.join(path.dirname(__file__), '../..', '.env'))
    load_dotenv(dotenv_path)
    bucket = getenv('BUCKET_NAME')

    spark = SparkSession.builder\
                        .appName("Clean DSV Data")\
                        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
                        .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
                        .getOrCreate()


    # Reading all data from bucket

    # Read the data from our wolrd in data as it all has the same schema
    annual_co2 = spark.read.csv(f"gs://{bucket}/annual_co2_emissions.csv", header=True, inferSchema=True)
    annual_ghe = spark.read.csv(f"gs://{bucket}/annual_ghe.csv", header=True, inferSchema=True)
    annual_deforest = spark.read.csv(f"gs://{bucket}/annual_deforest.csv", header=True, inferSchema=True)
    tree_cover_loss_wildfires = spark.read.csv(f"gs://{bucket}/tree_cover_loss_wildfires.csv", header=True, inferSchema=True)
    energy_consumption = spark.read.csv(f"gs://{bucket}/energy_consumption.csv", header=True, inferSchema=True)
    average_precipitation = spark.read.csv(f"gs://{bucket}/average_precipitation.csv", header=True, inferSchema=True)
    gdp_ppp_per_capita = spark.read.csv(f"gs://{bucket}/gdp_ppp_per_capita.csv", header=True, inferSchema=True)
    gdp_nominal_per_capita = spark.read.csv(f"gs://{bucket}/gdp_nominal_per_capita.csv", header=True, inferSchema=True)
    inflation_rate = spark.read.csv(f"gs://{bucket}/inflation_rate.csv", header=True, inferSchema=True)
    crime_rate = spark.read.csv(f"gs://{bucket}/crime_rate.csv", header=True, inferSchema=True)
    intl_tourist_spending = spark.read.csv(f"gs://{bucket}/intl_tourist_spending.csv", header=True, inferSchema=True)
    natural_disaster_death = spark.read.csv(f"gs://{bucket}/natural_disaster_death.csv", header=True, inferSchema=True)
    international_tourist_trips = spark.read.csv(f"gs://{bucket}/international_tourist_trips.csv", header=True, inferSchema=True)

    disease_death = spark.read.csv(f"gs://{bucket}/disease_death.csv")
    disease_death_renamed = disease_death.withColumnRenamed('location_name', 'name')
    disease_death_replaced = disease_death.replace('United States of America', 'United States', ['name'])

    # Aggregated disease_death
    disease_death_year = disease_death.groupBy(['name', 'year']).agg(sum('val').alias('deaths_value'))

    # Aggregated surface temp data by average
    yearly_monthly_surface_temp_full = average_monthly_surface_temp_full.filter(month(average_monthly_surface_temp_full.Day) == 1)
    yearly_monthly_surface_temp = yearly_monthly_surface_temp_full.select('Entity', 'Code', 'Year', 'temperature_2m.1')

    internet_penetration = spark.read.csv(internet_penetration_rate.csv, header=True, inferSchema=True)
    internet_penetration = internet_penetration\
        .withColumnRenamed('it_net_user_zs', 'internet_user_score')\
        .withColumnRenamed('entity', 'name')\
        .withColumnRenamed('Year', 'year')

    # Monthly surface temp data
    average_monthly_surface_temp_full = spark_csv.read.csv(f"gs://{bucket}/average_monthly_surface_temp.csv", header=True, inferSchema=True)
    average_monthly_surface_temp = average_monthly_surface_temp_full.select('Entity', 'Code', 'year', 'temperature_2m')
    average_monthly_surface_temp_renamed = average_monthly_surface_temp.withColumnRenamed('Entity', 'name')

    # Data that is not yearly
    world_heritage_sites = spark_csv.read.csv(f"gs://{bucket}/unesco_whs_by_country.csv", header=True, inferSchema=True)
    world_heritage_sites_renamed = world_heritage_sites\
        .withColumnRenamed('Total sites', 'heritage_sites')\
        .withColumnRenamed('Country', 'name')

    infrastructure = spark.read.csv(f"gs://{bucket}/infrastructure.csv", header=True, inferSchema=True)
    infrastructure_selected = infrastructure.select('Country', 'Overall Infrastructure Score', 'Basic Infrastructure Score', \
        'Technological Infrastructure Score', 'Scientific Infrastructure Score', 'Health and Environment Score', 'Education Score')
    
    # Make all the columns snake case
    for col_name in infrastructure_selected:
        infrastructure = infrastructure_selected.withColumnRenamed(col_name.lower().replace(' ', '_'))

    # The data will be grouped into the following tables:
    # Climate by year (Includes aggregates)
    # Health and Economy by year (Includes aggregates)
    # Tourism by year
    # Monthly rainfall (Not aggregated)
    # Disease death (Not aggregated)
    # Infrastructure

    climate_year = annual_co2 \
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
            .join(
                yearly_monthly_surface_temp_replaced, 
                (annual_co2.Entity == yearly_monthly_surface_temp_replaced.Entity) \
                    & (annual_co2.Year == yearly_monthly_surface_temp_replaced.Year), 
                'left')\
            .drop(
                yearly_monthly_surface_temp_replaced.Entity, 
                yearly_monthly_surface_temp_replaced.Year, 
                yearly_monthly_surface_temp_replaced.Code)


    health_econ_year = crime_rate
            .join(
                gdp_ppp_per_capita, 
                (crime_rate.Entity == gdp_ppp_per_capita.Entity) & (crime_rate.Year == gdp_ppp_per_capita.Year), 
                'right')\
            .drop(gdp_ppp_per_capita.Entity, gdp_ppp_per_capita.Year, gdp_ppp_per_capita.Code) \
            .join(
                gdp_nominal_per_capita, 
                (crime_rate.Entity == gdp_nominal_per_capita.Entity) & (crime_rate.Year == gdp_nominal_per_capita.Year), 
                'left')\
            .drop(gdp_nominal_per_capita.Entity, gdp_nominal_per_capita.Year, gdp_nominal_per_capita.Code) \
            .join(inflation_rate, (crime_rate.Entity == inflation_rate.Entity) & (crime_rate.Year == inflation_rate.Year), 'left')\
            .drop(inflation_rate.Entity, inflation_rate.Year, inflation_rate.Code) \
            .join(
                natural_disaster_death, 
                (crime_rate.Entity == natural_disaster_death.Entity) & \
                    (crime_rate.Year == natural_disaster_death.Year), 
                'left')\
            .drop(natural_disaster_death.Entity, natural_disaster_death.Year, natural_disaster_death.Code)\
            .join(
                internet_penetration, 
                (crime_rate.Entity == internet_penetration.Entity) & (crime_rate.Year == internet_penetration.Year), 
                'left')\
            .drop(internet_penetration.Entity, internet_penetration.Year, internet_penetration.Code)
            .join(disease_death_year, (crime_rate.Entity == disease_death_year.name) & (crime_rate.Year == disease_death_year.name), 'left')\
            .drop(disease_death_year.Entity, disease_death_year.Year, disease_death_year.Code)


    tourism_year = intl_tourist_spending\
            .join(
                international_tourist_trips, 
                (international_tourist_trips.Entity == intl_tourist_spending.Entity) & \
                    (international_tourist_trips.Year == intl_tourist_spending.Year), 
                'left')\
            .drop(inflation_rate.Entity, inflation_rate.Year, inflation_rate.Code)


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

    # Show the result
    print(country_stats_owid.head(1))

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