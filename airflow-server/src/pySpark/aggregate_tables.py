from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, sum, when
from gcsfs import GCSFileSystem

'''
To run this file locally, enter this in the command line:
spark-submit \
    --jars gcs-connector-hadoop2-2.2.5-shaded.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    clean_tables.py
'''

def aggregate_data_tables():
    """Aggregates data from multiple CSV files in a Google Cloud Storage bucket into structured tables for analysis"""
    bucket = "travel-analysis-bucket/cleaned"

    spark = SparkSession.builder\
                        .appName("Data Warehouse Aggreagetion")\
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

    disease_death = spark.read.csv(f"gs://{bucket}/disease_death.csv", header=True, inferSchema=True)
    disease_death_renamed = disease_death.withColumnRenamed('location_name', 'name')
    disease_death_replaced = disease_death_renamed.withColumn(
        "name",
        when(col("name") == "United States of America", "United States").otherwise(col("name")))

    # Aggregated disease_death
    disease_death_year = disease_death_replaced.groupBy(['name', 'year']).agg(sum('val').alias('disease_death_val'))

    internet_penetration = spark.read.csv(f"gs://{bucket}/internet_penetration_rate.csv", header=True, inferSchema=True)
    internet_penetration = internet_penetration\
        .withColumnRenamed('it_net_user_zs', 'internet_user_score')\
    
    # Monthly surface temp data
    average_monthly_surface_temp_full = spark.read.csv(f"gs://{bucket}/average_monthly_surface_temp.csv", header=True, inferSchema=True)
    average_monthly_surface_temp = average_monthly_surface_temp_full.select('Entity', 'Code', 'year', 'temperature_2m')
    average_monthly_surface_temp_renamed = average_monthly_surface_temp\
        .withColumnRenamed('Entity', 'name')\
        .withColumnRenamed('Code', 'country_code')\
        .withColumnRenamed('temperature_2m', 'surface_temp')

    # Aggregated surface temp data by average
    yearly_surface_temp_full = average_monthly_surface_temp_full.filter(month(average_monthly_surface_temp_full.Day) == 1)
    yearly_surface_temp = yearly_surface_temp_full.select(
        col("Entity"),
        col("Code"),
        col("year"),
        col("`temperature_2m.1`")
    )

    # Data that is not yearly
    world_heritage_sites = spark.read.csv(f"gs://{bucket}/unesco_whs_by_country.csv", header=True, inferSchema=True)
    world_heritage_sites_renamed = world_heritage_sites\
        .withColumnRenamed('Total sites', 'heritage_site_count')\
        .withColumnRenamed('Country', 'UNESCO_Country')
    world_heritage_sites_selected = world_heritage_sites_renamed.select('heritage_site_count', 'UNESCO_Country')

    infrastructure_raw = spark.read.csv(f"gs://{bucket}/infrastructure.csv", header=True, inferSchema=True)
    infrastructure_selected = infrastructure_raw.select('Country', 'Overall Infrastructure Score', 'Basic Infrastructure Score', \
        'Technological Infrastructure Score', 'Scientific Infrastructure Score', 'Health and Environment Score', 'Education Score')

    # Make all the columns in infrastructure snake case
    for col_name in infrastructure_selected.columns:
        infrastructure_selected = infrastructure_selected.withColumnRenamed(col_name, col_name.lower().replace(' ', '_'))

    ## Aggregate the data    
    cultural = infrastructure_selected\
        .join(
            world_heritage_sites_selected,
            world_heritage_sites_selected.UNESCO_Country == infrastructure_selected.country,
            'outer')\
        .select(
            when(
                col("UNESCO_Country").isNotNull(), 
                col("UNESCO_Country")).otherwise(col("country"))\
                    .alias("name"),
            "*"
        )\
        .drop(
            infrastructure_selected.country, world_heritage_sites_selected.UNESCO_Country
        )

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
                yearly_surface_temp, 
                (annual_co2.Entity == yearly_surface_temp.Entity) \
                    & (annual_co2.Year == yearly_surface_temp.year), 
                'left')\
            .drop(
                yearly_surface_temp.Entity, 
                yearly_surface_temp.year, 
                yearly_surface_temp.Code)

    health_econ_year = gdp_ppp_per_capita\
            .join(
                crime_rate,
                (gdp_ppp_per_capita.Entity == crime_rate.Entity) & (gdp_ppp_per_capita.Year == crime_rate.Year), 
                'left')\
            .drop(crime_rate.Entity, crime_rate.Year, crime_rate.Code) \
            .join(
                gdp_nominal_per_capita, 
                (gdp_ppp_per_capita.Entity == gdp_nominal_per_capita.Entity) & (gdp_ppp_per_capita.Year == gdp_nominal_per_capita.Year),
                'left')\
            .drop(gdp_nominal_per_capita.Entity, gdp_nominal_per_capita.Year, gdp_nominal_per_capita.Code) \
            .join(
                inflation_rate, 
                (gdp_ppp_per_capita.Entity == inflation_rate.Entity) & (gdp_ppp_per_capita.Year == inflation_rate.Year),
                'left')\
            .drop(inflation_rate.Entity, inflation_rate.Year, inflation_rate.Code) \
            .join(
                natural_disaster_death, 
                (gdp_ppp_per_capita.Entity == natural_disaster_death.Entity) & (gdp_ppp_per_capita.Year == natural_disaster_death.Year),
                'left')\
            .drop(natural_disaster_death.Entity, natural_disaster_death.Year, natural_disaster_death.Code)\
            .join(
                internet_penetration, 
                (gdp_ppp_per_capita.Entity == internet_penetration.Entity) & (gdp_ppp_per_capita.Year == internet_penetration.Year),
                'left')\
            .drop(internet_penetration.Entity, internet_penetration.Year, internet_penetration.Code)\
            .join(
                disease_death_year, 
                (gdp_ppp_per_capita.Entity == disease_death_year.name) & (gdp_ppp_per_capita.Year == disease_death_year.year),
                'left')\
            .drop(disease_death_year.name, disease_death_year.year)

    tourism_year = intl_tourist_spending\
            .join(
                international_tourist_trips, 
                (international_tourist_trips.Entity == intl_tourist_spending.Entity) & \
                    (international_tourist_trips.Year == intl_tourist_spending.Year), 
                'left')\
            .drop(international_tourist_trips.Entity, international_tourist_trips.Year, international_tourist_trips.Code)\
            .withColumnRenamed('Entity', 'name')\
            .withColumnRenamed('Code', 'country_code')\
            .withColumnRenamed('Year', 'year')

    # Rename columns
    climate_names = {
        'Entity': 'name', 
        'Code': 'country_code', 
        'Year': 'year', 
        'Annual COâ emissions': 'co2_emissions(carbon_tonnes)', 
        'Annual greenhouse gas emissions in COâ equivalents': 'gh_emissions(in_co2_tonnes)',
        'primary_energy_consumption__twh': 'energy_consumption(twh)',
        'total_precipitation': 'precipitation',
        'Deforestation as share of forest area': 'deforestation_per_area', 
        'wildfire': 'tree_loss_from_wildfires(ha)',
        'temperature_2m.1': 'avg_surface_temp(C)',
    }
    
    for old_col, new_col in climate_names.items():
        climate_year = climate_year.withColumnRenamed(old_col, new_col)

    health_econ_names = {
        'Entity': 'name',
        'Code': 'country_code',
        'Year': 'year',
        'ny_gdp_pcap_pp_kd': 'gdp_ppp_per_capita', 
        'ny_gdp_pcap_kd': 'nominal_gdp_ppp_per_capita',
        'fp_cpi_totl_zg': 'inflation_rate_cpi_based', 
        'value__category_total__sex_total__age_total__unit_of_measurement_rate_per_100_000_population': 'crime_rate_per_100000', 
        'death_count__age_group_allages__sex_both_sexes__cause_natural_disasters': 'natural_disaster_deaths',
    }

    for old_col, new_col in health_econ_names.items():
        health_econ_year = health_econ_year.withColumnRenamed(old_col, new_col)

    tourism_year = tourism_year.withColumnRenamed('outbound_exp_us_cpi_adjust', 'intl_tourist_spending')

    # The data is grouped into following tables:
    # Climate by year (Includes aggregates)
    # Health and Economy by year (Includes aggregates)
    # Tourism by year
    # Monthly rainfall (Not aggregated)
    # Disease death (Not aggregated)
    # Infrastructure

    print("\nClimate by Year Table:")
    climate_year.show(5)
    climate_year.describe()

    print("\nHealth and Economy by Year Table:")
    health_econ_year.show(5)
    health_econ_year.describe()

    print("\nTourism Stats by Year Table:")
    tourism_year.show(5)
    tourism_year.describe()

    print("\nAverage Monthly Surface Temperate Table:")
    average_monthly_surface_temp_renamed.show(5)
    average_monthly_surface_temp_renamed.describe()

    print("\nDisease Death Rate Table:")
    disease_death_replaced.show(5)
    disease_death_replaced.describe()

    print("\nInfrastructure and Heritage Sites Table:")
    cultural.show(5)
    cultural.describe()

    OUTPUT_BUCKET_NAME = "travel-analysis-bucket"
    OUTPUT_PATH  = f"gs://{OUTPUT_BUCKET_NAME}/aggregated"

    # write the data to the GCS bucket
    climate_year.write.csv(OUTPUT_PATH + "/climate_year", header=True, mode="overwrite")
    health_econ_year.write.csv(OUTPUT_PATH + "/health_econ_year", header=True, mode="overwrite")
    tourism_year.write.csv(OUTPUT_PATH + "/tourism_year", header=True, mode="overwrite")
    average_monthly_surface_temp_renamed.write.csv(OUTPUT_PATH + "/average_monthly_surface_temp", header=True, mode="overwrite")
    disease_death_replaced.write.csv(OUTPUT_PATH + "/disease_death", header=True, mode="overwrite")
    cultural.write.csv(OUTPUT_PATH + "/cultural", header=True, mode="overwrite")


if __name__ == '__main__':
    aggregate_data_tables()