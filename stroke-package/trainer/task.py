import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import os

# # Define headers with custom User-Agent
# headers = {'User-Agent': 'Our World In Data data fetch/1.0'}

# # Fetch CO2 data
# url_co2 = "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv?v=1&csvType=full&useColumnShortNames=false"
# response_co2 = requests.get(url_co2, headers=headers)

# # Check for a successful response
# if response_co2.status_code == 200:
#     # Convert response content into a pandas DataFrame
#     annual_co2 = pd.read_csv(StringIO(response_co2.text))
#     # print(annual_co2.head())
# else:
#     print(f"Failed to fetch CO2 data. Status code: {response_co2.status_code}")

# # Fetch GHG data
# url_ghe = "https://ourworldindata.org/grapher/total-ghg-emissions.csv?v=1&csvType=full&useColumnShortNames=false"
# response_ghe = requests.get(url_ghe, headers=headers)

# # Check for a successful response
# if response_ghe.status_code == 200:
#     # Convert response content into a pandas DataFrame
#     annual_ghe = pd.read_csv(StringIO(response_ghe.text))
#     # print(annual_ghe.head())
# else:
#     print(f"Failed to fetch GHG data. Status code: {response_ghe.status_code}")

# List of datasets to fetch
datasets = {
    "annual_co2": "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv?v=1&csvType=full&useColumnShortNames=false",
    "annual_ghe": "https://ourworldindata.org/grapher/total-ghg-emissions.csv?v=1&csvType=full&useColumnShortNames=false",
    "annual_deforest": "https://ourworldindata.org/grapher/deforestation-share-forest-area.csv?v=1&csvType=full&useColumnShortNames=true",
    "tree_cover_loss_wildfires": "https://ourworldindata.org/grapher/tree-cover-loss-from-wildfires.csv?v=1&csvType=full&useColumnShortNames=true",
    "energy_consumption": "https://ourworldindata.org/grapher/primary-energy-cons.csv?v=1&csvType=full&useColumnShortNames=true",
    "climate_support": "https://ourworldindata.org/grapher/support-policies-climate.csv?v=1&csvType=full&useColumnShortNames=true",
    "average_precipitation": "https://ourworldindata.org/grapher/average-precipitation-per-year.csv?v=1&csvType=full&useColumnShortNames=true",
    "gdp_ppp_per_capita": "https://ourworldindata.org/grapher/gdp-per-capita-worldbank.csv?v=1&csvType=full&useColumnShortNames=true",
    "gdp_nominal_per_capita": "https://ourworldindata.org/grapher/gdp-per-capita-world-bank-constant-usd.csv?v=1&csvType=full&useColumnShortNames=true",
    "inflation_rate": "https://ourworldindata.org/grapher/inflation-of-consumer-prices.csv?v=1&csvType=full&useColumnShortNames=true",
    "crime_rate": "https://ourworldindata.org/grapher/homicide-rate-unodc.csv?v=1&csvType=full&useColumnShortNames=true",
    "internet_penetration_rate": "https://ourworldindata.org/grapher/share-of-individuals-using-the-internet.csv?v=1&csvType=full&useColumnShortNames=true",
    "intl_tourist_spending": "https://ourworldindata.org/grapher/average-expenditures-of-tourists-abroad.csv?v=1&csvType=full&useColumnShortNames=true",
    "natural_disaster_death": "https://ourworldindata.org/grapher/deaths-from-natural-disasters.csv?v=1&csvType=full&useColumnShortNames=true",
    "population_density": "https://ourworldindata.org/grapher/population-density.csv?v=1&csvType=full&useColumnShortNames=true",
    "international_tourist_trips": "https://ourworldindata.org/grapher/international-tourist-trips.csv?v=1&csvType=full&useColumnShortNames=true",
    "average_monthly_surface_temp": "https://ourworldindata.org/grapher/average-monthly-surface-temperature.csv?v=1&csvType=full&useColumnShortNames=true"
}

# User-Agent headers
headers = {'User-Agent': 'Our World In Data data fetch/1.0'}

# Dictionary to store the DataFrames
dataframes = {}

# Loop through datasets to fetch and process
for name, url in datasets.items():
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            dataframes[name] = pd.read_csv(StringIO(response.text))
            print(f"{name} loaded successfully")
        else:
            print(f"Failed to fetch {name}")
    except Exception as e:
        print(f"Failed to fetch {name}. Error: {e}")


# Function to fetch tables from Wikipedia
def fetch_wiki_table(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')  # Get the first table on the page
            if table:
                return pd.read_html(str(table))[0]  # Convert the HTML table to a DataFrame
            else:
                print(f"No table found at {url}")
                return None
        else:
            print(f"Failed to fetch Wikipedia page. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching Wikipedia table from {url}: {e}")
        return None

# Error
# # Function to fetch files from Google Drive public links
# def fetch_google_drive_file(file_id, destination):
#     try:
#         # Use gdown to download the file
#         os.system(f"gdown --id {file_id}")
#         if os.path.exists(destination):
#             return pd.read_csv(destination) 
#         else:
#             print(f"Failed to download file with ID {file_id}")
#             return None
#     except Exception as e:
#         print(f"Error downloading Google Drive file with ID {file_id}: {e}")
#         return None

# Fixed
def fetch_google_drive_file(file_id, destination):
    try:
        # Print the actual command for debugging
        command = f"gdown --id {file_id} -O {destination}"
        print(f"Executing command: {command}")
        os.system(command)

        if os.path.exists(destination):
            return pd.read_csv(destination) 
        else:
            print(f"Failed to download file with ID {file_id}")
            return None
    except Exception as e:
        print(f"Error downloading Google Drive file with ID {file_id}: {e}")
        return None

# Example Usage: Multiple Wiki URLs and Google Drive files
wiki_urls = {
    "number_of_UNESCO_WHS": "https://en.wikipedia.org/wiki/World_Heritage_Sites_by_country",
    "population": "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population",
    "political_stability": "https://en.wikipedia.org/wiki/List_of_countries_by_Fragile_States_Index",
    "infrastructure": "https://worldpopulationreview.com/country-rankings/infrastructure-by-country"
}

google_drive_files = {
    "disease_death": "1FKaZWXoVQ8pIIPFSrRWR8vofTvU20LP6",
}

# Load data from Wikipedia
for key, url in wiki_urls.items():
    print(f"Fetching table from Wikipedia page for {key}: {url}")
    df = fetch_wiki_table(url)
    if df is not None:
        dataframes[key] = df  # Use the dictionary key as the dataframe name
        print(f"Loaded table for {key} from Wikipedia page")

# Load data from Google Drive
for name, file_id in google_drive_files.items():
    print(f"Fetching file from Google Drive: {name}")
    destination = f"{name}.csv"  # Destination path
    df = fetch_google_drive_file(file_id, destination)
    if df is not None:
        dataframes[name] = df
        print(f"Loaded dataset: {name}")

# Verify loaded datasets
for name, df in dataframes.items():
    print(f"\n{name} DataFrame preview:")
    print(df.head())

# Convert 2015 dollars to 2017 dollars
dataframes['gdp_nominal_per_capita']['ny_gdp_pcap_kd'] = dataframes['gdp_nominal_per_capita']['ny_gdp_pcap_kd'] * 64623.125 / 62789.130

# inflation_rate = inflation_rate.rename(columns={'fp_cpi_totl_zg': 'inflation_rate'})
# crime_rate = crime_rate.rename(columns={'value__category_total__sex_total__age_total__unit_of_measurement_rate_per_100_000_population': 'crime_rate'})
# internet_penetration_rate = internet_penetration_rate.rename(columns={'it_net_user_zs': 'internet_penetration_rate'})
# intl_tourist_spending = intl_tourist_spending.rename(columns={'outbound_exp_us_cpi_adjust': 'intl_tourist_spending'})
# natural_disaster_death = natural_disaster_death.rename(columns={'death_count__age_group_allages__sex_both_sexes__cause_natural_disasters': 'natural_disaster_death'})
# number_of_UNESCO_WHS = number_of_UNESCO_WHS[['Country', 'Total sites']]
# number_of_UNESCO_WHS.rename(columns={'Total sites': 'number_of_UNESCO_WHS', 'Country': 'Entity'}, inplace=True)
# population = population[['Location', 'Population']]
# population.rename(columns={'Location': 'Entity'}, inplace=True)
# political_stability = political_stability[['Country', '2024 score']]
# political_stability.rename(columns={'2024 score': 'political_stability', 'Country': 'Entity'}, inplace=True)
# infrastructure = infrastructure[['Country', 'Overall Infrastructure Score', 'Basic Infrastructure Score', 'Technological Infrastructure Score', 'Scientific Infrastructure Score', 'Health and Environment Score', 'Education Score']]
# infrastructure.rename(columns={'Country': 'Entity'}, inplace=True)
# disease_death = disease_death[['location_name', 'year', 'val']]
# disease_death.rename(columns={'location_name': 'Entity', 'year': 'Year', 'val': 'disease_death'}, inplace=True)
# disease_death['Entity'] = disease_death['Entity'].replace('United States of America', 'United States')
# average_monthly_surface_temp.rename(columns={'year': 'Year'}, inplace=True)
# average_yearly_temp = average_monthly_surface_temp.groupby(['Entity', 'Code', 'Year'])['temperature_2m'].mean().reset_index()

# Renaming
dataframes['inflation_rate'] = dataframes['inflation_rate'].rename(columns={'fp_cpi_totl_zg': 'inflation_rate'})
dataframes['crime_rate'] = dataframes['crime_rate'].rename(columns={'value__category_total__sex_total__age_total__unit_of_measurement_rate_per_100_000_population': 'crime_rate'})
dataframes['internet_penetration_rate'] = dataframes['internet_penetration_rate'].rename(columns={'it_net_user_zs': 'internet_penetration_rate'})
dataframes['intl_tourist_spending'] = dataframes['intl_tourist_spending'].rename(columns={'outbound_exp_us_cpi_adjust': 'intl_tourist_spending'})
dataframes['natural_disaster_death'] = dataframes['natural_disaster_death'].rename(columns={'death_count__age_group_allages__sex_both_sexes__cause_natural_disasters': 'natural_disaster_death'})
dataframes['number_of_UNESCO_WHS'] = dataframes['number_of_UNESCO_WHS'][['Country', 'Total sites']]
dataframes['number_of_UNESCO_WHS'].rename(columns={'Total sites': 'number_of_UNESCO_WHS', 'Country': 'Entity'}, inplace=True)
dataframes['population'] = dataframes['population'][['Location', 'Population']]
dataframes['population'].rename(columns={'Location': 'Entity'}, inplace=True)
dataframes['political_stability'] = dataframes['political_stability'][['Country', '2024 score']]
dataframes['political_stability'].rename(columns={'2024 score': 'political_stability', 'Country': 'Entity'}, inplace=True)
dataframes['infrastructure'] = dataframes['infrastructure'][['Country', 'Overall Infrastructure Score', 'Basic Infrastructure Score', 'Technological Infrastructure Score', 'Scientific Infrastructure Score', 'Health and Environment Score', 'Education Score']]
dataframes['infrastructure'].rename(columns={'Country': 'Entity'}, inplace=True)
dataframes['disease_death'] = dataframes['disease_death'][['location_name', 'year', 'val']]
dataframes['disease_death'].rename(columns={'location_name': 'Entity', 'year': 'Year', 'val': 'disease_death'}, inplace=True)
# dataframes['disease_death']['Entity'] = dataframes['disease_death']['Entity'].replace('United States of America', 'United States')
dataframes['disease_death'].loc[:, 'Entity'] = dataframes['disease_death']['Entity'].replace('United States of America', 'United States')
dataframes['average_monthly_surface_temp'].rename(columns={'year': 'Year'}, inplace=True)

# Grouping average yearly temperature
dataframes['average_yearly_temp'] = dataframes['average_monthly_surface_temp'].groupby(['Entity', 'Code', 'Year'])['temperature_2m'].mean().reset_index()


# Normal merge (from same source)
merge_df = pd.merge(dataframes['international_tourist_trips'], dataframes['average_yearly_temp'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['annual_co2'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['annual_ghe'], on=["Entity", "Code", "Year"], how="inner")
# merge_df = pd.merge(merge_df, dataframes['annual_deforest'], on=["Entity", "Code", "Year"], how="inner")
# merge_df = pd.merge(merge_df, dataframes['tree_cover_loss_wildfires'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['energy_consumption'], on=["Entity", "Code", "Year"], how="inner")
# merge_df = pd.merge(merge_df, dataframes['climate_support'], on=["Entity", "Code", "Year"], how="inner")  # Only 1 year, can't merge
merge_df = pd.merge(merge_df, dataframes['average_precipitation'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['gdp_ppp_per_capita'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['gdp_nominal_per_capita'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['inflation_rate'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['crime_rate'], on=["Entity", "Code", "Year"], how="inner")
# merge_df = pd.merge(merge_df, dataframes['internet_penetration_rate'], on=["Entity", "Code", "Year"], how="inner")
merge_df = pd.merge(merge_df, dataframes['intl_tourist_spending'], on=["Entity", "Code", "Year"], how="inner")
# merge_df = pd.merge(merge_df, dataframes['natural_disaster_death'], on=["Entity", "Code", "Year"], how="inner")  # negligible impact
# merge_df = pd.merge(merge_df, dataframes['population_density'], on=["Entity", "Code", "Year"], how="inner")  # negligible impact

# Merge from Wiki
merge_df = pd.merge(merge_df, dataframes['number_of_UNESCO_WHS'], on=["Entity"], how="left")
merge_df = pd.merge(merge_df, dataframes['population'], on=["Entity"], how="inner")
merge_df = pd.merge(merge_df, dataframes['political_stability'], on=["Entity"], how="inner")
merge_df = pd.merge(merge_df, dataframes['infrastructure'], on=["Entity"], how="inner")

# Merge from Google Drive upload
merge_df = pd.merge(merge_df, dataframes['disease_death'], on=["Entity", "Year"], how="inner")

# Additional processing
merge_df['purchasing_power_index'] = merge_df['ny_gdp_pcap_pp_kd'] / merge_df['ny_gdp_pcap_kd']


# Model
merge_df.fillna(0, inplace=True)
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Load your dataset
data = merge_df

# Drop "Code" column
data = data.drop(columns=['Code'])

# Convert "Entity" to numerical codes
entity_dict = {entity: idx for idx, entity in enumerate(data['Entity'].unique())}
data['Entity'] = data['Entity'].map(entity_dict)

# Define features (X) and target (y)
X = data.drop(columns=['in_tour_arrivals_ovn_vis_tourists'])  # Features
y = data['in_tour_arrivals_ovn_vis_tourists']  # Target

# Train-test split (80/20)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Random Forest Regressor
rf = RandomForestRegressor(random_state=42, n_estimators=100)
rf.fit(X_train, y_train)

# Make predictions
y_pred = rf.predict(X_test)

# Evaluate the model
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("Mean Absolute Error (MAE):", mae)
print("Mean Squared Error (MSE):", mse)
print("R² Score:", r2)




