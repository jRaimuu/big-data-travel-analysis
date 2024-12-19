import pandas as pd
import subprocess
import gcsfs
import requests
from bs4 import BeautifulSoup


def pull_data():
    BUCKET_NAME = "travel-analysis-bucket/source_data"

    fetch_disease_death(BUCKET_NAME)
    fetch_infrastucture_data(BUCKET_NAME)
    fetch_our_world_data(BUCKET_NAME)


def fetch_infrastucture_data(BUCKET_NAME):
    FILE_PATH = f"infrastructure.csv"

    print("===> Making request to fetch UNESCO WHS data")
    response = requests.get("https://worldpopulationreview.com/country-rankings/infrastructure-by-country")

    if response.status_code == 200:
        print("===> Webpage successfully fetched")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table') 

        print("===> Parsing the table")
        df = pd.read_html(str(table))[0]

        print("===> Data preview")
        print(df.head())
        print("===> Writing to GCS")
        gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
        
        with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
            df.to_csv(f, index=False)

        print(f"Data successfully written to {gcs_path}")
    else:
        print(f"Failed to fetch webpage. HTTP Status Code: {response.status_code}")


def fetch_disease_death(BUCKET_NAME):
    FILE_PATH = f"disease_death.csv"
    drive_file_id = "1FKaZWXoVQ8pIIPFSrRWR8vofTvU20LP6"
    local_file_path = "/tmp/IHME-GBD_2021_DATA.csv"
    
    print("===> Downloading dataset from Google Drive")
    subprocess.run(["gdown", "--id", drive_file_id, "--output", local_file_path], check=True)

    print("===> Reading the downloaded dataset")
    df = pd.read_csv(local_file_path)

    print("===> Data preview")
    print(df.head())

    print("===> Writing to GCS")
    gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
    
    with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
        df.to_csv(f, index=False)

    print(f"Data successfully written to {gcs_path}")


def fetch_our_world_data(BUCKET_NAME):
    print("===> Fetching Our World In Data datasets")

    datasets = {
        "annual_co2": "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv?v=1&csvType=full&useColumnShortNames=false",
        "annual_deforest": "https://ourworldindata.org/grapher/deforestation-share-forest-area.csv?v=1&csvType=full&useColumnShortNames=true",
        "annual_ghe": "https://ourworldindata.org/grapher/total-ghg-emissions.csv?v=1&csvType=full&useColumnShortNames=false",
        "average_monthly_surface_temp": "https://ourworldindata.org/grapher/average-monthly-surface-temperature.csv?v=1&csvType=full&useColumnShortNames=true",
        "average_precipitation": "https://ourworldindata.org/grapher/average-precipitation-per-year.csv?v=1&csvType=full&useColumnShortNames=true",
        "crime_rate": "https://ourworldindata.org/grapher/homicide-rate-unodc.csv?v=1&csvType=full&useColumnShortNames=true",
        "energy_consumption": "https://ourworldindata.org/grapher/primary-energy-cons.csv?v=1&csvType=full&useColumnShortNames=true",
        "gdp_ppp_per_capita": "https://ourworldindata.org/grapher/gdp-per-capita-worldbank.csv?v=1&csvType=full&useColumnShortNames=true",
        "gdp_nominal_per_capita": "https://ourworldindata.org/grapher/gdp-per-capita-world-bank-constant-usd.csv?v=1&csvType=full&useColumnShortNames=true",
        "internet_penetration_rate": "https://ourworldindata.org/grapher/share-of-individuals-using-the-internet.csv?v=1&csvType=full&useColumnShortNames=true",
        "inflation_rate": "https://ourworldindata.org/grapher/inflation-of-consumer-prices.csv?v=1&csvType=full&useColumnShortNames=true",
        "international_tourist_trips": "https://ourworldindata.org/grapher/international-tourist-trips.csv?v=1&csvType=full&useColumnShortNames=true",
        "intl_tourist_spending": "https://ourworldindata.org/grapher/average-expenditures-of-tourists-abroad.csv?v=1&csvType=full&useColumnShortNames=true",
        "natural_disaster_death": "https://ourworldindata.org/grapher/deaths-from-natural-disasters.csv?v=1&csvType=full&useColumnShortNames=true",
        "tree_cover_loss_wildfires": "https://ourworldindata.org/grapher/tree-cover-loss-from-wildfires.csv?v=1&csvType=full&useColumnShortNames=true"
    }

    # Write all files to gscfs
    for name, url in datasets.items():
        print(f"===> Fetching {name} dataset")
        df = pd.read_csv(url, storage_options={'User-Agent': 'Our World In Data data fetch/1.0'})
        
        print("===> Writing to GCS")
        gcs_path = f"gs://{BUCKET_NAME}/{name}.csv"
        
        with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
            df.to_csv(f, index=False)

        print(f"{name} data successfully written to {gcs_path}")
    

if __name__ == "__main__":
    pull_data()