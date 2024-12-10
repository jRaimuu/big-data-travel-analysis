import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os
import gcsfs
import datetime
import pytz

def fetch_co2_data():
    # GCS bucket details
    BUCKET_NAME = "travel-analysis-bucket"
    DATA_FILE_PATH = f"annual_co2_emissions.csv"

    # Fetch the CO2 emissions data
    data_url = "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv?v=1&csvType=full&useColumnShortNames=false"
    print("===> Fetching CO₂ data")
    annual_co2 = pd.read_csv(data_url, storage_options={'User-Agent': 'Our World In Data data fetch/1.0'})

    print("===> CO₂ data preview:")
    print(annual_co2.head())

    print("===> Writing data to GCS")
    gcs_path_data = f"gs://{BUCKET_NAME}/{DATA_FILE_PATH}"

    with gcsfs.GCSFileSystem().open(gcs_path_data, "w") as f:
        annual_co2.to_csv(f, index=False)

    print(f"Data written to {gcs_path_data}")
