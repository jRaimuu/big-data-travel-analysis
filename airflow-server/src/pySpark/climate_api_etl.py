import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os
import gcsfs
import datetime
import pytz

def fetch_weather_data():
    
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    edmonton_tz = pytz.timezone('America/Edmonton') 
    edmonton_now = utc_now.astimezone(edmonton_tz) 
    dt = edmonton_now.strftime("%Y-%m-%d %H:%M")

    BUCKET_NAME = "climate_analysis_bucket"
    FILE_PATH = f"weather_data/seven_day_forecast {dt}.csv"

    # fetch weather data from the API
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 51.0501,
        "longitude": -114.0853,
        "hourly": "temperature_2m"
    }
    print("===> Making request")
    response = requests.get(url, params=params)
    data = response.json()
    print(data)

    # parse data from JSON and convert it to a pandas DataFrame
    hourly_data = data.get('hourly', {})
    time = hourly_data.get('time', [])
    temp = hourly_data.get('temperature_2m', [])

    rows = [{'time': x, 'temp': y} for x, y in zip(time, temp)]
    print("===> Creating pandas DataFrame")
    df = pd.DataFrame(rows)
    print(df.head())

    print("===> Writing to GCS")
    gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
    
    # writing to GCS
    with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
        df.to_csv(f)

