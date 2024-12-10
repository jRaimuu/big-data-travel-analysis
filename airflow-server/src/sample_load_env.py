import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os
import gcsfs
import datetime
import pytz

def load_data():
    
    load_dotenv()
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    edmonton_tz = pytz.timezone('America/Edmonton') 
    edmonton_now = utc_now.astimezone(edmonton_tz) 
    dt = edmonton_now.strftime("%Y-%m-%d %H:%M")

    BUCKET_NAME = os.getenv("BUCKET_NAME")
    FILE_PATH = f"file_name {dt}.csv"

    gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
    print(gcs_path)

    return

load_data()