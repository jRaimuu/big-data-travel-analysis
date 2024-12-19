import requests
import pandas as pd
import io


def fetch_disease_death():
    """ Importing the disease_death dataset from Google Drive and writing it to GCS """
    BUCKET_NAME = "travel-analysis-bucket/source_data"
    FILE_PATH = f"disease_death.csv"
    drive_file_id = "1FKaZWXoVQ8pIIPFSrRWR8vofTvU20LP6"
    download_url = f"https://drive.google.com/uc?export=download&id={drive_file_id}"
    
    print("===> Downloading dataset from Google Drive")
    response = requests.get(download_url)
    
    if response.status_code == 200:
        print("===> Dataset downloaded successfully")
        
        print("===> Processing the dataset")
        data = response.content.decode('utf-8')
        df = pd.read_csv(io.StringIO(data))
        
        print("===> Data preview")
        print(df.head())

        print("===> Writing to GCS")
        gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
        
        df.to_csv(gcs_path, index=False)
        print(f"Data successfully written to {gcs_path}")

    else:
        raise Exception(f"Failed to download file from Google Drive. Status Code: {response.status_code}")
    

if __name__ == '__main__':
    fetch_disease_death()