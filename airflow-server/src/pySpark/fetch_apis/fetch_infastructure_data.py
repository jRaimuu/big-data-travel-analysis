import requests
from bs4 import BeautifulSoup
import pandas as pd


def fetch_infrastucture_data():
    BUCKET_NAME = "travel-analysis-bucket"
    FILE_PATH = f"infrastructure.csv"

    print("===> Making request to fetch UNESCO WHS data")
    url = "https://worldpopulationreview.com/country-rankings/infrastructure-by-country"
    response = requests.get(url)

    if response.status_code == 200:
        print("===> Webpage successfully fetched")

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='wpr-table')
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        rows = []
        for row in table.find('tbody').find_all('tr'):
            cells = [cell.text.strip() for cell in row.find_all(['td', 'th'])]
            rows.append(cells)

        infrastructure = pd.DataFrame(rows, columns=headers)

        print("===> Data preview")
        print(infrastructure.info())

        print("===> Writing to GCS")
        gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
        
        infrastructure.to_csv(gcs_path, index=False)

        print(f"Data successfully written to {gcs_path}")
    else:
        print(f"Failed to fetch webpage. HTTP Status Code: {response.status_code}")


if __name__ == '__main__':
    fetch_infrastucture_data()