import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import joblib
from google.cloud import storage
import logging

import subprocess

command = [
    "gsutil", "-m", "cp", "-r",
    "gs://travel-analysis-bucket/aggregated/average_monthly_surface_temp",
    "gs://travel-analysis-bucket/aggregated/climate_year",
    "gs://travel-analysis-bucket/aggregated/cultural",
    "gs://travel-analysis-bucket/aggregated/disease_death",
    "gs://travel-analysis-bucket/aggregated/health_econ_year",
    "gs://travel-analysis-bucket/aggregated/tourism_year",
    "."
]

subprocess.run(command)


# climate = pd.read_csv("/climate_year/part-00000-c32df0cd-3b2a-4121-940b-72b286b724dc-c000.csv")
# climate = climate.drop(columns=['deforestation_per_area'])

# cultural = pd.read_csv("/cultural/part-00000-45560606-287c-4c18-afc9-432b6a39563c-c000.csv")

# disease_death = pd.read_csv("/disease_death/part-00000-967e0ddc-6abc-43a1-a688-3021496db094-c000.csv")

# health_econ_year = pd.read_csv("/health_econ_year/part-00000-9eee8e25-14e3-4ccc-828a-bdb09f57136a-c000.csv")

# tourism_year = pd.read_csv("/tourism_year/part-00000-79b7fe51-3adb-4f25-b750-0992a331f5f7-c000.csv")

import pandas as pd
import os

def get_first_csv_file_path(folder_path):
    # List all files in the directory
    files = os.listdir(folder_path)
    # Filter for CSV files
    csv_files = [file for file in files if file.endswith('.csv')]
    # Return the first CSV file path, if any
    if csv_files:
        return os.path.join(folder_path, csv_files[0])
    return None

# Define the paths to the folders
folders = {
    'climate_year': 'climate_year',
    'cultural': 'cultural',
    'disease_death': 'disease_death',
    'health_econ_year': 'health_econ_year',
    'tourism_year': 'tourism_year'
}

# Load the CSV files
climate_path = get_first_csv_file_path(folders['climate_year'])
climate = pd.read_csv(climate_path)
climate = climate.drop(columns=['deforestation_per_area'])

cultural_path = get_first_csv_file_path(folders['cultural'])
cultural = pd.read_csv(cultural_path)

disease_death_path = get_first_csv_file_path(folders['disease_death'])
disease_death = pd.read_csv(disease_death_path)

health_econ_year_path = get_first_csv_file_path(folders['health_econ_year'])
health_econ_year = pd.read_csv(health_econ_year_path)

tourism_year_path = get_first_csv_file_path(folders['tourism_year'])
tourism_year = pd.read_csv(tourism_year_path)


merge_df = pd.merge(climate, cultural, on=['name'])
merge_df = pd.merge(merge_df, disease_death, on=['name', 'year'])
merge_df = pd.merge(merge_df, health_econ_year, on=['name', 'country_code', 'year'])
merge_df = pd.merge(merge_df, tourism_year, on=['name', 'country_code', 'year'])

merge_df = merge_df.dropna(subset=['in_tour_arrivals_ovn_vis_tourists', 'overall_infrastructure_score'])
merge_df = merge_df.drop(columns=['natural_disaster_deaths', 'tree_loss_from_wildfires_ha'])
merge_df = merge_df.fillna(0)


import pandas as pd
from fancyimpute import IterativeImputer

# Use IterativeImputer (MICE) to fill in the missing values
imputer = IterativeImputer()
imputed_data = imputer.fit_transform(merge_df[['precipitation', 'avg_surface_temp_C', 'heritage_site_count', 
                                               'crime_rate_per_100000', 'inflation_rate_cpi_based', 'internet_user_score']])

# Replace the missing data column with the imputed values
merge_df[['precipitation', 'avg_surface_temp(C)', 'heritage_site_count', 
          'crime_rate_per_100000', 'inflation_rate_cpi_based', 'internet_user_score']] = imputed_data


# Loop through the columns and convert object columns to numerical codes 
for column in merge_df.select_dtypes(include=['object']).columns: 
  merge_df[column] = merge_df[column].astype('category').cat.codes 


# Define the columns of interest
columns_of_interest = merge_df.columns.drop(['name'])

# Subset the DataFrame to include only the numeric columns of interest
numeric_df = merge_df[columns_of_interest]

# Calculate the correlations
target_column = 'in_tour_arrivals_ovn_vis_tourists'
correlations = numeric_df.corr()[target_column]

# Print correlations
print(correlations.sort_values(ascending=False))



import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Load your dataset
data = merge_df

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

from sklearn.metrics import mean_absolute_percentage_error

# Evaluate the model using MAPE
mape = mean_absolute_percentage_error(y_test, y_pred)

print("Mean Absolute Percentage Error (MAPE):", mape)


# Save model artifact to Local filesystem (doesn't persist)
artifact_filename = 'model.joblib'
local_path = artifact_filename
joblib.dump(rf, local_path)

# Upload model artifact to Cloud Storage
model_directory = "gs://travel-analysis-bucket/scripts/machine-learning"
storage_path = os.path.join(model_directory, artifact_filename)

blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
blob.upload_from_filename(local_path)

logging.info("model exported to : {}".format (storage_path))