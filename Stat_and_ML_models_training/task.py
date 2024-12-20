# # Running this code will query a table in BigQuery and download
# # the results to a Pandas DataFrame named `results`.
# # Learn more here: https://cloud.google.com/bigquery/docs/visualize-jupyter

# %%bigquery results --project phonic-sunbeam-443308-r6
# SELECT * FROM `phonic-sunbeam-443308-r6.travel_dataset.ml_features` #this table name was set based on the table you chose to query

from google.cloud import bigquery
import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import os
import joblib
from google.cloud import storage
import logging
# from google.colab import auth

# # Authenticate
# auth.authenticate_user()

# Create client with the correct project ID
project_id = "phonic-sunbeam-443308-r6" 
bq_client = bigquery.Client(project=project_id)

# Define the query
query = """
    SELECT *
    FROM `phonic-sunbeam-443308-r6.travel_dataset.ml_features`
"""

# Execute query and convert to dataframe
results = bq_client.query(query).to_dataframe()

results = results.drop(columns = ["deforestation_per_area", "tree_loss_from_wildfires_ha"])
results = results.dropna(subset=['in_tour_arrivals_ovn_vis_tourists', 'overall_infrastructure_score'])

impute_array = []

# Print how many data entry each column have
for column in results.columns:
    if results[column].count() < results['name'].count():
      impute_array.append(column)

import pandas as pd
from fancyimpute import IterativeImputer

# Use IterativeImputer (MICE) to fill in the missing values
imputer = IterativeImputer()
imputed_data = imputer.fit_transform(results[impute_array])

# Replace the missing data column with the imputed values
results[impute_array] = imputed_data

# Loop through the columns and convert object columns to numerical values 
for column in results.select_dtypes(include=['object']).columns: 
  results[column] = results[column].astype('category').cat.codes 

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Load the dataset
data = results

# Define features (X) and target (y)
X = data.drop(columns=['in_tour_arrivals_ovn_vis_tourists']) 
y = data['in_tour_arrivals_ovn_vis_tourists']  

# Split the dta
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Random Forest Regressor model
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

# Calculate Relative MAE
relative_mae = mae / y_test.mean()
print(f"Relative Mean Absolute Error (Relative MAE): {relative_mae}")

from sklearn.metrics import mean_absolute_percentage_error

# Evaluate the model using MAPE
mape = mean_absolute_percentage_error(y_test, y_pred)

print("Mean Absolute Percentage Error (MAPE):", mape)

# Save model artifact to Local filesystem 
artifact_filename = 'model.joblib'
local_path = artifact_filename
joblib.dump(rf, local_path)

# Upload model artifact to Cloud Storage
model_directory = "gs://travel-analysis-bucket/scripts/machine-learning"
storage_path = os.path.join(model_directory, artifact_filename)

blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
blob.upload_from_filename(local_path)

logging.info("model exported to : {}".format (storage_path))