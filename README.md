# Tourism Analysis

![data_pipeline3](https://github.com/user-attachments/assets/e724749d-0c3f-47ff-ba8e-bd3a2a65e5b7)

## Overview
This project analyzes and predicts the number of international tourists visiting various countries annually. Using datasets from sources like Our World in Data, Wikipedia, and World Population Review, we processed, cleaned, and merged socio-economic, environmental, and infrastructure data to derive insights and a build a predictive model for tourism.

## Key Features
- **Data Processing**: ETL pipeline built with Apache Airflow and Apache Spark to process and store data in BigQuery.
- **Feature Engineering**: Identified key predictors such as UNESCO Heritage Sites, tourist spending, and energy consumption.
- **Predictive Models**: Implemented various machine learning models, including Random Forest and Neural Networks. The best Random Forest model achieved an R² score of 0.993.


## Tools and Technologies
- **Data Engineering**: Apache Airflow, Apache Spark, Google Cloud Storage, BigQuery.
- **Modeling**: scikit-learn, TensorFlow, Vertex AI.
- **Visualization**: Looker Studio dashboard.

![image](https://github.com/user-attachments/assets/cbbdfbbd-ea4a-423e-963f-d3a5bb255efd)

## Findings
- UNESCO World Heritage Sites and infrastructure are strong predictors of tourist arrivals.
- Economic factors like purchasing power index show unexpected correlations.
- Random Forest outperformed other models, achieving high accuracy.

| Variable                                   | Correlation |
|-------------------------------------------|-------------|
| in_tour_arrivals_ovn_vis_tourists         | 1.000000    |
| number_of_UNESCO_WHS                      | 0.749004    |
| intl_tourist_spending                     | 0.602060    |
| primary_energy_consumption_twh            | 0.525072    |
| Annual CO₂ emissions                      | 0.480139    |
| Annual greenhouse gas emissions in CO₂ equivalents | 0.444617    |
| Scientific Infrastructure Score           | 0.434699    |
| Overall Infrastructure Score              | 0.259355    |
| Population                                | 0.233212    |
| Health and Environment Score              | 0.202516    |
| Technological Infrastructure Score        | 0.192875    |
| Education Score                           | 0.130696    |
| ny_gdp_pcap_pp_kd                         | 0.102353    |
| ny_gdp_pcap_kd                            | 0.098619    |
| Basic Infrastructure Score                | 0.049510    |
| political_stability                       | 0.020021    |
| temperature_2m                            | -0.040805   |
| inflation_rate                            | -0.049155   |
| total_precipitation                       | -0.075576   |
| disease_death                             | -0.129860   |
| crime_rate                                | -0.135651   |
| purchasing_power_index                    | -0.237597   |


![image](https://github.com/user-attachments/assets/ba833cde-5514-4081-8715-3913ed05d53e)

## Links
- **Dashboard**: [Looker Studio](https://lookerstudio.google.com/reporting/b1472ef5-dd70-4da2-acc3-fdbde90671b9)

![image](https://github.com/user-attachments/assets/a8fd7843-b676-433c-a192-82bc61d2399f)

## Contributors
- Arush Sanghal
- Liam Sarjeant
- Gustavo Bravo
- NamNguyen Vu

