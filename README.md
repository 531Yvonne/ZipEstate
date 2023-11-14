# Tracking 10-Year US Real Estate Market Data by Zip Code

## Introduction
A Big Data Application built with
- Over 7.4 Million data records covering 10-year quarterly US real estate market transaction information at zipcode level (Downloaded from Redfin Data Portal);
- Over 42 Thousand data records covering all US zipcode and corresponding primary city information.

## Data Preparation
### Data Ingestion
Download Data and Ingest to AWS EMR Cluster

HDFS File Structure:
```
./yvesyang
    /zipcode
        /zipcode_city.csv
    /new_2023_data
        /zip_market_after_2023.csv
    /history_data
        /zip_market_before_2023.csv
```
### Data Manipulation
Launch Hive in EMR: beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver

#### Hive Table
Create Hive Table from raw source csv files
- yvesyang_before2023
- yvesyang_after2023
- yvesyang_zipcode

Connect US zipcode data to US Housing Market data
- yvesyang_combined_before2023
- yvesyang_combined_after2023


## Presentation
Web UI
Add generative map in the web ui?


## Video Demo
(to update)
[![Watch the video](https://img.youtube.com/vi/mTe6FmaFXeo/0.jpg)](https://youtu.be/mTe6FmaFXeo)

## Snapshot
![Tech Stack](./app_snapshots/components.png)
![Chat](./app_snapshots/Prediction.png)
![File](./app_snapshots/Prediction.png)
![Video](./app_snapshots/Prediction.png)
![Chart](./app_snapshots/Prediction.png)
![About](./app_snapshots/Prediction.png)