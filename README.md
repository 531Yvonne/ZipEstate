# Tracking 10-Year US Real Estate Market Data by Zip Code

## Introduction
A Big Data Application built with
- Over 7.4 Million data records covering 10-year quarterly US real estate market transaction information at zipcode level;
- Over 42 Thousand data records covering all US zipcode information

## Process
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

Connect US zipcode data to US Hosing Market data
- yvesyang_combined_before2023
- yvesyang_combined_after2023
