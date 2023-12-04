# Tracking 10-Year US Real Estate Market Data by Zip Code

## Introduction

A Big Data Application built with

- Over 7.4 Million data records covering 10-year US real estate market transaction information at zipcode level
- Over 42 Thousand data records covering all US zipcode and corresponding primary city information
- Implemented using a Lambda Architecture (Batch Layer, Serving Layer, Speed Layer)
- Tools used: [AWS EMR](https://aws.amazon.com/emr/), [Hadoop](https://hadoop.apache.org), [Hive](https://hive.apache.org), [Spark](https://spark.apache.org), [HBase](https://hbase.apache.org), [Kafka](https://kafka.apache.org), [Node.js](https://nodejs.org/en/about), [Python](https://www.python.org)

## Data Preparation

### Data Ingestion

- Download Data source: [Redfin Data Portal](https://www.redfin.com/news/data-center/)

- Transform raw tsv000 data to csv and obtain basic understanding of the source data (size, parameters, scope, etc...) using Python. ([Market Data](./raw_data_processing/market_data_cleaning.ipynb), [Zip Data](./raw_data_processing/zipcode_cleaning.ipynb))

- Ingest to AWS EMR Cluster using `scp`

- HDFS File Structure:

```
  ./yvesyang
    /zipcode
        /zipcode_city.csv
    /history_data
        /zip_market_before_2023.csv
```

### Data Manipulation

Launch Hive in AWS EMR:
`beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver`

#### Batch Layer with Hive Table

Create Hive Table from raw source csv files

- yvesyang_market_csv: contains all market data before 2023 ([Hive Code](./data_preparation/hive_market.hql))
- yvesyang_market_update_csv: contains all market data after 2023
- yvesyang_zipcode: contains US zipcode and corresponding primary city information ([Hive Code](./data_preparation/hive_zipcode.hql))

- yvesyang_market_orc: rewrite the yvesyang_market_csv table into [ORC](https://orc.apache.org/docs/) format ([Hive Code](./data_preparation/hive_market.hql))

Join US zipcode-city data and US Housing Market data to enable user's query by both zipcode and city name

- yvesyang_market_and_zip ([Hive Code](./data_preparation/market_and_zip.hql))

#### Serving Layer with HBase Table

Extract Essential Data from the combined hive big table and Write to HBase

Launch HBase using "hbase shell"
Create HBase table:
create 'yvesyang_zip_estate', 'md'

Write Essential Data to HBase for user query

Additionally, extract market data at City level using Spark Scala and Write to HBase

Create HBase table:
create 'yvesyang_city_estate', 'mdc'

[Code](./data_preparation/by_city_write_to_hbase.scala)

Write Essential Data to HBase for user query

An extra column "from_batch_layer" was created, to mark the HBase table record is from reliable Batch Layer Hive Table

## Data Query and Presentation

### Query

To query HBase, use HBase' REST interface, Creating a tunnel on port 8070

ssh -i ~/.ssh/yvesyang_mpcs53014.pem -L 8070:ec2-3-131-137-149.us-east-2.compute.amazonaws.com:8070 hadoop@ec2-3-131-137-149.us-east-2.compute.amazonaws.com

Node.js Application using JavaScript

Open localhost:8070

### Map Demonstration

Add openstreetmap

## Data Updates and Streaming

### Kafka

Create a new topic yvesyang_data_updates in Kafka to catch real-time data submission made at submit-data.html

./kafka-topics.sh --create --zookeeper z-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181 --replication-factor 2 --partitions 1 --topic yvesyang_data_updates

Use the console consumer to see new data record :
./kafka-console-consumer.sh --bootstrap-server b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic yvesyang_data_updates --from-beginning

### Web Submission

Create submit-data.html for market updates submission

ssh -i ~/.ssh/yvesyang_mpcs53014.pem ec2-user@ec2-3-143-113-170.us-east-2.compute.amazonaws.com

/yvesyang/zip_estate

node app.js 3059 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070 b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092

Submit data updates manually at:
http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3059/submit-data.html

### Data Streaming

Write Code to Stream Kafka Data Updates to Hbase

Here, the HBase extra column "from_batch_layer" will be False, to mark the new HBase table record is from real time data updates. When monthly official data is generated, to batch layer, to maintain data integrity.

Maven Install and Deploy to the Hadoop Cluster

spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamUpdates uber-process_data_updates-1.0-SNAPSHOT.jar b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092

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

```

```
