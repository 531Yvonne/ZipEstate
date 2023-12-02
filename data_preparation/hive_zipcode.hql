-- This file will create a hive table with zipcode_city.csv data

CREATE EXTERNAL TABLE yvesyang_zipcode_city(
    zip string,
    primary_city string,
    state string,
    latitude decimal,
    longitude decimal,
    city_state string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/yvesyang/zipcode'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Test the table
SELECT * FROM yvesyang_zipcode_city LIMIT 5;