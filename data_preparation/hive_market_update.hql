--- THIS IS TEMP

-- Similarly, this file will create an ORC table with zip_market_after_2023.csv data

-- First, map the CSV data we downloaded in Hive
CREATE EXTERNAL TABLE yvesyang_after2023_csv(
    period_begin string,
    period_end string,
    period_duration tinyint,
    region_type string,
    region_type_id tinyint,
    table_id smallint,
    is_seasonally_adjusted string,
    region string,
    state string,
    state_code string,
    property_type string,
    property_type_id tinyint,
    median_sale_price double,
    median_sale_price_mom double,
    median_sale_price_yoy double,
    median_list_price double,
    median_list_price_mom double,
    median_list_price_yoy double,
    median_ppsf double,
    median_ppsf_mom double,
    median_ppsf_yoy double,
    median_list_ppsf double,
    median_list_ppsf_mom double,
    median_list_ppsf_yoy double,
    homes_sold double,
    homes_sold_mom double,
    homes_sold_yoy double,
    pending_sales double,
    pending_sales_mom double,
    pending_sales_yoy double,
    new_listings double,
    new_listings_mom double,
    new_listings_yoy double,
    inventory double,
    inventory_mom double,
    inventory_yoy double,
    median_dom double,
    median_dom_mom double,
    median_dom_yoy double,
    avg_sale_to_list double,
    avg_sale_to_list_mom double,
    avg_sale_to_list_yoy double,
    sold_above_list double,
    sold_above_list_mom double,
    sold_above_list_yoy double,
    off_market_in_two_weeks double,
    off_market_in_two_weeks_mom double,
    off_market_in_two_weeks_yoy double,
    parent_metro_region string,
    parent_metro_region_metro_code string,
    last_updated string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/yvesyang/new_2023_data'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
SELECT period_begin, period_end, region, median_sale_price from yvesyang_after2023_csv limit 5;


-- Create an ORC table for the above data
CREATE TABLE yvesyang_after2023(
    period_begin string,
    period_end string,
    period_duration tinyint,
    region_type string,
    region_type_id tinyint,
    table_id smallint,
    is_seasonally_adjusted string,
    region string,
    state string,
    state_code string,
    property_type string,
    property_type_id tinyint,
    median_sale_price double,
    median_sale_price_mom double,
    median_sale_price_yoy double,
    median_list_price double,
    median_list_price_mom double,
    median_list_price_yoy double,
    median_ppsf double,
    median_ppsf_mom double,
    median_ppsf_yoy double,
    median_list_ppsf double,
    median_list_ppsf_mom double,
    median_list_ppsf_yoy double,
    homes_sold double,
    homes_sold_mom double,
    homes_sold_yoy double,
    pending_sales double,
    pending_sales_mom double,
    pending_sales_yoy double,
    new_listings double,
    new_listings_mom double,
    new_listings_yoy double,
    inventory double,
    inventory_mom double,
    inventory_yoy double,
    median_dom double,
    median_dom_mom double,
    median_dom_yoy double,
    avg_sale_to_list double,
    avg_sale_to_list_mom double,
    avg_sale_to_list_yoy double,
    sold_above_list double,
    sold_above_list_mom double,
    sold_above_list_yoy double,
    off_market_in_two_weeks double,
    off_market_in_two_weeks_mom double,
    off_market_in_two_weeks_yoy double,
    parent_metro_region string,
    parent_metro_region_metro_code string,
    last_updated string)
STORED AS ORC;

-- Copy the CSV table to the ORC table
INSERT OVERWRITE TABLE yvesyang_after2023
SELECT * 
FROM yvesyang_after2023_csv
WHERE period_begin is not null and period_end is not null
and region is not null and median_sale_price is not null and median_list_price is not null;