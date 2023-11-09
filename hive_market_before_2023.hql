-- This file will create an ORC table with zip_market_before_2023.csv data

-- First, map the CSV data we downloaded in Hive
CREATE EXTERNAL TABLE yvesyang_before2023_csv(
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
    median_sale_price decimal,
    median_sale_price_mom decimal,
    median_sale_price_yoy decimal,
    median_list_price decimal,
    median_list_price_mom decimal,
    median_list_price_yoy decimal,
    median_ppsf decimal,
    median_ppsf_mom decimal,
    median_ppsf_yoy decimal,
    median_list_ppsf decimal,
    median_list_ppsf_mom decimal,
    median_list_ppsf_yoy decimal,
    homes_sold decimal,
    homes_sold_mom decimal,
    homes_sold_yoy decimal,
    pending_sales decimal,
    pending_sales_mom decimal,
    pending_sales_yoy decimal,
    new_listings decimal,
    new_listings_mom decimal,
    new_listings_yoy decimal,
    inventory decimal,
    inventory_mom decimal,
    inventory_yoy decimal,
    median_dom decimal,
    median_dom_mom decimal,
    median_dom_yoy decimal,
    avg_sale_to_list decimal,
    avg_sale_to_list_mom decimal,
    avg_sale_to_list_yoy decimal,
    sold_above_list decimal,
    sold_above_list_mom decimal,
    sold_above_list_yoy decimal,
    off_market_in_two_weeks decimal,
    off_market_in_two_weeks_mom decimal,
    off_market_in_two_weeks_yoy decimal,
    parent_metro_region string,
    parent_metro_region_metro_code string,
    last_updated string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/yvesyang/history_data'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
SELECT period_begin, period_end, region, median_sale_price from yvesyang_before2023_csv limit 5;


-- Create an ORC table for the above data
CREATE TABLE yvesyang_before2023(
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
    median_sale_price decimal,
    median_sale_price_mom decimal,
    median_sale_price_yoy decimal,
    median_list_price decimal,
    median_list_price_mom decimal,
    median_list_price_yoy decimal,
    median_ppsf decimal,
    median_ppsf_mom decimal,
    median_ppsf_yoy decimal,
    median_list_ppsf decimal,
    median_list_ppsf_mom decimal,
    median_list_ppsf_yoy decimal,
    homes_sold decimal,
    homes_sold_mom decimal,
    homes_sold_yoy decimal,
    pending_sales decimal,
    pending_sales_mom decimal,
    pending_sales_yoy decimal,
    new_listings decimal,
    new_listings_mom decimal,
    new_listings_yoy decimal,
    inventory decimal,
    inventory_mom decimal,
    inventory_yoy decimal,
    median_dom decimal,
    median_dom_mom decimal,
    median_dom_yoy decimal,
    avg_sale_to_list decimal,
    avg_sale_to_list_mom decimal,
    avg_sale_to_list_yoy decimal,
    sold_above_list decimal,
    sold_above_list_mom decimal,
    sold_above_list_yoy decimal,
    off_market_in_two_weeks decimal,
    off_market_in_two_weeks_mom decimal,
    off_market_in_two_weeks_yoy decimal,
    parent_metro_region string,
    parent_metro_region_metro_code string,
    last_updated string)
STORED AS ORC;

-- Copy the CSV table to the ORC table
INSERT OVERWRITE TABLE yvesyang_before2023
SELECT * 
FROM yvesyang_before2023_csv
WHERE period_begin is not null and period_end is not null
and region is not null and median_sale_price is not null and median_list_price is not null;