-- Write Hive Table to HBase

CREATE EXTERNAL TABLE yvesyang_housing_market_data(
    record_key string,
    year smallint,
    month tinyint,
    zipcode string,
    city string,
    state string,
    latitude decimal,
    longitude decimal,
    city_state string,
    property_type string,
    property_type_id tinyint,
    median_sale_price decimal,
    median_list_price decimal,
    median_ppsf decimal,
    median_list_ppsf decimal,
    homes_sold decimal,
    pending_sales decimal,
    new_listings decimal,
    inventory decimal,
    median_dom decimal,
    avg_sale_to_list decimal,
    sold_above_list decimal,
    off_market_in_two_weeks decimal)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,
    md:year,
    md:month,
    md:zipcode,
    md:city,
    md:state,
    md:latitude,
    md:longitude,
    md:city_state,
    md:property_type,
    md:property_type_id,
    md:median_sale_price#b,
    md:median_list_price#b,
    md:median_ppsf#b,
    md:median_list_ppsf#b,
    md:homes_sold#b,
    md:pending_sales#b,
    md:new_listings#b,
    md:inventory#b,
    md:median_dom#b,
    md:avg_sale_to_list#b,
    md:sold_above_list#b,
    md:off_market_in_two_weeks#b')
TBLPROPERTIES ('hbase.table.name' = 'yvesyang_housing_market_data');


INSERT OVERWRITE TABLE yvesyang_housing_market_data
SELECT
    CONCAT(year, "_", month, "_", zipcode, "_", property_type_id),
    year, month, zipcode, city, state, latitude, longitude, city_state,
    property_type, property_type_id,
    median_sale_price,
    median_list_price,
    median_ppsf,
    median_list_ppsf,
    homes_sold,
    pending_sales,
    new_listings,
    inventory,
    median_dom,
    avg_sale_to_list,
    sold_above_list,
    off_market_in_two_weeks
FROM yvesyang_market_essentials;