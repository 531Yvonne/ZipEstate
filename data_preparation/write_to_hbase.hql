-- Write Hive Table to HBase

CREATE EXTERNAL TABLE yvesyang_zip_estate(
    record_key string,
    year smallint,
    month tinyint,
    zipcode string,
    city string,
    state string,
    latitude double,
    longitude double,
    city_state string,
    property_type string,
    property_type_id tinyint,
    median_sale_price bigint,
    median_list_price bigint,
    homes_sold bigint,
    pending_sales bigint,
    new_listings bigint,
    inventory bigint,
    median_dom bigint,
    off_market_in_two_weeks bigint)
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
    md:median_sale_price,
    md:median_list_price,
    md:homes_sold,
    md:pending_sales,
    md:new_listings,
    md:inventory,
    md:median_dom,
    md:off_market_in_two_weeks')
TBLPROPERTIES ('hbase.table.name' = 'yvesyang_zip_estate');


INSERT OVERWRITE TABLE yvesyang_zip_estate
SELECT
    CONCAT(year, "_", month, "_", zipcode, "_", property_type_id),
    year, month, zipcode, city, state, latitude, longitude, city_state,
    property_type, property_type_id,
    median_sale_price,
    median_list_price,
    homes_sold,
    pending_sales,
    new_listings,
    inventory,
    median_dom,
    off_market_in_two_weeks
FROM yvesyang_market_essentials;

-- Test
SELECT * FROM yvesyang_zip_estate LIMIT 1;