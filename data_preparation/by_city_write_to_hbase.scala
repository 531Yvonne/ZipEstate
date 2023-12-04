-- Create Hive Table using Spark Scala, then write to HBase

val essential = spark.table("yvesyang_market_essentials")
essential.createOrReplaceTempView("essential")

val essential_by_city = spark.sql("""SELECT year, month, city, state,
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude,
    city_state,
    property_type,
    property_type_id,
    PERCENTILE_APPROX(median_sale_price, 0.5) AS median_sale_price,
    PERCENTILE_APPROX(median_list_price, 0.5) AS median_list_price,
    PERCENTILE_APPROX(median_ppsf, 0.5) AS median_ppsf,
    PERCENTILE_APPROX(median_list_ppsf, 0.5) AS median_list_ppsf,
    SUM(homes_sold) AS homes_sold,
    SUM(pending_sales) AS pending_sales,
    SUM(new_listings) AS new_listings,
    SUM(inventory) AS inventory,
    PERCENTILE_APPROX(median_dom, 0.5) AS median_dom,
    AVG(avg_sale_to_list) AS avg_sale_to_list,
    AVG(sold_above_list) AS sold_above_list,
    AVG(off_market_in_two_weeks) AS off_market_in_two_weeks
  FROM essential
  GROUP BY year, month, city, state, city_state, property_type, property_type_id
""")

import org.apache.spark.sql.SaveMode
essential_by_city.write.mode(SaveMode.Overwrite).saveAsTable("yvesyang_market_essentials_by_city")

-- Test
SELECT year, month, property_type,
    city, state,
    median_sale_price,
    median_list_price 
FROM yvesyang_market_essentials_by_city
WHERE city_state = "Chicago, IL"
LIMIT 5;


CREATE EXTERNAL TABLE yvesyang_city_estate(
    record_key string,
    year smallint,
    month tinyint,
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
    off_market_in_two_weeks bigint,
    from_batch_layer boolean)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,
    mdc:year,
    mdc:month,
    mdc:city,
    mdc:state,
    mdc:latitude,
    mdc:longitude,
    mdc:city_state,
    mdc:property_type,
    mdc:property_type_id,
    mdc:median_sale_price,
    mdc:median_list_price,
    mdc:homes_sold,
    mdc:pending_sales,
    mdc:new_listings,
    mdc:inventory,
    mdc:median_dom,
    mdc:off_market_in_two_weeks,
    mdc:from_batch_layer')
TBLPROPERTIES ('hbase.table.name' = 'yvesyang_city_estate');


INSERT OVERWRITE TABLE yvesyang_city_estate
SELECT
    CONCAT(year, "_", month, "_", city, ", ", state, "_", property_type_id),
    year, month, city, state, latitude, longitude, city_state,
    property_type, property_type_id,
    median_sale_price,
    median_list_price,
    homes_sold,
    pending_sales,
    new_listings,
    inventory,
    median_dom,
    off_market_in_two_weeks,
    TRUE as from_batch_layer
FROM yvesyang_market_essentials_by_city;

-- Test
SELECT * FROM yvesyang_city_estate LIMIT 1;