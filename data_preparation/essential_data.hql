-- This file will extract Essential Data

CREATE TABLE yvesyang_market_essentials(
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
STORED AS ORC;

INSERT OVERWRITE TABLE yvesyang_market_essentials
SELECT
    CAST(SUBSTR(period_begin, 1, 4) AS SMALLINT) AS year,
    CAST(SUBSTR(period_begin, 6, 2) AS TINYINT) AS month,
    zipcode,
    primary_city AS city,
    state,
    latitude,
    longitude,
    city_state,
    property_type,
    property_type_id,
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
FROM yvesyang_combined_before2023;

-- Test the table
SELECT year, month, city_state, property_type,
    property_type_id,
    median_sale_price,
    median_list_price 
FROM yvesyang_market_essentials
WHERE year = 2020 AND zipcode = "27106"
LIMIT 5;