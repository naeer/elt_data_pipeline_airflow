import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


#########################################################
#
#   Load Environment Variables
#
#########################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"

########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'Naeer',
    'start_date': datetime.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='bde_at_3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

query_refresh_dim_suburb = f"""
alter external table raw.nsw_lga_suburb refresh;

-- Transferring the data from raw.nsw_lga_suburb to staging as a table
create or replace table staging.nsw_lga_suburb as
select 
    value:c1::varchar as lga_name
    , value:c2::varchar as suburb_name
from raw.nsw_lga_suburb;

-- Create an intermediate table to get the mapping of the lga code to the suburb
create or replace table staging.lga_code_suburb as
select staging.nsw_lga_code.lga_code, staging.nsw_lga_code.lga_name, staging.nsw_lga_suburb.suburb_name
from staging.nsw_lga_suburb
left join staging.nsw_lga_code on lower(staging.nsw_lga_suburb.lga_name) = lower(staging.nsw_lga_code.lga_name);

-- Create a suburb dimension table with suburb_id, lg_code and suburb_name
create or replace table datawarehouse.dim_suburb as
select 
row_number() over (order by count(*) desc) as suburb_id, 
lga_code, suburb_name
from staging.lga_code_suburb
group by lga_code, suburb_name;
"""

query_refresh_dim_lga = f"""
alter external table raw.nsw_lga_code refresh;

-- Transferring the data from raw.nsw_lga_code to staging as a table
create or replace table staging.nsw_lga_code as
select 
    value:c1::int as lga_code
    , value:c2::varchar as lga_name
from raw.nsw_lga_code;

alter external table raw.go1_census refresh;

-- Transferring the data from raw.go1_census to staging as a table 
create or replace table staging.go1_census as
select
    value:c1::varchar as lga_code
    , value:c2::int as Tot_P_M
    , value:c3::int as Tot_P_F
    , value:c4::int as Tot_P_P
    , value:c5::int as Age_0_4_yr_M
    , value:c6::int as Age_0_4_yr_F
    , value:c7::int as Age_0_4_yr_P
    , value:c8::int as Age_5_14_yr_M
    , value:c9::int as Age_5_14_yr_F
    , value:c10::int as Age_5_14_yr_P
    , value:c11::int as Age_15_19_yr_M
    , value:c12::int as Age_15_19_yr_F
    , value:c13::int as Age_15_19_yr_P
    , value:c14::int as Age_20_24_yr_M
    , value:c15::int as Age_20_24_yr_F
    , value:c16::int as Age_20_24_yr_P
    , value:c17::int as Age_25_34_yr_M
    , value:c18::int as Age_25_34_yr_F
    , value:c19::int as Age_25_34_yr_P
    , value:c20::int as Age_35_44_yr_M
    , value:c21::int as Age_35_44_yr_F
    , value:c22::int as Age_35_44_yr_P
    , value:c23::int as Age_45_54_yr_M
    , value:c24::int as Age_45_54_yr_F
    , value:c25::int as Age_45_54_yr_P
    , value:c26::int as Age_55_64_yr_M
    , value:c27::int as Age_55_64_yr_F
    , value:c28::int as Age_55_64_yr_P
    , value:c29::int as Age_65_74_yr_M
    , value:c30::int as Age_65_74_yr_F
    , value:c31::int as Age_65_74_yr_P
    , value:c32::int as Age_75_84_yr_M
    , value:c33::int as Age_75_84_yr_F
    , value:c34::int as Age_75_84_yr_P
    , value:c35::int as Age_85ov_M
    , value:c36::int as Age_85ov_F
    , value:c37::int as Age_85ov_P
    , value:c38::int as Counted_Census_Night_home_M
    , value:c39::int as Counted_Census_Night_home_F
    , value:c40::int as Counted_Census_Night_home_P
    , value:c41::int as Count_Census_Nt_Ewhere_Aust_M
    , value:c42::int as Count_Census_Nt_Ewhere_Aust_F
    , value:c43::int as Count_Census_Nt_Ewhere_Aust_P
from raw.go1_census;

-- Adding a column to go1 census table with clean lga code
create or replace table staging.go1_census as
select substr(lga_code, 4)::int as clean_lga_code, * from staging.go1_census;

alter external table raw.go2_census refresh;

-- Transferring the data from raw.go2_census to staging as a table 
create or replace table staging.go2_census as
select
    value:c1::varchar as lga_code
    , value:c2::int as median_age_people
    , value:c3::int as median_mortgage_repay_monthly
    , value:c4::int as median_tot_prsnl_inc_weekly
    , value:c5::int as median_rent_weekly
    , value:c6::int as median_tot_fam_inc_weekly
    , value:c7::int as average_num_psns_per_bedroom
    , value:c8::int as median_tot_hhd_inc_weekly
    , value:c9::int as average_household_size
from raw.go2_census;

-- Adding a column to go2 census table with clean lga code
create or replace table staging.go2_census as
select substr(lga_code, 4)::int as clean_lga_code, * from staging.go2_census;

-- Create a lga dimension table
create or replace table datawarehouse.dim_lga as 
select 
staging.nsw_lga_code.lga_code,
staging.nsw_lga_code.lga_name,
staging.go1_census.tot_p_m,
staging.go1_census.tot_p_f,
staging.go1_census.tot_p_p,
staging.go1_census.Age_0_4_yr_M,
staging.go1_census.Age_0_4_yr_F,
staging.go1_census.Age_0_4_yr_P,
staging.go1_census.Age_5_14_yr_M,
staging.go1_census.Age_5_14_yr_F,
staging.go1_census.Age_5_14_yr_P,
staging.go1_census.Age_15_19_yr_M,
staging.go1_census.Age_15_19_yr_F,
staging.go1_census.Age_15_19_yr_P,
staging.go1_census.Age_20_24_yr_M,
staging.go1_census.Age_20_24_yr_F,
staging.go1_census.Age_20_24_yr_P,
staging.go1_census.Age_25_34_yr_M,
staging.go1_census.Age_25_34_yr_F,
staging.go1_census.Age_25_34_yr_P,
staging.go2_census.median_age_people,
staging.go2_census.median_mortgage_repay_monthly,
staging.go2_census.median_tot_prsnl_inc_weekly,
staging.go2_census.median_rent_weekly,
staging.go2_census.median_tot_fam_inc_weekly,
staging.go2_census.average_num_psns_per_bedroom,
staging.go2_census.median_tot_hhd_inc_weekly,
staging.go2_census.average_household_size
from staging.nsw_lga_code
left join staging.go1_census on staging.nsw_lga_code.lga_code = staging.go1_census.clean_lga_code
left join staging.go2_census on staging.nsw_lga_code.lga_code = staging.go2_census.clean_lga_code
;
"""

query_refresh_dim_listings = f"""
alter external table raw.listings refresh;

-- Transferring the data from raw.listings to staging as a table 
create or replace table staging.listings as
select 
    value:c1::int as listing_id
    , value:c2::bigint as scrape_id
    , value:c3::date as scrape_date
    , value:c4::int as host_id
    , value:c5::varchar as host_name
    , value:c6::varchar as host_since
    , value:c7::boolean as host_is_superhost
    , value:c8::varchar as host_neighbourhood
    , value:c9::varchar as listing_neighbourhood
    , value:c10::varchar as property_type
    , value:c11::varchar as room_type
    , value:c12::int as accomodates
    , value:c13::int as price
    , value:c14::boolean as has_availability
    , value:c15::int as availability_30
    , value:c16::int as number_reviews
    , value:c17::int as review_scores_ratings
    , value:c18::int as review_scores_accuracy
    , value:c19::int as review_scores_cleanliness
    , value:c20::int as review_scores_checkin
    , value:c21::int as review_scores_communication
    , value:c22::int as review_scores_value
    , split_part(substr(metadata$filename, 15), '.', 0)::varchar as month_year
from raw.listings;

-- Adding host_since_date and month_year_date columns with the correct data type and format
create or replace table staging.listings as
select *, 
to_date(host_since, 'DD/MM/YYYY')::date as host_since_date,
to_date('01' || '_' || month_year, 'DD_MM_YYYY')::date as month_year_date
from staging.listings;

-- Drop host_since and month_year column from listings table
alter table staging.listings
drop column host_since, month_year;

-- Rename host_since_date column to host_since 
alter table staging.listings
rename column host_since_date to host_since;

-- Rename month_year_date column to month_year 
alter table staging.listings
rename column month_year_date to month_year;

-- Create listings dimension table
create or replace table datawarehouse.dim_listings as
select 
row_number() over (order by count(*) desc) as auto_gen_listing_id,
listing_id as original_listing_id, property_type, room_type, accomodates, has_availability
from staging.listings
group by original_listing_id, property_type, room_type, accomodates, has_availability;
"""

query_refresh_dim_host = f"""
-- Create host dimension table
create or replace table datawarehouse.dim_host as
select 
row_number() over (order by count(*) desc) as auto_gen_host_id,
host_id as original_host_id, host_name, host_is_superhost, host_since
from staging.listings
group by original_host_id, host_name, host_is_superhost, host_since;
"""

query_refresh_dim_date = f"""
-- Create a dimension table for date 
create or replace table datawarehouse.dim_date as
select
row_number() over (order by count(*) desc) as date_id,
month_year
from staging.listings
group by month_year;
"""

query_refresh_fact_listings = f"""
-- Create intermediary table by merging the listings table with listings, host, lga, date and suburb dimension tables
create or replace table datawarehouse.temp_listings_lga_suburb as
select
datawarehouse.dim_listings.auto_gen_listing_id,
datawarehouse.dim_listings.original_listing_id,
staging.listings.scrape_id,
staging.listings.scrape_date,
datawarehouse.dim_host.auto_gen_host_id,
datawarehouse.dim_host.original_host_id,
staging.listings.host_name,
staging.listings.host_since,
staging.listings.host_is_superhost,
staging.listings.host_neighbourhood,
staging.listings.listing_neighbourhood,
staging.listings.property_type,
staging.listings.room_type,
staging.listings.accomodates,
staging.listings.price,
staging.listings.has_availability,
staging.listings.availability_30,
staging.listings.number_reviews,
staging.listings.review_scores_ratings,
staging.listings.review_scores_accuracy,
staging.listings.review_scores_cleanliness,
staging.listings.review_scores_checkin,
staging.listings.review_scores_communication,
staging.listings.review_scores_value,
staging.nsw_lga_code.lga_code,
staging.nsw_lga_code.lga_name,
datawarehouse.dim_suburb.suburb_id,
datawarehouse.dim_suburb.suburb_name,
datawarehouse.dim_date.date_id,
datawarehouse.dim_date.month_year
from staging.listings
left join staging.nsw_lga_code on staging.listings.listing_neighbourhood = staging.nsw_lga_code.lga_name
left join datawarehouse.dim_suburb on lower(staging.listings.host_neighbourhood) = lower(datawarehouse.dim_suburb.suburb_name)
left join datawarehouse.dim_date on staging.listings.month_year = datawarehouse.dim_date.month_year
left join datawarehouse.dim_listings on staging.listings.listing_id = datawarehouse.dim_listings.original_listing_id and staging.listings.property_type = datawarehouse.dim_listings.property_type and staging.listings.room_type = datawarehouse.dim_listings.room_type and staging.listings.accomodates = datawarehouse.dim_listings.accomodates and staging.listings.has_availability = datawarehouse.dim_listings.has_availability
left join datawarehouse.dim_host on staging.listings.host_id = datawarehouse.dim_host.original_host_id and staging.listings.host_name = datawarehouse.dim_host.host_name and staging.listings.host_is_superhost = datawarehouse.dim_host.host_is_superhost and staging.listings.host_since = datawarehouse.dim_host.host_since;

-- Create fact table
create or replace table datawarehouse.fact_listings as 
select
auto_gen_listing_id, 
auto_gen_host_id, 
lga_code, 
suburb_id, 
date_id, 
price, 
availability_30,
number_reviews, 
review_scores_ratings
from datawarehouse.temp_listings_lga_suburb;

---- Adding primary key constraints to the dimension tables
alter table datawarehouse.dim_date add primary key (date_id);
alter table datawarehouse.dim_host add primary key (auto_gen_host_id);
alter table datawarehouse.dim_lga add primary key (lga_code);
alter table datawarehouse.dim_listings add primary key(auto_gen_listing_id);
alter table datawarehouse.dim_suburb add primary key(suburb_id);

-- Adding foreign key constraint for auto_gen_listing_id to fact_listings table
alter table datawarehouse.fact_listings
add constraint fact_listings_fk_dim_listings
foreign key (auto_gen_listing_id)
references datawarehouse.dim_listings (auto_gen_listing_id);

-- Adding foreign key constraint for auto_gen_host_id to fact_listings table
alter table datawarehouse.fact_listings
add constraint fact_listings_fk_dim_host
foreign key (auto_gen_host_id)
references datawarehouse.dim_host (auto_gen_host_id);

-- Adding foreign key constraint for lga_code to fact_listings table
alter table datawarehouse.fact_listings
add constraint fact_listings_fk_dim_lga
foreign key (lga_code)
references datawarehouse.dim_lga (lga_code);

-- Adding foreign key constraint for suburb_id to fact_listings table
alter table datawarehouse.fact_listings
add constraint fact_listings_fk_dim_suburb
foreign key (suburb_id)
references datawarehouse.dim_suburb (suburb_id);

-- Adding foreign key constraint for date_id to fact_listings table
alter table datawarehouse.fact_listings
add constraint fact_listings_fk_dim_date
foreign key (date_id)
references datawarehouse.dim_date (date_id);
"""

query_datamart_listing_neighbourhood = f"""
-- Create a listing_neighbourhood table for datamart schema grouping by listing_neighbourhood and month_year
create or replace table datamart.dm_listing_neighbourhood as 
with listing_neighbourhood_stats as (
    select
    lga_name as listing_neighbourhood, 
    month_year,
    count(case when has_availability = TRUE then 1 END) as total_active_listings,
    count(case when has_availability = FALSE then 1 END) as total_inactive_listings,
    count(case when has_availability = TRUE or has_availability = FALSE then 1 END) as total_listings,
    min(case when has_availability = TRUE then price END) as min_price,
    max(case when has_availability = TRUE then price END) as max_price,
    approx_percentile(case when has_availability = TRUE then price END, 0.5) as median_price,
    avg(case when has_availability = TRUE then price END) as avg_price,
    count(distinct(original_host_id)) as distinct_hosts,
    count(distinct(case when host_is_superhost = TRUE then original_host_id END)) as distinct_superhosts,
    avg(case when has_availability = TRUE then review_scores_ratings END) as avg_review_scores_ratings,
    sum(case when has_availability = TRUE then (30-availability_30) END) as total_stays,
    avg(case when has_availability = TRUE then ((30-availability_30)*price) END) as                                 avg_estimated_revenue_per_active_listing
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_date on datawarehouse.fact_listings.date_id = datawarehouse.dim_date.date_id
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    group by listing_neighbourhood, month_year
)
select 
listing_neighbourhood,
month_year,
case when total_listings = 0 then null else (total_active_listings/total_listings)*100 END as active_listings_rate,
min_price,
max_price,
median_price,
avg_price,
distinct_hosts,
case when distinct_hosts = 0 then null else (distinct_superhosts/distinct_hosts)*100 END as superhost_rate,
avg_review_scores_ratings,
case when lag(total_active_listings) over(partition by listing_neighbourhood order by month_year) = 0 then null else ((total_active_listings - lag(total_active_listings) over(partition by listing_neighbourhood order by month_year))/lag(total_active_listings) over(partition by listing_neighbourhood order by month_year))*100 END as      per_change_active_listings,
case when lag(total_inactive_listings) over(partition by listing_neighbourhood order by month_year) = 0 then null else
((total_inactive_listings - lag(total_inactive_listings) over(partition by listing_neighbourhood order by month_year))/lag(total_inactive_listings) over(partition by listing_neighbourhood order by month_year))*100 END as per_change_inactive_listings,
total_stays,
avg_estimated_revenue_per_active_listing
from listing_neighbourhood_stats
;
"""

query_refresh_datamart_property_type = f"""
-- Create a property_type table for datamart schema grouping by property_type, room_type, accomodates, month_year
create or replace table datamart.dm_property_type as 
with property_type_stats as (
    select
    property_type,
    room_type,
    accomodates,
    month_year,
    count(case when has_availability = TRUE then 1 END) as total_active_listings,
    count(case when has_availability = FALSE then 1 END) as total_inactive_listings,
    count(case when has_availability = TRUE or has_availability = FALSE then 1 END) as total_listings,
    min(case when has_availability = TRUE then price END) as min_price,
    max(case when has_availability = TRUE then price END) as max_price,
    approx_percentile(case when has_availability = TRUE then price END, 0.5) as median_price,
    avg(case when has_availability = TRUE then price END) as avg_price,
    count(distinct(original_host_id)) as distinct_hosts,
    count(distinct(case when host_is_superhost = TRUE then original_host_id END)) as distinct_superhosts,
    avg(case when has_availability = TRUE then review_scores_ratings END) as avg_review_scores_ratings,
    sum(case when has_availability = TRUE then (30-availability_30) END) as total_stays,
    avg(case when has_availability = TRUE then ((30-availability_30)*price) END) as                                 avg_estimated_revenue_per_active_listing
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_date on datawarehouse.fact_listings.date_id = datawarehouse.dim_date.date_id
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    group by property_type, room_type, accomodates, month_year
)
select 
property_type,
room_type,
accomodates,
month_year,
case when total_listings = 0 then null else (total_active_listings/total_listings)*100 END as active_listings_rate,
min_price,
max_price,
median_price,
avg_price,
distinct_hosts,
case when distinct_hosts = 0 then null else (distinct_superhosts/distinct_hosts)*100 END as superhost_rate,
avg_review_scores_ratings,
case when lag(total_active_listings) over(partition by property_type, room_type, accomodates order by month_year) = 0 then null else ((total_active_listings - lag(total_active_listings) over(partition by property_type, room_type, accomodates order by month_year))/lag(total_active_listings) over(partition by property_type, room_type, accomodates order by month_year))*100 END as per_change_active_listings,
case when lag(total_inactive_listings) over(partition by property_type, room_type, accomodates order by month_year) = 0 then null else ((total_inactive_listings - lag(total_inactive_listings) over(partition by property_type, room_type, accomodates order by month_year))/lag(total_inactive_listings) over(partition by property_type, room_type, accomodates order by month_year))*100 END as per_change_inactive_listings,
total_stays,
avg_estimated_revenue_per_active_listing
from property_type_stats
;
"""

query_refresh_datamart_host_neighbourhood = f"""
-- Create a host_neighbourhood table for datamart schema grouping by host_neighbourhood_lga, month_year
create or replace table datamart.dm_host_neighbourhood as
with host_neighbourhood_stats as (
    select 
    lga_name as host_neighbourhood_lga,
    month_year,
    count(distinct(original_host_id)) as distinct_hosts,
    sum((30 - availability_30)*price) as estimated_revenue,
    sum(case when has_availability = TRUE then ((30 - availability_30)*price) END) as estimated_revenue_active_listing
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_date on datawarehouse.fact_listings.date_id = datawarehouse.dim_date.date_id
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    left join datawarehouse.dim_suburb on datawarehouse.fact_listings.suburb_id = datawarehouse.dim_suburb.suburb_id
    group by host_neighbourhood_lga, month_year
)
select 
host_neighbourhood_lga,
month_year,
distinct_hosts,
estimated_revenue,
estimated_revenue_active_listing/distinct_hosts as estimated_revenue_per_host
from host_neighbourhood_stats
;
"""

#########################################################
#
#   DAG Operator Setup
#
#########################################################


refresh_dim_lga = SnowflakeOperator(
    task_id='refresh_dim_lga_task',
    sql=query_refresh_dim_lga,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_dim_listings = SnowflakeOperator(
    task_id='refresh_dim_listings_task',
    sql=query_refresh_dim_listings,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_dim_suburb = SnowflakeOperator(
    task_id='refresh_dim_suburb_task',
    sql=query_refresh_dim_suburb,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_dim_host = SnowflakeOperator(
    task_id='refresh_dim_host_task',
    sql=query_refresh_dim_host,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_dim_date = SnowflakeOperator(
    task_id='refresh_dim_date_task',
    sql=query_refresh_dim_date,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_fact_listings = SnowflakeOperator(
    task_id='refresh_fact_listings_task',
    sql=query_refresh_fact_listings,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_fact_datamart_listing_neighbourhood = SnowflakeOperator(
    task_id='refresh_datamart_listing_neighbourhood_task',
    sql=query_datamart_listing_neighbourhood,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_datamart_property_type = SnowflakeOperator(
    task_id='refresh_datamart_property_type_task',
    sql=query_refresh_datamart_property_type,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_datamart_host_neighbourhood = SnowflakeOperator(
    task_id='refresh_datamart_host_neighbourhood_task',
    sql=query_refresh_datamart_host_neighbourhood,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)


refresh_dim_lga >> refresh_dim_suburb
refresh_dim_listings >> refresh_dim_host
refresh_dim_listings >> refresh_dim_date
[refresh_dim_suburb, refresh_dim_host, refresh_dim_date] >> refresh_fact_listings
refresh_fact_listings >> refresh_fact_datamart_listing_neighbourhood
refresh_fact_listings >> refresh_datamart_property_type
refresh_fact_listings >> refresh_datamart_host_neighbourhood

