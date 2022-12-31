-- Part 3a
-- Finding the differences between best performing and worst performing listing neighbourhoods in terms of Under 35 population
with listing_neighbourhood_monthly_stats as(
    select
    lga_name as listing_neighbourhood, 
    month_year,
    sum(median_age_people)/count(*) as median_age,
    sum(tot_p_p)/count(*) as total_population,
    sum(age_0_4_yr_p)/count(*) as age_0_4_population,
    sum(age_5_14_yr_p)/count(*) as age_5_14_population,
    sum(age_15_19_yr_p)/count(*) as age_15_19_population,
    sum(age_20_24_yr_p)/count(*) as age_20_24_population,
    sum(age_25_34_yr_p)/count(*) as age_25_34_population,
    avg(case when has_availability = TRUE then ((30-availability_30)*price) END) as                                 avg_estimated_revenue_per_active_listing
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_date on datawarehouse.fact_listings.date_id = datawarehouse.dim_date.date_id
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    group by listing_neighbourhood, month_year
)
select 
listing_neighbourhood,
sum(median_age)/count(*) as median_age,
sum(total_population)/count(*) as total_population,
(sum(age_0_4_population)/count(*)) + (sum(age_5_14_population)/count(*)) + (sum(age_15_19_population)/count(*)) + (sum(age_20_24_population)/count(*)) + (sum(age_25_34_population)/count(*)) as under_35_total_population,
sum(avg_estimated_revenue_per_active_listing) as estimated_revenue_per_active_listing
from listing_neighbourhood_monthly_stats
group by listing_neighbourhood
order by estimated_revenue_per_active_listing desc;


-- Part 3b
-- Finding the best type of listing type for top 5 listing neighbourhoods in terms of estimated revenue per active listing
-- create cte that has the neighbourhoods listed in terms of estimated revenue per active listing
-- create another cte that has the neighbourhoods and their listing types with total stays
-- join the two ctes by the listing neighbourhood to find the best listing type for the top 5 listing neighbourhoods
with listing_neighbourhoods_est_revenue as (
    with listing_neighbourhood_monthly_stats as (
        select
        lga_name as listing_neighbourhood,
        month_year,
        avg(case when has_availability = TRUE then ((30-availability_30)*price) END) as                                 avg_estimated_revenue_per_active_listing
        from datawarehouse.fact_listings
        left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
        left join datawarehouse.dim_date on datawarehouse.fact_listings.date_id = datawarehouse.dim_date.date_id
        left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
        group by listing_neighbourhood, month_year
    )
    select 
    listing_neighbourhood,
    sum(avg_estimated_revenue_per_active_listing) as estimated_revenue_per_active_listing,
    row_number() over(order by estimated_revenue_per_active_listing desc) as rk_est_revenue
    from listing_neighbourhood_monthly_stats
    group by listing_neighbourhood
),
listing_neighbourhood_stays as (
    select
    lga_name as listing_neighbourhood,
    property_type,
    room_type,
    accomodates,
    sum(30-availability_30) as total_stays,
    row_number() over(partition by listing_neighbourhood order by total_stays desc) as rank_stays
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    group by listing_neighbourhood, property_type, room_type, accomodates
)
select 
listing_neighbourhoods_est_revenue.listing_neighbourhood,
property_type,
room_type,
accomodates,
total_stays,
estimated_revenue_per_active_listing
from listing_neighbourhoods_est_revenue
left join listing_neighbourhood_stays on listing_neighbourhoods_est_revenue.listing_neighbourhood = listing_neighbourhood_stays.listing_neighbourhood
where rk_est_revenue <=5 and rank_stays = 1
order by estimated_revenue_per_active_listing desc;


-- Part 3c
-- Do hosts with multiple listings have their listings in the same LGA as they live?
-- create a cte to find hosts with multiple listings
-- create another cte to find hosts and their lga
-- create another cte to find the listings cte for each host
-- join the three ctes to find if the host lga is same as the listing lga and create a temporary view
create or replace view datamart.hosts_lga_same_as_listing_lga as 
with hosts_multiple_listings as (
    select 
    original_host_id,
    count(distinct(original_listing_id)) as num_listings
    from datawarehouse.fact_listings
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    group by original_host_id
    having num_listings > 1
),
hosts_lga as (
    select 
    lga_name as host_lga,
    datawarehouse.dim_host.auto_gen_host_id,
    original_host_id,
    suburb_name
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    left join datawarehouse.dim_suburb on datawarehouse.fact_listings.suburb_id = datawarehouse.dim_suburb.suburb_id
    where suburb_name is not null
),
listings_lga as (
    select
    datawarehouse.dim_host.auto_gen_host_id,
    original_host_id,
    original_listing_id,
    lga_name as listing_lga
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
)
select 
distinct(hosts_lga.auto_gen_host_id),
hosts_lga.original_host_id,
host_lga,
listing_lga,
case when host_lga = listing_lga then TRUE else FALSE END as hosts_lga_same_as_listing_lga,
num_listings
from hosts_lga
inner join listings_lga on hosts_lga.auto_gen_host_id = listings_lga.auto_gen_host_id
inner join hosts_multiple_listings on hosts_lga.original_host_id = hosts_multiple_listings.original_host_id;

-- view datamart.hosts_lga_same_as_listing_lga
select * from datamart.hosts_lga_same_as_listing_lga;

-- Finding the total count of cases where host lga was same as listing lga
select 
hosts_lga_same_as_listing_lga,
count(*) as total_count
from datamart.hosts_lga_same_as_listing_lga
group by hosts_lga_same_as_listing_lga;

-- Part 3d
-- Can hosts with a unique listing cover the annualised median mortgage repayment with their estimated revenue?
-- create a cte with the annual revenue for each host
-- create a cte with annualised mortgage repayment for each host's listing neighbourhood 
-- join the two ctes together to find if the hosts can cover the annualised mortgage repayment and create a temporary view
create or replace view datamart.hosts_can_cover_mortgage as
with hosts_annual_revenue as (
    select 
    original_host_id,
    count(distinct(original_listing_id)) as num_listings,
    sum(case when has_availability = TRUE then ((30-availability_30)*price) END) as estimated_annual_revenue
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    group by original_host_id
    having num_listings = 1
),
annualised_mortgage_revenue as (
    select
    distinct original_host_id,
    lga_name as listing_neighbourhood,
    median_mortgage_repay_monthly*12 as annualised_median_mortgage
    from datawarehouse.fact_listings
    left join datawarehouse.dim_lga on datawarehouse.fact_listings.lga_code = datawarehouse.dim_lga.lga_code
    left join datawarehouse.dim_listings on datawarehouse.fact_listings.auto_gen_listing_id= datawarehouse.dim_listings.auto_gen_listing_id
    left join datawarehouse.dim_host on datawarehouse.fact_listings.auto_gen_host_id = datawarehouse.dim_host.auto_gen_host_id
    order by annualised_median_mortgage
)
select 
hosts_annual_revenue.original_host_id, 
listing_neighbourhood,
estimated_annual_revenue,
annualised_median_mortgage,
estimated_annual_revenue >= annualised_median_mortgage as can_cover_mortgage
from hosts_annual_revenue
inner join annualised_mortgage_revenue on hosts_annual_revenue.original_host_id = annualised_mortgage_revenue.original_host_id;

-- view datamart.hosts_can_cover_mortgage
select * from datamart.hosts_can_cover_mortgage;

-- Finding the number of hosts that can cover mortgage with their estimated revenue
select 
can_cover_mortgage,
count(*) num_hosts
from datamart.hosts_can_cover_mortgage
group by can_cover_mortgage;
