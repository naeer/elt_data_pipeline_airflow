# ELT Data Pipeline utilising Snowflake and Airflow

## Overview
Airbnb has revolutionized the accommodation industry by providing an online marketplace where users can rent out their properties and other users can rent these properties for a fixed period of time. It was founded in 2008 by Joe Gebbia, Brian Chesky, and Nathan Blecharczyk and ever since its inception it has been a major disruption to the traditional hospitality industry. As of 2019, Airbnb has more than 150 million guest users and 6 million+ listings worldwide, meaning that it generates a huge amount of data including details about the rentals, their prices and the density of these rentals across regions. Sydney is going to be the focus of this project as it is one of the most popular cities in the world for tourism.

In addition to the Airbnb data, this project makes use of 2016 census data collected by Australian Bureau of Statistics to contextualize the Airbnb listings data and to aid in insights regarding the listing neighbourhoods of these rentals. The aim of this project is to build production-ready pipelines with Airflow, where the input datasets are processed and cleaned before loading into a data warehouse using ELT pipelines. Once the data is loaded into a warehouse, they need to be transformed and then transferred into a data mart for analytical purposes.

## Data Understanding
The Airbnb's listings data was obtained from [Airbnb's website](http://insideairbnb.com/get-the-data/) and contained data from May 2020 to April 2021. The 2016 census data was obtained from [Australian Bureau of Statistics' website](https://datapacks.censusdata.abs.gov.au/datapacks/) and an additional dataset was retrieved to help with the mapping of Airbnb data and 2016 census data. This dataset contained the LGA codes, LGA names and suburb names.


| Dataset | File name | Details about dataset |
| --- | --- | --- |
|        | 05_2020.csv | 
|        | 06_2020.csv |
|        | 07_2020.csv |
|        | 08_2020.csv |
|        | 09_2020.csv |
| Airbnb | 10_2020.csv | Listings data for all the months including data about the property type, host, price and customer reviews |
|        | 11_2020.csv |
|        | 12_2020.csv |
|        | 01_2021.csv |
|        | 02_2021.csv |
|        | 03_2021.csv |
|        | 04_2021.csv |
| 2016 Census |  2016Census_G01_NSW_LGA.csv | People characteristics by Sex for each LGA |
|             | 2016Census_G02_NSW_LGA.csv | Medians and Averages for each LGA |
| NSW LGA |  NSW_LGA_CODE.csv | LGA Code and LGA names |
|         | NSW_LGA_SUBURB.csv | Suburb names and their corresponding LGA |

## Overall Architecture

![](https://github.com/naeer/elt_data_pipeline_airflow/blob/main/images/architecture.png?raw=true)

- Google Cloud Storage: At the start of the project, all the Airbnb listings data, census data, and New South Wales LGA data was uploaded to the Google cloud storage in three separate folders, which were named as listings, Census_LGA, NSW_LGA.
- Data Warehouse: Once the data files were uploaded to the google cloud storage, they were extracted and stored in the raw layer of the data warehouse as Snowflake external tables. Afterwards, they were loaded in the staging layer as Snowflake internal tables and then transformed into a star schema and eventually stored in the warehouse layer as dimension and fact tables.
- Data Mart: Using the dimension and fact tables from the warehouse layer, 3 tables with KPIs were generated and stored in the data mart layer of the Data Warehouse.
- Airflow: Airflow was used to build a data pipeline and automate the above-mentioned processes. It ensured that any changes made to the raw files in the google cloud storage will be detected and once triggered, the data pipeline would be executed with the newly modified files.

## Data Warehouse Design
A data warehouse was designed with 4 separate layers which were named as raw, staging, warehouse and data mart. In the warehouse, the data was decided to be stored in a star schema as it makes accessing the data much faster and the queries much simpler.

### Raw schema
In the raw schema, all the data files from the three datasets were loaded as external tables in Snowflake following the ELT process.

### Staging schema
In the staging schema, all the data files from the external tables were loaded into proper data tables (following the ELT process) by parsing existing fields with appropriate column types. All the columns from the census data were not loaded as they were deemed not crucial for the analysis conducted for this project.

### Datawarehouse schema
The tables from the staging schema were transformed and loaded into the datawarehouse schema as fact and dimension tables. These fact and dimension tables were created to design a star schema. A star schema usually has a fact table in the centre and points to all the dimension tables around it with the help of foreign keys. In this project, a total of 5 dimension tables were created along with 1 fact table.

![](https://github.com/naeer/elt_data_pipeline_airflow/blob/main/images/star_schema.png?raw=true)

### Data Mart Schema
Once all the fact and dimension tables were created and loaded in the datawarehouse schema. 3 tables were created in the data mart schema to calculate some key performance indicators from different points of view.

The first data mart table, dm_listing_neighbourhood, calculated various KPIs including the active listings rate, superhost rate, percentage change for active listings, and average estimated revenue per active listings for each listing_neighbourhood and month_year. The output of this table is stored in `dm_listing_neighbourhood.csv`

The second data mart table, dm_property_type, calculated the same KPIs as the dm_listing_neighbourhood but this time for each listing type. The KPIs were calculated for each property type, room type, accommodates and month_year. This was stored in `dm_property_type.csv`

The third data mart table, dm_host_neighbourhood, calculated the following KPIs: number of distinct hosts, estimated revenue, and estimated revenue per host for each host_neighbourhood_lga and month_year. The output of this final table was stored in `dm_host_neighbourhood.csv`

## Populating Data Warehouse following an ELT pattern using Airflow
Airflow was used to build a data pipeline and automate the process of extracting, loading, and transforming the data. Airflow makes use of Directed Acyclic Graphs (DAGs) to specify the dependencies between tasks and to order them in such a way that it does not create any hindrance while the data pipeline is running. One of the biggest advantages of Airflow is that it not only allows the user to create workflows but also allows them to schedule and monitor workflows.

As part of this project, the following tasks were created in python to automate the process of extracting, loading, and transforming the data:
- refresh_dim_lga_task: Refreshes the nsw_lga_code and census external tables and loads them into staging before creating a dimension table for lga
- refresh_dim_listings_task: Refreshes the listings external table and loads it into staging before creating a dimension table for listings
- refresh_dim_suburb_task: Refreshes the nsw_lga_suburb external table and loads it into staging before creating a dimension table for suburb
- refresh_dim_host_task: Creates a host dimension table from staging.listings table
- refresh_dim_date_task: Creates a host dimension table from staging.listings table
- refresh_fact_listings_task: Creates a fact table by combining all the dimension tables
- refresh_datamart_listing_neighbourhood_task: Creates a data mart table that contains KPIs for each listing neighbourhood
- refresh_datamart_property_type_task: Creates a data mart table that contains KPIs for each property type
- refresh_datamart_host_neighbourhood_task: Creates a data mart table that contains KPIs for each host neighbourhood

As can be seen from the description of the tasks, some of the tasks were dependent on other tasks. So, a Directed Acyclic Graph (DAG) was constructed to ensure that the tasks were executed in the correct order. The following figure shows the DAG.

![](https://github.com/naeer/elt_data_pipeline_airflow/blob/main/images/dag_airflow.png?raw=true)
