# ELT Data Pipeline with Airflow using Airbnb and Census data

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
