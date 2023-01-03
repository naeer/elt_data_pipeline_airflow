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
