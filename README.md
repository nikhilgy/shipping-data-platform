# Shipping Data Platform

## Overview

This repo is for processing shipping and logistical data, ranging from a database BigQuery data mart tables. The objective is to clean, organize, and transform this data to provide valuable insights. We will implement Kedro pipelines for data ingestion and processing, and dbt for data transformation and standardization.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

### Tech Stack

- Python 3.11
- Kedro
- PostgreSQL
- BigQuery
- Airflow

### Overview of ETL pipeline
![shipping-data-platform (1)](https://github.com/user-attachments/assets/e6ccaec2-02dc-459f-8b2c-a69b2076edc3)

### Overview of Data Flow
![Data Flow](https://github.com/user-attachments/assets/db58efd2-c672-40fb-9c45-6e54122f2744)

## How to setup development environment

Clone the repo

```
git clone https://github.com/nikhilgy/shipping-data-platform.git
```

Activate virtual environment
```
python -m venv venv
source venv/Scripts/activate
```

Install dependencies mentioned in `requirements.txt`. To install them, run:
```
pip install -r requirements.txt
```

Setup database connection and add credentials

1. For Kedro project, create credentials.yml in `\conf\base\` for PostgreSQL source connection and BigQuery target connection

## How to run your Kedro pipeline

This project has 1 pipelines stage:

1. **data_extracting_and_cleaning** : Here we're identifying data issues such as duplicates, noise in values, wrong formats, fixing them and loading into raw database for 2 datasets: `Containers, Operations` . To run data cleaning, 
    
    ```
    kedro run --pipeline data_extracting_and_cleaning
    ```
