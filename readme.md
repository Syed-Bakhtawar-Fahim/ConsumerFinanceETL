# ETL Process with Airflow

This repository contains code for an Extract, Transform, Load (ETL) process implemented using Apache Airflow. The ETL process extracts data from an API endpoint, transforms it, and loads it into a MySQL database and a Google Sheet.

## Components

### 1. `extract_data` Task
- Extracts data from a given API endpoint which provides consumer complaint data for various states in the US.

### 2. `insert_into_table` Task
- Loads the extracted data into a MySQL database table named `consumer_finance`.

### 3. `transform_data` Task
- Transforms the extracted data by performing data cleaning and aggregation.
- Aggregates the complaint data by various dimensions such as product, issue, sub-product, etc.
- Converts the transformed data into JSON format and pushes it to XCom for further use.

### 4. `load_data_sheet` Task
- Loads the transformed data into a Google Sheet.
- Uses Google Sheets API for authentication and data loading.

## Requirements
- Python 3.x
- Apache Airflow
- MySQL database
- Google Developer Console project with Sheets API enabled
- Google service account credentials JSON file

## Configuration
- Ensure that Airflow is properly configured with the necessary connections and variables for MySQL and Google Sheets authentication.
- Modify the `default_args` dictionary in the DAG definition file to match your requirements, including email notifications and retry settings.

## Usage
1. Clone this repository.
2. Install the required Python packages using `pip install -r requirements.txt`.
3. Configure Airflow with the necessary connections and variables.
4. Place the Google service account credentials JSON file in a secure location and update the `credentials_path` variable in the DAG definition file.
5. Start the Airflow scheduler and web server.
6. Trigger the DAG manually or set up a schedule interval for automatic execution.


