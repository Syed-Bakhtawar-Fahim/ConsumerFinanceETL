# Libraries import
import requests
import pandas as pd
from datetime import date, timedelta
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
import json
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials


default_args = {
    'owner': "bakhtawar",
    'start_date': days_ago(0),
    'email': ['bakhtawarfahim10@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id = 'etl-scraping-csv-1',
    default_args = default_args,
    description = "This is complete ETL of 1 ",
    schedule_interval = timedelta(days=1)
)

# Extract data from given endpoints
def extract(url, ti):
    list_of_states = list(requests.get(url).json().keys())
    final_data = []
    for state in list_of_states:
        ('state', state)
        date_received_min = (date.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        size = 500
        date_received_max = (date.today()).strftime("%Y-%m-%d")
        url = f'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={size}&date_received_max={date_received_max}&date_received_min={date_received_min}&state={state}'
        response = requests.get(url)
        data = response.json()
        final_data.extend([entry['_source'] for entry in data.get('hits', {}).get('hits', [])])
        ti.xcom_push(key = "data", value = final_data)
    return final_data


def sql_query(ti):
    conn = mysql.connector.connect(
        host = 'host.docker.internal',
        password = "root",
        user = "root",
        database = "consumer_finance"

    )
    mycursor = conn.cursor()

    data = ti.xcom_pull(task_ids = "extract", key = "data")
    print("data", data)
    
    data_values = (
        data['complaint_what_happened'],
        data['date_sent_to_company'],
        data['issue'],
        data['sub_product'],
        data['zip_code'],
        data['tags'],
        data['has_narrative'],
        data['complaint_id'],
        data['timely'],
        data['consumer_consent_provided'],
        data['company_response'],
        data['submitted_via'],
        data['company'],
        data['date_received'],
        data['state'],
        data['consumer_disputed'],
        data['company_public_response'],
        data['sub_issue']
    )

    try:
        qry = ("INSERT INTO consumer (product, complaint_what_happened, date_sent_to_company, issue, sub_product, zip_code, tags, has_narrative, complaint_id, timely, consumer_consent_provided, company_response, submitted_via, company, date_received, state, consumer_disputed, company_public_response, sub_issue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.executemany(qry, data_values)
        conn.commit()
        print("Data inserted")
    
    except mysql.connector.Error as error:
        print(f"Something went wrong due to: {error}")
        conn.rollback()
    finally:
        conn.close()


def transform(**kwargs):
    import_data = ti.xcom_pull(key = "data")
    df = pd.DataFrame(import_data)

    unnecessary_columns = ['complaint_what_happened', 'date_sent_to_company', 'zip_code',
                           'tags', 'has_narrative', 'consumer_consent_provided',
                           'consumer_disputed', 'company_public_response']  
    df.drop(columns=unnecessary_columns, inplace=True)

    df['date_received'] = pd.to_datetime(df['date_received'])
    df['month_year'] = df['date_received'].dt.to_period('M')

    dim_columns = ['product', 'issue', 'sub_product', 'timely', 'company_response',
                   'submitted_via', 'company', 'state', 'sub_issue', 'month_year']
    grouped_data = df.groupby(dim_columns).size().reset_index(name='complaint_count')
    print(grouped_data.head())  

    ti = kwargs['ti']
    ti.xcom_push(key='data', value=grouped_data.to_json(orient='records'))


def load_to_google_sheet(data, spreadsheet_id, sheet_name, credentials_path): 
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet(sheet_name)
    sheet.clear()
    data = json.loads(transformed_data)
    headers = list(data[0].keys())
    sheet.insert_row(headers, index=1)
    rows = [[row[col] for col in headers] for row in data]
    sheet.add_rows(len(rows))
    for i, row in enumerate(rows):
        sheet.insert_row(row, index=i+2)

extract_data = PythonOperator(
    task_id = "extract",
    python_callable = extract,
    op_kwargs = {'url': "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json"},
    dag = dag
)

insert_into_table = PythonOperator(
    task_id = "Load",
    python_callable = sql_query,
    dag = dag
)
transform_data = PythonOperator(
    task_id = "Transform",
    python_callable = transform,
    dag = dag
)
load_data_sheet = PythonOperator(
    task_id = "GoogleSheet",
    python_callable = load_to_google_sheet,
    dag = dag
)



extract_data >> insert_into_table >> transform_data >> load_data_sheet