from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG
from datetime import datetime, timedelta
import requests
import sys
import csv
import codecs
import pandas as pd
import urllib.request
import sys
import json
import requests
import sys
import csv
import codecs
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from google.cloud import storage



def etl_data():
 
    ResultBytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Boulder/2021-04-01/2023-12-15?unitGroup=metric&include=days&key=PTENURU5XPKQLR3DC9GEE88K6&contentType=json")
    
    #Parse the results as JSON
    jsonData = json.load(ResultBytes)

    #data = pd.json_normalize(jsonData, 'days', sep='_')
    #file_path = 'https://raw.githubusercontent.com/Sid-701/DCS-Homework/main/api1.csv'
    #data = pd.read_csv(file_path,index_col=0)
    label_mapping = {
        'Clear': 'Clear',
        'Rain, Partially cloudy': 'Rain',
        'Rain': 'Rain',
        'Partially cloudy': 'Cloudy',
        'Snow, Rain, Partially cloudy': 'Snow',
        'Snow, Rain': 'Snow',
        'Snow, Partially cloudy': 'Snow',
        'Snow, Rain, Overcast': 'Snow',
        'Rain, Overcast': 'Rain',
        'Snow, Overcast': 'Snow',
        'Overcast': 'Cloudy',
        'Snow': 'Snow',
        'Snow, Rain, Freezing Drizzle/Freezing Rain, Partially cloudy': 'Snow',
        'Snow, Rain, Freezing Drizzle/Freezing Rain': 'Snow'
    }

    # Create a new column with the mapped labels
    data['conditions'] = data['conditions'].map(label_mapping)

    columns_to_drop = [
        'datetimeEpoch', 'sunriseEpoch', 'sunsetEpoch',  # Epoch timestamps
        'sunrise', 'sunset',  # Keeping 'datetime' for time information
        'precipcover', 'preciptype', 'snowdepth',  # Additional weather details
        'windgust', 'pressure', 'cloudcover', 'visibility', 'solarradiation', 'solarenergy', 'uvindex',  # Weather parameters
        'icon', 'stations', 'source', 'tzoffset', 'severerisk','precipprob'# Non-contributing or redundant columns
    ]

    # Drop the specified columns
    data = data.drop(columns=columns_to_drop)

    data.drop_duplicates(inplace=True)

    data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce')
    # Extracting the relevant components
    data['month'] = data['datetime'].dt.month
    data['day'] = data['datetime'].dt.day
    data['year'] = data['datetime'].dt.year
    # Dropping the original column 
    #data = data.drop(columns=['datetime'], axis=1)
    csv_data = data.to_csv(index=False).encode('utf-8')

    gcs_conn_id = 'google_cloud_default'
    bucket_name = 'dcscweather'
    object_path = 'data.csv'

    # Upload data to Google Cloud Storage
    gcs_hook = GoogleCloudStorageHook(gcs_conn_id=gcs_conn_id)
    #gcs_hook.upload(bucket_name=bucket_name, object_name=object_path, filename=local_file_path, mime_type='text/csv')
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(csv_data, content_type='text/csv')

default_args = {
    'owner': 'Sid',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='upload',
    default_args=default_args,
    description='Your ETL DAG',
    schedule_interval='@daily', 
)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=etl_data,
    dag=dag,
)
