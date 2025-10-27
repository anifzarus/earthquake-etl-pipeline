from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime,  timedelta, date
import requests
import pandas as pd
from google.cloud import bigquery

def extract_earthquakedata(**context):

    today = date.today()
    yesterday = today - timedelta(days= 1)
    response = requests.get(f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={yesterday}&endtime={today}")

    d = {}

    todays_count = response.json()['metadata']['count']

    for count in range(todays_count):
        
        
        magnitude = response.json()['features'][count]['properties']['mag']
        place = response.json()['features'][count]['properties']['place']
        disaster_time = response.json()['features'][count]['properties']['time']
        tsunami = response.json()['features'][count]['properties']['tsunami']
        disaster_type = response.json()['features'][count]['properties']['type']
        title = response.json()['features'][count]['properties']['title']
        long = response.json()['features'][count]['geometry']['coordinates'][0]
        lat = response.json()['features'][count]['geometry']['coordinates'][1]
        depth = response.json()['features'][count]['geometry']['coordinates'][2]
        geometry_type = response.json()['features'][count]['geometry']['type']
        id = response.json()['features'][count]['id']

        if 'Event_ID' not in d:
            d['Event_ID'] = [id]
        else:
            d['Event_ID'].append(id)


        if 'Title' not in d:
            d['Title'] = [title]
        else:
            d['Title'].append(title)

        if 'Place' not in d:
            a = place.split()[-1]
            d['Place'] = [a]
        else:
            a = place.split()[-1]
            d['Place'].append(a)

        if 'Magnitude' not in d:
            d['Magnitude'] = [abs(magnitude)]
        else:
            d['Magnitude'].append(abs(magnitude))

            
        timestamp_s = disaster_time / 1000
        dt = datetime.fromtimestamp(timestamp_s)
        
        if 'Time' not in d:
            d['Time'] = [dt.strftime("%Y-%m-%d %H:%M:%S")]
        else:
            d['Time'].append(dt.strftime("%Y-%m-%d %H:%M:%S"))
    
        if 'Tsunami' not in d:
            d['Tsunami'] = [tsunami]
        else:
            d['Tsunami'].append(tsunami)
    
        if 'Type' not in d:
            d['Type'] = [disaster_type]
        else:
            d['Type'].append(disaster_type)

        if 'Longitude' not in d:
            d['Longitude'] = [long]
        else:
            d['Longitude'].append(long)

        if 'Latitude' not in d:
            d['Latitude'] = [lat]
        else:
            d['Latitude'].append(lat)

        if 'Depth' not in d:
            d['Depth'] = [abs(depth)]
        else:
            d['Depth'].append(abs(depth))

        if 'Geometry_Type' not in d:
            d['Geometry_Type'] = [geometry_type]
        else:
            d['Geometry_Type'].append(geometry_type)

        
    filepath = f'/opt/airflow/dags/out{today}.csv'
    pd.DataFrame(d).to_csv(filepath, index=False)
    

    context['ti'].xcom_push(key='csv_path', value= filepath)

def loading_to_bigquery(**context):

    file_path = context['ti'].xcom_pull(key='csv_path', task_ids='extract_earthquake_data')

    key_path = "/opt/airflow/dags/earthquakeapi-20102025-72704aa9f14c.json"
    
    
    client = bigquery.Client.from_service_account_json(
        key_path,
        project="earthquakeapi-20102025"  # must match the dataset’s project
    )

    df = pd.read_csv(file_path)

    table_id = "earthquakeapi-20102025.earthquake2010.earthquakemonitoring"

    # Upload dataframe to BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    
    job.result()  # Wait for the job to finish
    #print("Data uploaded successfully!")

def delete_loadedfile(**context):

    file_path = context['ti'].xcom_pull(key='csv_path', task_ids='extract_earthquake_data')

    os.remove(file_path)


    

with DAG(
    dag_id="earthquake_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="extract_earthquake_data",
        python_callable= extract_earthquakedata
    )
    task2 = PythonOperator(
        task_id="loading_earthquake_data",
        python_callable= loading_to_bigquery
    )
    task3 = PythonOperator(
        task_id='delete_loadedfile',
        python_callable=delete_loadedfile,
    )


    task1 >> task2 >> task3