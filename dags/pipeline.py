from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import json

AVIATIONSTACK_API_KEY = "ab9bb4f1d4e23edad4d15b63c81934be"

def init_buckets():
    """Tâche 0 : Vérifier et créer les buckets S3 s'ils n'existent pas"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    for bucket in ['bronze', 'silver']:
        if not s3_hook.check_for_bucket(bucket_name=bucket):
            s3_hook.create_bucket(bucket_name=bucket)
            print(f"Bucket '{bucket}' créé avec succès dans LocalStack.")
        else:
            print(f"Bucket '{bucket}' existe déjà.")

def extract_airports_reference(**context):
    """Tâche 1 : Charger le référentiel des aéroports mondiaux en brut"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    url = "https://raw.githubusercontent.com/mwgg/Airports/master/airports.json"
    response = requests.get(url)
    
    file_key = "raw_reference/airports_db.json"
    s3_hook.load_string(
        string_data=response.text,
        key=file_key,
        bucket_name='bronze',
        replace=True
    )
    print(f"Référentiel aéroports sauvegardé dans {file_key}")
    context['ti'].xcom_push(key='airports_file', value=file_key)

def extract_aviationstack_flights(**context):
    """Tâche 2 : Charger les vols Aviationstack en brut"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    url = "https://api.aviationstack.com/v1/flights"
    params = {"access_key": AVIATIONSTACK_API_KEY, "dep_iata": "CDG", "limit": 50}
    
    response = requests.get(url, params=params)
    flights = response.json().get("data", [])
    
    execution_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_key = f"raw_flights/cdg_flights_{execution_time}.json"
    
    s3_hook.load_string(
        string_data=json.dumps(flights, indent=4),
        key=file_key,
        bucket_name='bronze',
        replace=True
    )
    print(f"{len(flights)} vols bruts sauvegardés dans {file_key}")
    context['ti'].xcom_push(key='flights_file', value=file_key)

def extract_openmeteo_weather(**context):
    """Tâche 3 : Lire les aéroports d'arrivée et charger leur météo en brut"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ti = context['ti']
    
    flights_file = ti.xcom_pull(task_ids='t2_extract_flights', key='flights_file')
    airports_file = ti.xcom_pull(task_ids='t1_extract_airports', key='airports_file')
    
    # Lecture des données brutes stockées dans S3
    flights = json.loads(s3_hook.read_key(key=flights_file, bucket_name='bronze'))
    airports_db = json.loads(s3_hook.read_key(key=airports_file, bucket_name='bronze'))
    
    # Isoler les aéroports d'arrivée uniques
    unique_icaos = {f.get("arrival", {}).get("icao") for f in flights if f.get("arrival", {}).get("icao")}
    unique_icaos.add("LFPG")
    
    weather_data = []
    
    for icao in unique_icaos:
        if icao in airports_db:
            lat = airports_db[icao]["lat"]
            lon = airports_db[icao]["lon"]
            
            url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
            res = requests.get(url)
            
            if res.status_code == 200:
                weather_data.append({
                    "icao_code": icao,
                    "fetch_time": datetime.now().isoformat(),
                    "weather_raw": res.json()
                })
                
    execution_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    weather_file_key = f"raw_weather/weather_{execution_time}.json"
    
    s3_hook.load_string(
        string_data=json.dumps(weather_data, indent=4),
        key=weather_file_key,
        bucket_name='bronze',
        replace=True
    )
    print(f"Météo brute de {len(weather_data)} aéroports sauvegardée dans {weather_file_key}")

with DAG('bronze_elt_pipeline', start_date=datetime(2023, 1, 1), schedule_interval='@hourly', catchup=False) as dag:
    
    t0_init = PythonOperator(
        task_id='init_buckets',
        python_callable=init_buckets
    )
    
    t1_bronze = PythonOperator(task_id='t1_extract_airports', python_callable=extract_airports_reference, provide_context=True)
    t2_bronze = PythonOperator(task_id='t2_extract_flights', python_callable=extract_aviationstack_flights, provide_context=True)
    t3_bronze = PythonOperator(task_id='t3_extract_weather', python_callable=extract_openmeteo_weather, provide_context=True)
    
    
    # L'orchestration des tâches:
    t0_init >> [t1_bronze, t2_bronze] >> t3_bronze