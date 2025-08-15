from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
import pandas as pd
import s3fs

def catch_load_all_data(task_instance):

    cities = ["Pune", "Delhi", "Bengaluru", "Kolkata"]
    all_loaded_data = []

    for city in cities:
        task_id = f"extract_weather_data_{city.lower()}"
        data = task_instance.xcom_pull(task_ids=task_id)

        if not data:
            print(f"No data found for {city}")
            continue
          
   
        # Processing same as before...
        timezone_offset = data["timezone"]
        time_diff = timedelta(seconds=timezone_offset)
        tz = timezone(time_diff)

        dt_time = datetime.fromtimestamp(data["dt"], tz=tz).isoformat()
        sunrise_time = datetime.fromtimestamp(data["sys"]["sunrise"], tz=tz).isoformat()
        sunset_time = datetime.fromtimestamp(data["sys"]["sunset"], tz=tz).isoformat()

        loaded_data = {"City": data["name"],
                        "Country": data["sys"]["country"],  
                        "Weather_main": data["weather"][0]['main'],
                        "Weather_subtype": data["weather"][0]['description'],
                        "Temperature": data["main"]["temp"],  # Unit Default: Kelvin
                        "Feels_Like": data["main"]["feels_like"],
                        "Min_Temp":data["main"]["temp_min"],
                        "Max_Temp": data["main"]["temp_max"],
                        "Pressure": data["main"]["pressure"],     # Atmospheric pressure on the sea level, hPa
                        "Humidity": data["main"]["humidity"],     # Humidity, %
                        "Visibility": data.get("visibility", None),
                        "Wind_speed": data["wind"]["speed"],       #Unit Default: meter/sec
                        "cloudiness_percent": data["clouds"]["all"],    # Cloudiness, %
                        "Rain_mm_hour": data.get("rain", {}).get("1h", 0.0),
                        "Snow_mm_hour": data.get("snow", {}).get("1h", None),
                        "Time": data["dt"],  #dt Time of data calculation, unix, UTC
                        "Timezone_offset": data["timezone"],
                        "Time_Recorded_local": dt_time, # default was unix (in seconds)
                        "Sunrise_local": sunrise_time,
                        "Sunset_local": sunset_time      
                        }
        # loaded_data_list = [loaded_data]
        all_loaded_data.append(loaded_data)

    if not all_loaded_data:
        print("No data to load.")
        return

    # Create DataFrame with all cities
    df_data = pd.DataFrame(all_loaded_data)

    # India Standard Time is UTC+05:30
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)

    # filename = f'{data["name"].lower()}_weather-{now.strftime("%A-%d-%b-%Y_%H-%M-%S")}'
    filename = f'{len(cities)}-cities-weather-{now.strftime("%A-%d-%b-%Y_%H-%M-%S")}'

    fs = s3fs.S3FileSystem(
        key='AKIAUYXOGVO7D65JFF4A',
        secret='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'
    )

  # Upload using s3fs
    bucket_name = 'openweather-etl-extracted-data'  #S3 bucket name
    file_path = f's3://{bucket_name}/{filename}.csv'
    try:
        with fs.open(file_path, 'w') as f:
            df_data.to_csv(f, index=False)
        print(f"Successfully uploaded to {file_path}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 15),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

cities = ["Pune", "Delhi", "Bengaluru", "Kolkata"]
api_key = "31ca38fc7637871abc371a1cee31ec04"


with DAG(
    'weather_dag_new',
    default_args=default_args,
    schedule=timedelta(minutes=2),  #='@once', # earlier version schedule_interval, "@daily","@weekly","@once", =None(i.e Manual triggering only),=timedelta(hours=2),="30 9 * * 1" (cron expression,Every Monday at 9:30 AM)
    catchup=False
) as dag:

    city_chains = []
    for city in cities:
        sensor = HttpSensor(
            task_id=f'is_weather_api_ready_{city.lower()}',
            http_conn_id='weathermap_api',
            endpoint=f'/data/2.5/weather?q={city},in&appid={api_key}', #https://api.openweathermap.org/data/2.5/weather?q={city name},{country code}&appid={API key}
            timeout=30,         # Max 30 seconds for the task
            poke_interval=10,   # Check every 10 seconds (default is 60!) Time to wait between two checks of the HTTP endpoint not using it.
            mode='poke'         # Default mode
        )

        extractor = HttpOperator(
            task_id=f'extract_weather_data_{city.lower()}',
            http_conn_id='weathermap_api',
            endpoint=f'/data/2.5/weather?q={city},in&appid={api_key}',
            method='GET',
            response_filter=lambda r: json.loads(r.text),
            log_response=True
        )

        # Each chain is sensor -> extractor
        sensor >> extractor
        city_chains.append(sensor >> extractor)

    final_task = PythonOperator(
        task_id='load_all_weather_data',
        python_callable=catch_load_all_data
    )

    # All city chains converge into final task
    city_chains >> final_task        #  [A>>B, C>>D, E>>F] >> G



#  https://api.openweathermap.org/data/2.5/weather?q=Pune,ind&appid=31ca38fc7637871abc371a1cee31ec04 
#  weathermap_api = https://api.openweathermap.org (this put in connectors as Http in Airflow)
