from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
import pandas as pd
import s3fs



# Note 
# I extracted the current data for Pune, the JSON formatted retured data is like:
'''
{
    'coord': {'lon': 73.8553, 'lat': 18.5196},
    'weather': [{'id': 804, 'main': 'Clouds', 'description': 'overcast clouds', 'icon': '04n'}],
    'base': 'stations',
    'main': {'temp': 298.38, 'feels_like': 299.23, 'temp_min': 298.38,
    'temp_max': 298.38,
    'pressure': 1010,
    'humidity': 87,
    'sea_level': 1010,
    'grnd_level': 937},
    'visibility': 10000,
    'wind': {'speed': 2.82, 'deg': 252, 'gust': 4.26},
    'clouds': {'all': 100},
    'dt': 1754498140,
    'sys': {'country': 'IN', 'sunrise': 1754441024, 'sunset': 1754487435},
    'timezone': 19800,
    'id': 1259229,
    'name': 'Pune',
    'cod': 200
}

'''
###

# def kelvin_to_fahrenheit(temp_in_kelvin):
#     temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
#     return temp_in_fahrenheit

# Function to convert unix timestamp + timezone to localized datetime
def convert_time(unix_ts, timezone_offset_sec):
    time_diff=timedelta(seconds=timezone_offset_sec)
    tz = timezone(time_diff)
    return datetime.fromtimestamp(unix_ts, tz=tz).isoformat()  # in iso format ex 2025-08-08T13:59:18+02:00  , 	T :Separator between date and time ,Timezone offset from UTC 

def catch_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

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
                        # "temp_min_celsius": round(data["main"]["temp_min"] - 273.15, 2),
                        # "temp_max_celsius": round(data["main"]["temp_max"] - 273.15, 2),
                        "Pressure": data["main"]["pressure"],
                        "Humidity": data["main"]["humidity"],
                        "Visibility": data.get("visibility", None),
                        "Wind_speed": data["wind"]["speed"],  #Unit Default: meter/sec
                        "cloudiness_percent": data["clouds"]["all"],
                        "Rain_mm_hour": data.get("rain", {}).get("1h", 0.0),
                        "Snow_mm_hour": data.get("snow", {}).get("1h", None),
                        "Time": data["dt"],  #dt Time of data calculation, unix, UTC
                        "Timezone_offset": data["timezone"],
                        "Time_Recorded_local": dt_time, # default was unix (in seconds)
                        "Sunrise_local": sunrise_time,
                        "Sunset_local": sunset_time      
                        }
    loaded_data_list = [loaded_data]
    df_data = pd.DataFrame(loaded_data_list)
    # aws_credentials = {"key": "xxxxxxxxx", "secret": "xxxxxxxxxx", "token": "xxxxxxxxxxxxxx"}


    now = datetime.now()
    dt_string = now.strftime("%A-%d-%b-%Y_%H-%M-%S")  # to just give proper naming like--> Tuesday-06-Aug-2025_14-30-12, alterantive .strftime("%Y%m%d_%H%M%S")
    filename = 'pune_weather-' + dt_string
    # df_data.to_csv(f"{filename}.csv", index=False) # this way saved in EC2
    # df_data.to_csv(f"s3://openweather-etl-extracted-data/{filename}.csv", index=False)

    #############
    fs = s3fs.S3FileSystem(
        key='<key>',
        secret='<secret-key>'
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
    ##################


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


with DAG('weather_dag',
        default_args=default_args,
        schedule = timedelta(seconds=120),  #='@once', # earlier version schedule_interval, "@daily","@weekly","@once", =None(i.e Manual triggering only),=timedelta(hours=2),="30 9 * * 1" (cron expression,Every Monday at 9:30 AM)
        catchup=False) as dag:


        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Pune&appid=31ca38fc7637871abc371a1cee31ec04',
        timeout=30,        # Max 30 seconds for the task
        poke_interval=10,  # Check every 10 seconds (default is 60!) Time to wait between two checks of the HTTP endpoint not using it.
        mode='poke'        # Default mode
        )


        extract_weather_data = HttpOperator(  # Changed from SimpleHttpOperato
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Pune&appid=31ca38fc7637871abc371a1cee31ec04',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        load_weather_data = PythonOperator(
        task_id= 'load_weather_data',
        python_callable=catch_load_data
        )


        is_weather_api_ready >> extract_weather_data >> load_weather_data




#  https://api.openweathermap.org/data/2.5/weather?q=Pune,ind&appid=31ca38fc7637871abc371a1cee31ec04 
#  weathermap_api = https://api.openweathermap.org (this put in connectors as Http in Airflow)


