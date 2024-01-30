from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
import pandas as pd

def kelvinToCelsius(kelvin):
    return kelvin - 273.15

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group.tsk_extract_houston_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvinToCelsius(data["main"]["temp"])
    feels_like_celsius = kelvinToCelsius(data["main"]["feels_like"])
    min_temp_celsius = kelvinToCelsius(data["main"]["temp_min"])
    max_temp_celsius = kelvinToCelsius(data["main"]["temp_max"])
    pressure = data ["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "city": city,
        "description": weather_description,
        "feels_like_celsius": feels_like_celsius,
        "temperature_celsius": temp_celsius,
        "minimum_temp_celsius": min_temp_celsius,
        "maximum_temp_celsius": max_temp_celsius,
        "pressure": pressure,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "time_of_record": time_of_record,
        "sunrise_local_time": sunrise_time,
        "sunset_local_time": sunset_time
    }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    df_data.to_csv("current_weather_data.csv", index=False, header=False)

def load_weather():
    hook = PostgresHook(postgres_conn_id = 'postgres_conn')
    hook.copy_expert(
        sql="copy weather_data from stdin with delimiter as ','",
        filename = 'current_weather_data.csv'
    )

def save_joined_data_s3(task_instance):
    data = task_instance.xcom_pull(task_ids="tsk_join_data")
    df = pd.DataFrame(data, columns = ['city', 'description', 'temperature_celsius', 'feels_like_celsius', 'minimum_temp_celsius', 'maximum_temp_celsius', 'pressure', 'humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time', 'state', 'census_2020', 'estimate_2022', 'land_area_km2_2020'])
    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    dt_string = 'joined_weather_data_' + dt_string 
    df.to_csv("joined_weather_data.csv", index=False)
    


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG('weather_dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup=False) as dag:
    
    start_pipeline = DummyOperator(
        task_id = "tsk_start_pipeline"
    )

    join_data = PostgresOperator(
        task_id='tsk_join_data',
        postgres_conn_id = "postgres_conn",
        sql = """
            select w.city, description, temperature_celsius, 
            feels_like_celsius, minimum_temp_celsius, 
            maximum_temp_celsius, pressure, humidity,
            wind_speed, time_of_record, sunrise_local_time,
            sunset_local_time, state, census_2020, 
            estimate_2022, land_area_km2_2020
            from weather_data w
            inner join city_look_up c
            on w.city = c.city;
        """
    )

    load_joined_data = PythonOperator(
        task_id = 'tsk_load_joined_data',
        python_callable = save_joined_data_s3
    )

    end_pipeline = DummyOperator(
        task_id = "task_end_pipeline"
    )

    with TaskGroup(group_id = "group", tooltip="Extract_from_S3_and_weatherapi") as group:
        create_table_1 = PostgresOperator(
            task_id = "tsk_create_table_1",
            postgres_conn_id = "postgres_conn",
            sql="""
                CREATE TABLE IF NOT EXISTS city_look_up(
                    city text not null,
                    state text not null,
                    estimate_2022 numeric not null,
                    census_2020 numeric not null,
                    land_area_km2_2020 numeric not null
                );
            """
        )

        truncate_table = PostgresOperator(
            task_id = "tsk_truncate_table",
            postgres_conn_id = "postgres_conn",
            sql = "truncate table city_look_up;"
        )

        uploadS3_to_postgres = PostgresOperator(
            task_id = "tsk_uploadS3_to_postgres",
            postgres_conn_id = "postgres_conn",
            sql = """select aws_s3.table_import_from_s3(
                'city_look_up', 
                'city,state,estimate_2022,census_2020,land_area_km2_2020',
                '(format csv, DELIMITER '','', HEADER true)',  
                aws_commons.create_s3_uri('us-weather-bucket', 'us_city.csv', 'us-east-1'));
            """
        )

        create_table_2 = PostgresOperator(
            task_id = 'tsk_create_table_2',
            postgres_conn_id = "postgres_conn",
            sql = """
                create table if not exists weather_data(
                    city text,
                    description text,
                    temperature_celsius numeric,
                    feels_like_celsius numeric,
                    minimum_temp_celsius numeric,
                    maximum_temp_celsius numeric,
                    pressure numeric,
                    humidity numeric,
                    wind_speed numeric,
                    time_of_record timestamp,
                    sunrise_local_time timestamp,
                    sunset_local_time timestamp
                );
            """
        )

        is_houston_weather_api_ready = HttpSensor(
            task_id = 'tsk_is_houston_weather_api_ready',
            http_conn_id='weathermap_api',
            endpoint='/data/2.5/weather?q=houston&appid=3c6033dd8b0c3daf284f8db2588d1118'
        )

        extract_houston_weather_data = SimpleHttpOperator(
            task_id = 'tsk_extract_houston_weather_data',
            http_conn_id = 'weathermap_api',
            endpoint='/data/2.5/weather?q=houston&appid=3c6033dd8b0c3daf284f8db2588d1118',
            method = 'GET',
            response_filter = lambda r: json.loads(r.text),
            log_response = True
        )

        transform_load_houston_weather_data = PythonOperator(
            task_id = 'transform_load_houston_weather_data',
            python_callable = transform_load_data
        )

        load_weather_data = PythonOperator(
            task_id = 'tsk_load_weather_data',
            python_callable = load_weather
        )

        create_table_1 >> truncate_table >> uploadS3_to_postgres >> load_weather_data
        create_table_2 >> is_houston_weather_api_ready >> extract_houston_weather_data >> transform_load_houston_weather_data

    start_pipeline >> group >> join_data >> load_joined_data >> end_pipeline

 