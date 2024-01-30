from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# pip install apache-airflow-providers-amazon
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
import json, requests
from datetime import datetime

current_path = '/home/ubuntu/'
s3_bucket = 'transformed-zillow-data-pipeline'

with open(current_path + 'airflow/files/config_zillow_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']

    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    output_file_path = current_path + f"response_data_{dt_string}.json"
    file_str = current_path +  f'response_data_{dt_string}.csv'

    with open(output_file_path, file_str) as output_file:
        json.dump(response_data, output_file, indent= 4)
    output_list = [output_file_path, file_str]
    return output_list

    

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

with DAG('zillow_analytics_dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup=False) as dag:
    
    extract_zillow_data_var = PythonOperator(
        task_id="tsk_extract_zillow_data_var",
        python_callable=extract_zillow_data,
        op_kwargs= {
            'url':'https://zillow69.p.rapidapi.com/search',
            'querystring': {'location': 'Houston, TX'},
            'headers' : api_host_key,
            'date_string':dt_now_string
        }
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    load_to_s3 = BashOperator(
        task_id="load_to_s3",
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://zillow-data-pipeline/',
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )

    # pip install apache-airflow-providers-amazon

    is_file_in_s3_available = S3KeySensor(
        task_id = "tsk_is_file_in_s3_available",
        bucket_key = '{{ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',
        bucket_name=s3_bucket,
        wildcard_match=False,
        check_fn=None,
        aws_conn_id='aws_s3_conn',
        verify=None,
        timeout = 120,
        poke_interval=5
    )

    # from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id='tsk_transfer_s3_to_redshift',
        schema ="PUBLIC",
        table="zillowdata",
        s3_bucket=s3_bucket,
        s3_key='{{ti.xcom_pul("tsk_extract_zillow_data_var")[1]}}',
        redshift_conn_id='conn_id_redshift',
        aws_conn_id='aws_s3_conn',
        verify=None,
        column_list=None,
        autocommit=False,
        method='APPEND',
        upsert_keys=None,
        copy_options=["csv IGNOREHEADER 1"]
        )

    extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift