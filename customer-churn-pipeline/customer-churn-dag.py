from airflow import DAG
from airflow.operators.python import PythonOperator
# pip install apache-airflow-providers-amazon
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime, timedelta
import time
from datetime import datetime

job_name = 'S3-to-redshift'

def glue_job_s3_redshift_transfer(job_name, **kwargs):
    # from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
    session = AwsGenericHook(aws_conn_id ='aws_s3_conn')

    boto3_session = session.get_session(region_name='us-east-1')
    client = boto3_session.client('glue')
    client.start_job_run(JobName=job_name)

def get_run_id():
    time.sleep(8)
    session = AwsGenericHook(aws_conn_id='aws_s3_conn')
    boto3_session = session.get_session(region_name='us-east-1')
    client = boto3_session.client('glue', region_name='us-east-1')
    respone = client.get_job_runs(JobName=job_name)
    job_run_id = respone['JobRuns'][0]['Id']
    return job_run_id



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

with DAG('customer-churn-dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup=False) as dag:

    glue_jobb_trigger = PythonOperator(
        task_id="tsk_glue_jobb_trigger",
        python_callable=glue_job_s3_redshift_transfer,
        op_kwargs={
            'job_name':job_name
        }
    )

    grab_glue_job_run_id = PythonOperator(
        task_id="tsk_grab_glue_job_run_id",
        python_callable=get_run_id,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    # from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
    is_glue_job_finish_running = GlueJobSensor(
        task_id='tsk_is_glue_job_finish_running',
        job_name=job_name,
        run_id='{{task_instance.xcom_pull("tsk_grab_glue_job_run_id")}}',
        verbose=True,
        aws_conn_id='aws_s3_conn',
        poke_interval=60,
        timeout=3000
        )

    glue_jobb_trigger >> grab_glue_job_run_id >> is_glue_job_finish_running