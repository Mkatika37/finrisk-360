import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import boto3

def on_failure_callback(context):
    logging.error(f"Task {context.get('task_instance').task_id} failed!")

default_args = {
    'owner': 'manohar',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

def trigger_and_wait_for_glue_job(job_name, region_name='us-east-1'):
    # Initialize AWS Glue Client
    client = boto3.client('glue', region_name=region_name)
    
    # Start the AWS Glue Job
    logging.info(f"Starting Glue Job: {job_name}")
    response = client.start_job_run(JobName=job_name)
    run_id = response['JobRunId']
    logging.info(f"Successfully triggered. Job Run ID: {run_id}")
    
    # Poll for completion status
    while True:
        status_response = client.get_job_run(JobName=job_name, RunId=run_id)
        state = status_response['JobRun']['JobRunState']
        
        logging.info(f"Job {job_name} (Run ID: {run_id}) is currently in state: {state}")
        
        if state == 'SUCCEEDED':
            logging.info(f"Glue Job {job_name} completed successfully!")
            break
        elif state in ['FAILED', 'TIMEOUT', 'STOPPED']:
            error_msg = status_response['JobRun'].get('ErrorMessage', 'No explicit error message provided.')
            raise Exception(f"Glue Job {job_name} terminated with status {state}. Error: {error_msg}")
        
        time.sleep(30)

with DAG(
    dag_id='finrisk360_nightly_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['finrisk360', 'production']
) as dag:

    # 1. Start pipeline
    start = DummyOperator(
        task_id='start'
    )

    # 2. Run FRED producer
    run_fred_producer = BashOperator(
        task_id='run_fred_producer',
        bash_command='cd /opt/airflow && python producers/fred_producer.py'
    )

    # 3. Run Alpha Vantage (Census) producer in parallel
    run_alpha_vantage_producer = BashOperator(
        task_id='run_alpha_vantage_producer',
        bash_command='cd /opt/airflow && python producers/census_producer.py'
    )

    # 4. Run Kinesis Bridge
    run_kinesis_bridge = BashOperator(
        task_id='run_kinesis_bridge',
        bash_command='cd /opt/airflow && timeout 300 python kinesis_bridge.py'
    )

    # 5. Trigger Glue Job 1 (Raw -> Silver)
    trigger_glue_job1 = PythonOperator(
        task_id='trigger_glue_job1',
        python_callable=trigger_and_wait_for_glue_job,
        op_kwargs={'job_name': 'finrisk360-raw-to-silver'}
    )

    # 6. Trigger Glue Job 2 (Silver -> Gold)
    trigger_glue_job2 = PythonOperator(
        task_id='trigger_glue_job2',
        python_callable=trigger_and_wait_for_glue_job,
        op_kwargs={'job_name': 'finrisk360-silver-to-gold'}
    )

    # 7. Run dbt transformation models and tests
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/finrisk_dbt && dbt run && dbt test'
    )

    # 8. End pipeline
    end = DummyOperator(
        task_id='end'
    )

    # Setup execution topology graph
    start >> [run_fred_producer, run_alpha_vantage_producer] >> run_kinesis_bridge >> trigger_glue_job1 >> trigger_glue_job2 >> run_dbt >> end
