"""
finrisk_dag.py — Main Airflow DAG for the finrisk-360 pipeline.

Schedule: Daily at 06:00 UTC
Tasks:
  1. produce  → Run all producers (FRED, Alpha Vantage, Census)
  2. ingest   → Consume Kafka messages and load to Snowflake
  3. dbt_run  → Execute dbt models
  4. dbt_test → Run dbt tests
  5. quality  → Run Great Expectations validations
  6. alert    → Send Slack / SNS notifications on failure
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "finrisk-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finrisk_360_daily",
    default_args=default_args,
    description="Daily pipeline: produce → ingest → transform → quality → alert",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finrisk", "production"],
) as dag:

    produce = BashOperator(
        task_id="produce",
        bash_command=(
            "python /opt/airflow/producers/fred_producer.py && "
            "python /opt/airflow/producers/alpha_vantage_producer.py && "
            "python /opt/airflow/producers/census_producer.py"
        ),
    )

    ingest = BashOperator(
        task_id="ingest",
        bash_command="timeout 300 python /opt/airflow/kinesis_bridge.py",
    )

    def trigger_glue_job(job_name: str):
        import boto3
        import time
        import os
        from botocore.exceptions import ClientError
        client = boto3.client("glue", region_name=os.getenv("AWS_REGION", "us-east-1"))
        run_id = None
        try:
            response = client.start_job_run(JobName=job_name)
            run_id = response['JobRunId']
            print(f"Started Glue Job: {job_name} | Run ID: {run_id}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConcurrentRunsExceededException':
                print(f"Glue Job {job_name} is already running. Attempting to attach to active run...")
                runs = client.get_job_runs(JobName=job_name)['JobRuns']
                active_runs = [r for r in runs if r['JobRunState'] in ['STARTING', 'RUNNING']]
                if active_runs:
                    run_id = active_runs[0]['Id']
                else:
                    raise Exception("Concurrent run exceeded, but no active runs found. Maybe it's finishing?")
            else:
                raise e
        
        while True:
            status = client.get_job_run(JobName=job_name, RunId=run_id)['JobRun']['JobRunState']
            print(f"Status: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'TIMEOUT', 'STOPPED']:
                if status != 'SUCCEEDED':
                    raise Exception(f"Glue job {job_name} failed with state {status}")
                break
            time.sleep(30)

    from airflow.operators.python import PythonOperator

    glue_job1 = PythonOperator(
        task_id="trigger_glue_job1",
        python_callable=trigger_glue_job,
        op_kwargs={"job_name": "finrisk360-raw-to-silver"},
    )

    glue_job2 = PythonOperator(
        task_id="trigger_glue_job2",
        python_callable=trigger_glue_job,
        op_kwargs={"job_name": "finrisk360-silver-to-gold"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="set -a && source /opt/airflow/.env && set +a && cd /opt/airflow/finrisk_dbt && dbt run --profiles-dir . && dbt test --profiles-dir .",
    )

    quality = BashOperator(
        task_id="data_quality",
        bash_command="echo 'TODO: run Great Expectations checkpoint'",
    )

    alert = BashOperator(
        task_id="alert_on_failure",
        bash_command="echo 'TODO: send Slack/SNS alert'",
        trigger_rule="one_failed",
    )

    produce >> ingest >> glue_job1 >> glue_job2 >> dbt_run >> quality >> alert
