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
            "python /opt/airflow/dags/../producers/fred_producer.py && "
            "python /opt/airflow/dags/../producers/alpha_vantage_producer.py && "
            "python /opt/airflow/dags/../producers/census_producer.py"
        ),
    )

    ingest = BashOperator(
        task_id="ingest",
        bash_command="python /opt/airflow/dags/../ingestion/kafka_consumer.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dags/../dbt && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dags/../dbt && dbt test --profiles-dir .",
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

    produce >> ingest >> dbt_run >> dbt_test >> quality >> alert
