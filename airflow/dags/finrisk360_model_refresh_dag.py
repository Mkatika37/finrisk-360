import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'manohar',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 4, 1)
}

SNS_TOPIC = 'arn:aws:sns:us-east-1:322960458535:finrisk360-alerts'

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def fetch_latest_rates(**context):
    # Simulated fetch of FRED mortgage rates
    # In reality, this would hit FRED API via requests
    average_rate = 6.85 
    context['ti'].xcom_push(key='avg_mortgage_rate', value=average_rate)

def calculate_macro_stress(**context):
    avg_rate = context['ti'].xcom_pull(task_ids='fetch_latest_rates', key='avg_mortgage_rate')
    # Simulated macro stress calc 
    fed_funds = 5.25
    unemployment = 3.9
    gdp_growth = 2.1
    
    # Simple simulated formula
    macro_stress_score = (fed_funds * 0.1) + (unemployment * 0.05) - (gdp_growth * 0.05)
    macro_stress_score = max(0.0, min(1.0, macro_stress_score))
    
    context['ti'].xcom_push(key='new_macro_stress_score', value=macro_stress_score)

def update_risk_scores(**context):
    new_macro = context['ti'].xcom_pull(task_ids='calculate_macro_stress', key='new_macro_stress_score')
    conn = get_snowflake_conn()
    cur = conn.cursor()
    
    query = f"""
    UPDATE LOAN_RISK_SCORES 
    SET macro_stress_score = {new_macro},
        risk_score = (ltv_score * 0.30) + (dti_score * 0.25) + (rate_spread_score * 0.25) + ({new_macro} * 0.20)
    """
    cur.execute(query)
    
    tier_query = """
    UPDATE LOAN_RISK_SCORES
    SET risk_tier = CASE 
        WHEN risk_score < 0.3 THEN 'LOW'
        WHEN risk_score < 0.6 THEN 'MEDIUM'
        WHEN risk_score < 0.85 THEN 'HIGH'
        ELSE 'CRITICAL'
    END
    """
    cur.execute(tier_query)
    updated_count = cur.rowcount
    context['ti'].xcom_push(key='updated_count', value=updated_count)
    conn.close()

def validate_refresh(**context):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES")
    total = cur.fetchone()[0]
    conn.close()
    context['ti'].xcom_push(key='total_loans', value=total)

def notify_refresh_complete(**context):
    ti = context['ti']
    score = ti.xcom_pull(task_ids='calculate_macro_stress', key='new_macro_stress_score')
    count = ti.xcom_pull(task_ids='validate_refresh', key='total_loans')
    if score is not None and count is not None:
        sns = boto3.client('sns', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        message = f"FinRisk360: Weekly model refresh complete. New macro_stress_score: {score:.3f}. Risk distribution updated for {count} loans."
        try:
            sns.publish(TopicArn=SNS_TOPIC, Message=message, Subject='FinRisk360 Model Refresh')
            print(f"Sent SNS: {message}")
        except Exception as e:
            print(f"Failed SNS publish: {e}")

with DAG(
    dag_id="finrisk360_model_refresh",
    default_args=default_args,
    description="Weekly full model refresh with updated macro data",
    schedule_interval="0 0 * * 0",
    catchup=False,
    tags=["finrisk360", "production"]
) as dag:

    t1 = PythonOperator(task_id="fetch_latest_rates", python_callable=fetch_latest_rates)
    t2 = PythonOperator(task_id="calculate_macro_stress", python_callable=calculate_macro_stress)
    t3 = PythonOperator(task_id="update_risk_scores", python_callable=update_risk_scores)
    
    t4 = BashOperator(
        task_id="trigger_dbt_refresh",
        bash_command="set -a && source /opt/airflow/.env && set +a && cd /opt/airflow/finrisk_dbt && dbt run --select mart_risk_summary+ --profiles-dir . && dbt test --profiles-dir ."
    )
    
    t5 = PythonOperator(task_id="validate_refresh", python_callable=validate_refresh)
    t6 = PythonOperator(task_id="notify_refresh_complete", python_callable=notify_refresh_complete)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
