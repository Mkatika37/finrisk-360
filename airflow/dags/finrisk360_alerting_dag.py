import os
from datetime import datetime, timedelta
import snowflake.connector
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'manohar',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 4, 1)
}

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def check_critical_loans(**context):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES WHERE risk_tier = 'CRITICAL' AND processing_date = CURRENT_DATE()")
    count = cur.fetchone()[0]
    conn.close()
    context['ti'].xcom_push(key='critical_count', value=count)
    return count

def compare_with_previous_hour(**context):
    current_count = context['ti'].xcom_pull(task_ids='check_critical_loans', key='critical_count')
    # Simulated previous count logic for alerting purposes
    previous_count = current_count - 5 if current_count > 5 else 0
    
    alert = current_count > previous_count
    context['ti'].xcom_push(key='send_alert', value=alert)
    context['ti'].xcom_push(key='new_critical_count', value=(current_count - previous_count))
    return alert

def send_sns_alert(**context):
    send_alert = context['ti'].xcom_pull(task_ids='compare_with_previous_hour', key='send_alert')
    if send_alert:
        new_count = context['ti'].xcom_pull(task_ids='compare_with_previous_hour', key='new_critical_count')
        sns = boto3.client('sns', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        message = f"FinRisk360 Alert: {new_count} new CRITICAL risk loans detected at {datetime.now()}"
        topic_arn = 'arn:aws:sns:us-east-1:322960458535:finrisk360-alerts'
        try:
            sns.publish(TopicArn=topic_arn, Message=message, Subject='FinRisk360 CRITICAL Alert')
            print(f"Alert sent: {message}")
        except Exception as e:
            print(f"Failed to send SNS alert: {e}")

def log_to_cloudwatch(**context):
    current_count = context['ti'].xcom_pull(task_ids='check_critical_loans', key='critical_count')
    cw = boto3.client('cloudwatch', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    try:
        cw.put_metric_data(
            Namespace='FinRisk360/Monitoring',
            MetricData=[
                {
                    'MetricName': 'critical_count',
                    'Value': float(current_count),
                    'Unit': 'Count'
                }
            ]
        )
        print("Logged critical count to CloudWatch")
    except Exception as e:
        print(f"Failed to log metric: {e}")

with DAG(
    dag_id="finrisk360_alerting",
    default_args=default_args,
    description="Hourly critical loan detection and alerting",
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["finrisk360", "production"]
) as dag:

    t1 = PythonOperator(
        task_id="check_critical_loans",
        python_callable=check_critical_loans
    )

    t2 = PythonOperator(
        task_id="compare_with_previous_hour",
        python_callable=compare_with_previous_hour
    )

    t3 = PythonOperator(
        task_id="send_sns_alert",
        python_callable=send_sns_alert
    )

    t4 = PythonOperator(
        task_id="log_to_cloudwatch",
        python_callable=log_to_cloudwatch
    )

    t1 >> t2 >> t3 >> t4
