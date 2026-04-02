import os
import json
from datetime import datetime, timedelta
import boto3
import snowflake.connector
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

RAW_BUCKET = 'finrisk360-raw-322960458535'
SILVER_BUCKET = 'finrisk360-silver-322960458535'
GOLD_BUCKET = 'finrisk360-gold-322960458535'
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

def _get_s3_client():
    return boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))

def validate_s3_raw(**context):
    s3 = _get_s3_client()
    prefix = f"loans/year={datetime.now().strftime('%Y')}/month={datetime.now().strftime('%m')}/day={datetime.now().strftime('%d')}/"
    res = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=prefix)
    contents = res.get('Contents', [])
    if not contents:
        raise ValueError("No files found in raw bucket for today")
    total_size = sum(obj['Size'] for obj in contents)
    if total_size < 1024 * 1024:
        raise ValueError(f"Total size in raw bucket is less than 1MB: {total_size} bytes")
    context['ti'].xcom_push(key='raw_status', value='PASS')

def validate_s3_silver(**context):
    s3 = _get_s3_client()
    prefix = f"loans/"
    res = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    contents = res.get('Contents', [])
    parquet_files = [obj for obj in contents if obj['Key'].endswith('.parquet')]
    if not parquet_files:
        raise ValueError("No parquet files found in silver bucket")
    context['ti'].xcom_push(key='silver_status', value='PASS')

def validate_s3_gold(**context):
    s3 = _get_s3_client()
    prefix = f"risk_scores/"
    res = s3.list_objects_v2(Bucket=GOLD_BUCKET, Prefix=prefix)
    contents = res.get('Contents', [])
    if not contents:
        raise ValueError("No files found in gold bucket")
    # Deep parquet inspection would require awswrangler/pandas or pyarrow. Doing broad check.
    context['ti'].xcom_push(key='gold_status', value='PASS')

def validate_snowflake(**context):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    errors = []
    
    cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES")
    cnt = cur.fetchone()[0]
    if cnt <= 100000:
        errors.append(f"Row count {cnt} is not > 100000")
        
    cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES WHERE risk_score IS NULL")
    null_cnt = cur.fetchone()[0]
    if null_cnt > 0:
        errors.append(f"Found {null_cnt} records with NULL risk_score")
        
    cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES WHERE risk_tier NOT IN ('LOW','MEDIUM','HIGH','CRITICAL')")
    invalid_tier_cnt = cur.fetchone()[0]
    if invalid_tier_cnt > 0:
        errors.append(f"Found {invalid_tier_cnt} records with invalid risk_tier")
        
    cur.execute("SELECT MAX(processing_date) FROM LOAN_RISK_SCORES")
    max_date = cur.fetchone()[0]
    if max_date is None or max_date != datetime.now().date():
        errors.append(f"Max processing_date {max_date} != {datetime.now().date()}")
        
    conn.close()
    
    if errors:
        context['ti'].xcom_push(key='sf_status', value='FAIL: ' + ', '.join(errors))
        raise ValueError("Snowflake validation failed: " + "; ".join(errors))
    context['ti'].xcom_push(key='sf_status', value='PASS')

def generate_quality_report(**context):
    ti = context['ti']
    report = {
        'date': datetime.now().isoformat(),
        'raw_check': ti.xcom_pull(task_ids='validate_s3_raw', key='raw_status') or 'FAIL',
        'silver_check': ti.xcom_pull(task_ids='validate_s3_silver', key='silver_status') or 'FAIL',
        'gold_check': ti.xcom_pull(task_ids='validate_s3_gold', key='gold_status') or 'FAIL',
        'snowflake_check': ti.xcom_pull(task_ids='validate_snowflake', key='sf_status') or 'FAIL'
    }
    
    s3 = _get_s3_client()
    key = f"reports/quality_report_{datetime.now().strftime('%Y%m%d')}.json"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(report, indent=2)
    )
    ti.xcom_push(key='report_data', value=report)

def alert_on_quality_failure(**context):
    ti = context['ti']
    report = ti.xcom_pull(task_ids='generate_quality_report', key='report_data')
    if report:
        failed_checks = [k for k, v in report.items() if v and 'FAIL' in str(v)]
        if failed_checks:
            sns = boto3.client('sns', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
            message = f"FinRisk360 Data Quality Alert! Failed checks: {', '.join(failed_checks)}"
            try:
                sns.publish(TopicArn=SNS_TOPIC, Message=message, Subject='Data Quality Failure')
            except Exception as e:
                print(f"Failed to publish SNS: {e}")

with DAG(
    dag_id="finrisk360_data_quality",
    default_args=default_args,
    description="Data quality validation across all pipeline layers",
    schedule_interval="30 6 * * *",
    catchup=False,
    tags=["finrisk360", "production"]
) as dag:

    t1 = PythonOperator(task_id="validate_s3_raw", python_callable=validate_s3_raw)
    t2 = PythonOperator(task_id="validate_s3_silver", python_callable=validate_s3_silver)
    t3 = PythonOperator(task_id="validate_s3_gold", python_callable=validate_s3_gold)
    t4 = PythonOperator(task_id="validate_snowflake", python_callable=validate_snowflake)
    t5 = PythonOperator(
        task_id="generate_quality_report", 
        python_callable=generate_quality_report,
        trigger_rule='all_done'
    )
    t6 = PythonOperator(
        task_id="alert_on_quality_failure", 
        python_callable=alert_on_quality_failure,
        trigger_rule='all_done'
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
