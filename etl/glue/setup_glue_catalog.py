import os
import boto3
import time
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
ROLE_ARN = 'arn:aws:iam::322960458535:role/finrisk360-glue-role'
DB_NAME = 'finrisk360_catalog'

glue = boto3.client('glue', region_name=REGION)
athena = boto3.client('athena', region_name=REGION)

print("1. Creating Glue Database...")
try:
    glue.create_database(
        DatabaseInput={
            'Name': DB_NAME,
            'Description': 'FinRisk 360 mortgage risk data catalog'
        }
    )
    print(f" Database '{DB_NAME}' created.")
except glue.exceptions.AlreadyExistsException:
    print(f" Database '{DB_NAME}' already exists.")

print("\n2. Creating Glue Crawlers...")
crawlers_info = [
    {
        'Name': 'finrisk360-raw-crawler',
        'S3Path': 's3://finrisk360-raw-322960458535/loans/',
        'Prefix': 'raw_',
        'Schedule': 'cron(0 3 * * ? *)'
    },
    {
        'Name': 'finrisk360-silver-crawler',
        'S3Path': 's3://finrisk360-silver-322960458535/loans/',
        'Prefix': 'silver_',
        'Schedule': 'cron(30 3 * * ? *)'
    },
    {
        'Name': 'finrisk360-gold-crawler',
        'S3Path': 's3://finrisk360-gold-322960458535/risk_scores/',
        'Prefix': 'gold_',
        'Schedule': 'cron(0 4 * * ? *)'
    }
]

for c in crawlers_info:
    try:
        glue.create_crawler(
            Name=c['Name'],
            Role=ROLE_ARN,
            DatabaseName=DB_NAME,
            Targets={'S3Targets': [{'Path': c['S3Path']}]},
            Schedule=c['Schedule'],
            TablePrefix=c['Prefix']
        )
        print(f" Crawler '{c['Name']}' created.")
    except glue.exceptions.AlreadyExistsException:
        print(f" Crawler '{c['Name']}' already exists.")

print("\n3. Running all Crawlers...")
for c in crawlers_info:
    try:
        glue.start_crawler(Name=c['Name'])
        print(f" Started Crawler '{c['Name']}'")
    except Exception as e:
        print(f" Could not start crawler '{c['Name']}': {e}")

print("\nWaiting for Crawlers to complete (this takes ~1-2 mins)...")
# We poll for up to 3 minutes
for _ in range(36):
    running = False
    for c in crawlers_info:
        state = glue.get_crawler(Name=c['Name'])['Crawler']['State']
        if state in ('RUNNING', 'STOPPING'):
            running = True
    if not running:
        break
    time.sleep(5)

print("\n4. Verifying Tables...")
tables = glue.get_tables(DatabaseName=DB_NAME)['TableList']
print(" Tables found in catalog:")
for t in tables:
    print(f"   - {t['Name']}")

print("\n5. Creating Athena Workgroup...")
wg_name = 'finrisk360-workgroup'
try:
    athena.create_work_group(
        Name=wg_name,
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': 's3://finrisk360-raw-322960458535/athena-results/'
            }
        }
    )
    print(f" Athena workgroup '{wg_name}' created.")
except Exception as e:
    if 'AlreadyExists' in str(e) or 'already exists' in str(e).lower():
         print(f" Athena workgroup '{wg_name}' already exists.")
    else:
         print(f" Error creating workgroup: {e}")

print("\n6. Running Athena Test Query...")
query = f"SELECT risk_tier, COUNT(*) FROM {DB_NAME}.gold_risk_scores GROUP BY risk_tier"
try:
    response = athena.start_query_execution(
        QueryString=query,
        WorkGroup=wg_name
    )
    exec_id = response['QueryExecutionId']
    print(f" Query submitted! Execution ID: {exec_id}")
    
    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'SUCCEEDED':
                res = athena.get_query_results(QueryExecutionId=exec_id)
                print(" Query Results:")
                for row in res['ResultSet']['Rows']:
                    print("   " + " | ".join([col.get('VarCharValue', 'NULL') for col in row['Data']]))
            else:
                reason = athena.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status'].get('StateChangeReason')
                print(f" Query did not succeed. Status: {status}, Reason: {reason}")
            break
        time.sleep(2)
        
except Exception as e:
    print(f" Athena query error: {e}")

print("\nAWS Glue Catalog setup fully complete!")
