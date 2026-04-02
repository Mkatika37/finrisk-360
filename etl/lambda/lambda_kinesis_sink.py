import base64
import json
import os
import boto3
import time
from datetime import datetime

s3_client = boto3.client('s3')
BUCKET_NAME = os.environ.get('S3_RAW_BUCKET', 'finrisk360-raw-322960458535')

def handler(event, context):
    records_to_save = []
    
    # Process Kinesis Records
    for record in event.get('Records', []):
        try:
            # Decode payload
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            parsed_payload = json.loads(payload)
            records_to_save.append(parsed_payload)
        except Exception as e:
            print(f"Error decoding record: {e}")
            
    if not records_to_save:
        print("No valid records found in batch.")
        return {'statusCode': 200, 'body': 'No records processed.'}

    # Generate timestamp and S3 path logic
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    batch_timestamp = int(time.time() * 1000)
    
    s3_key = f"loans/year={year}/month={month}/day={day}/{batch_timestamp}.json"
    
    try:
        # Write to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(records_to_save)
        )
        print(f"Successfully wrote {len(records_to_save)} records to s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Failed to write batch to S3: {e}")
        raise e
        
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Success', 'records_processed': len(records_to_save)})
    }
