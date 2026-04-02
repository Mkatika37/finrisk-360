import zipfile
import os

# Create directories
os.makedirs('ingestion', exist_ok=True)
os.makedirs('alerting', exist_ok=True)

# List of files and their target directories
files_to_create = [
    ('lambda_kinesis_sink.py', 'ingestion'), 
    ('lambda_dq_trigger.py', 'ingestion'), 
    ('lambda_alert.py', 'alerting')
]

for name, folder in files_to_create:
    full_path = os.path.join(folder, name)
    # Write empty placeholder handler
    with open(full_path, 'w') as f:
        f.write('def handler(event, context):\n    pass\n')
    
    # Create the zip file
    zip_path = os.path.join(folder, name.replace('.py', '.zip'))
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(full_path, arcname=name)

print("Created placeholder lambda scripts and zip files.")
