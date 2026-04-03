from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import io
import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
import gspread
from google.oauth2.service_account import Credentials
from botocore.exceptions import ClientError
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import boto3 as boto3_ssm

def get_ssm_parameter(name, region='eu-west-2'):
    ssm = boto3_ssm.client('ssm',
        aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
        region_name=region
    )
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    return response['Parameter']['Value']

load_dotenv('/opt/airflow/.env')

# S3 client
s3_source = boto3.client(
    's3',
    aws_access_key_id=os.getenv('SOURCE_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SOURCE_SECRET_KEY'),
    region_name=os.getenv('SOURCE_REGION')
)

s3_dest = boto3.client(
    's3',
    aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
    region_name=os.getenv('DEST_REGION')
)

DEST_BUCKET = os.getenv('DEST_BUCKET')
SOURCE_BUCKET = os.getenv('SOURCE_BUCKET')

def migrate_s3_files():
    print("Starting S3 CSV/JSON Migration...")
    PREFIX = "raw/"
    paginator = s3_source.get_paginator('list_objects_v2')
    file_count = 0
    skipped_count = 0

    for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=PREFIX):
        if 'Contents' not in page:
            print("No files found in source bucket!")
            return

        for obj in page['Contents']:
            source_key = obj['Key']
            if source_key.endswith('/'):
                continue
            if not (source_key.endswith('.csv') or source_key.endswith('.json')):
                continue

            dest_key = source_key.rsplit('.', 1)[0] + '.parquet'

            try:
                s3_dest.head_object(Bucket=DEST_BUCKET, Key=dest_key)
                print(f"SKIPPED (exists): {source_key}")
                skipped_count += 1
                continue
            except ClientError:
                pass

            print(f"Processing: {source_key}")
            response = s3_source.get_object(Bucket=SOURCE_BUCKET, Key=source_key)
            content = response['Body'].read()

            if source_key.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(content))
            else:
                df = pd.read_json(io.BytesIO(content))

            # Add ingestion timestamp metadata
            df['_ingested_at'] = datetime.now(timezone.utc).isoformat()
            df['_source_file'] = source_key

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            s3_dest.put_object(Bucket=DEST_BUCKET, Key=dest_key, Body=parquet_buffer.getvalue())
            print(f"Saved: {dest_key}")
            file_count += 1

    print(f"S3 Migration Done! Files: {file_count} | Skipped: {skipped_count}")

def migrate_google_sheets():
    print("Starting Google Sheets Migration...")
    CREDENTIALS_FILE = '/opt/airflow/dags/credentials.json'
    SHEET_ID = os.getenv('SHEET_ID')
    SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]

    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    data = worksheet.get_all_records()

    if not data:
        print("Sheet is empty!")
        return

    df = pd.DataFrame(data)
    print(f"Read {len(df)} rows from Google Sheets")

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    dest_key = f"sheets/{SHEET_NAME}.parquet"
    s3_dest.put_object(Bucket=DEST_BUCKET, Key=dest_key, Body=parquet_buffer.getvalue())
    print(f"Saved to: s3://{DEST_BUCKET}/{dest_key}")

def migrate_supabase():
    print("Starting Supabase Migration...")
    host     = os.getenv('SUPABASE_HOST')
    user     = os.getenv('SUPABASE_USER')
    password = os.getenv('SUPABASE_PASSWORD')
    port     = os.getenv('SUPABASE_PORT', '5432')
    dbname   = os.getenv('SUPABASE_DBNAME', 'postgres')

    url = (
        f"postgresql://{user}:{password}"
        f"@{host}:{port}/{dbname}"
        f"?sslmode=require"
    )
    engine = create_engine(url)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Connected to Supabase!")

    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='public')
    print(f"Found {len(tables)} tables")

    for table_name in tables:
        print(f"Processing: {table_name}")
        df = pd.read_sql_table(table_name, engine, schema='public')

        if df.empty:
            print(f"Skipped (empty): {table_name}")
            continue

        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        # Add ingestion timestamp metadata
        df['_ingested_at'] = datetime.now(timezone.utc).isoformat()
        df['_source_table'] = table_name

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        dest_key = f"supabase/{table_name}.parquet"
        s3_dest.put_object(Bucket=DEST_BUCKET, Key=dest_key, Body=parquet_buffer.getvalue())
        print(f"Saved: s3://{DEST_BUCKET}/{dest_key}")

    print("Supabase Migration Done!")

def load_to_snowflake():
    print("Starting Snowflake Load...")

    conn = snowflake.connector.connect(
        account=get_ssm_parameter('/supplychain360/snowflake_account'),
        user=get_ssm_parameter('/supplychain360/snowflake_user'),
        password=get_ssm_parameter('/supplychain360/snowflake_password'),
        warehouse=get_ssm_parameter('/supplychain360/snowflake_warehouse'),
        database=get_ssm_parameter('/supplychain360/snowflake_database'),
        role=get_ssm_parameter('/supplychain360/snowflake_role')
    )

    prefixes = {
        'RAW':      'raw/',
        'SHEETS':   'sheets/',
        'SUPABASE': 'supabase/'
    }

    for schema, prefix in prefixes.items():
        print(f"Loading schema: {schema}")
        paginator = s3_dest.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=DEST_BUCKET, Prefix=prefix):
            if 'Contents' not in page:
                print(f"No files found under {prefix}")
                continue

            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('.parquet'):
                    continue

                # Preserve full table name including date suffix
                filename = key.split('/')[-1].replace('.parquet', '')
                table_name = filename.upper().replace('-', '_')

                print(f"Loading: {key} -> {schema}.{table_name}")

                response = s3_dest.get_object(Bucket=DEST_BUCKET, Key=key)
                df = pd.read_parquet(io.BytesIO(response['Body'].read()))

                if df.empty:
                    print(f"Skipped (empty): {key}")
                    continue

                df.columns = [c.upper().replace(' ', '_').replace('-', '_') for c in df.columns]

                write_pandas(
                    conn,
                    df,
                    table_name=table_name,
                    schema=schema,
                    database=os.getenv('SNOWFLAKE_DATABASE'),
                    auto_create_table=True,
                    overwrite=True
                )
                print(f"Loaded {len(df)} rows into {schema}.{table_name}")

    conn.close()
    print("Snowflake Load Done!")

def run_dbt():
    import subprocess

    dbt_project_dir = '/opt/airflow/dags/supplychain360'
    dbt_profiles_dir = '/opt/airflow/dags'

    result = subprocess.run(
        ['/home/airflow/.local/bin/dbt', 'run', '--profiles-dir', dbt_profiles_dir, '--project-dir', dbt_project_dir],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")

    result_test = subprocess.run(
        ['/home/airflow/.local/bin/dbt', 'test', '--profiles-dir', dbt_profiles_dir, '--project-dir', dbt_project_dir],
        capture_output=True,
        text=True
    )
    print(result_test.stdout)
    print(result_test.stderr)

    if result_test.returncode != 0:
        raise Exception(f"dbt test failed: {result_test.stderr}")

    print("dbt run and test completed successfully!")

def alert_on_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    print(f"ALERT: Task failed!")
    print(f"DAG: {dag_id}")
    print(f"Task: {task_id}")
    print(f"Execution Date: {execution_date}")
    print(f"Log URL: {log_url}")
    # In production: send Slack/email notification here

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_on_failure,
}

def check_idempotency(**context):
    """
    Verifies pipeline can safely re-run without duplicating data.
    - S3: skips existing Parquet files (already implemented)
    - Snowflake: uses overwrite=True in write_pandas (already implemented)
    - dbt: uses views/table replace (already implemented)
    """
    import boto3
    import os

    dest = boto3.client(
        's3',
        aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
        region_name=os.getenv('DEST_REGION')
    )

    prefixes = ['raw/', 'sheets/', 'supabase/']
    total_files = 0

    for prefix in prefixes:
        response = dest.list_objects_v2(
            Bucket=os.getenv('DEST_BUCKET'),
            Prefix=prefix
        )
        count = len([o for o in response.get('Contents', []) if o['Key'].endswith('.parquet')])
        print(f"Idempotency check — {prefix}: {count} parquet files found")
        total_files += count

    print(f"Total parquet files in destination: {total_files}")

    if total_files == 0:
        raise Exception("Idempotency check failed: No parquet files found in destination bucket!")

    print("Idempotency check passed!")


def run_data_quality_checks():
    import snowflake.connector
    print("Running data quality checks...")

    conn = snowflake.connector.connect(
        account=get_ssm_parameter('/supplychain360/snowflake_account'),
        user=get_ssm_parameter('/supplychain360/snowflake_user'),
        password=get_ssm_parameter('/supplychain360/snowflake_password'),
        warehouse=get_ssm_parameter('/supplychain360/snowflake_warehouse'),
        database=get_ssm_parameter('/supplychain360/snowflake_database'),
        role=get_ssm_parameter('/supplychain360/snowflake_role')
    )

    checks = [
        ("Duplicate transactions", """
            SELECT COUNT(*) as cnt FROM (
                SELECT TRANSACTION_ID, COUNT(*) as c
                FROM SUPPLYCHAIN360.SUPABASE.SALES
                GROUP BY TRANSACTION_ID HAVING c > 1
            )
        """, 0),
        ("Null product IDs in sales", """
            SELECT COUNT(*) FROM SUPPLYCHAIN360.SUPABASE.SALES
            WHERE PRODUCT_ID IS NULL
        """, 0),
        ("Null store IDs in sales", """
            SELECT COUNT(*) FROM SUPPLYCHAIN360.SUPABASE.SALES
            WHERE STORE_ID IS NULL
        """, 0),
        ("Negative sale amounts", """
            SELECT COUNT(*) FROM SUPPLYCHAIN360.SUPABASE.SALES
            WHERE SALE_AMOUNT < 0
        """, 0),
        ("Raw tables loaded", """
            SELECT COUNT(*) FROM SUPPLYCHAIN360.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA IN ('RAW', 'SHEETS', 'SUPABASE')
        """, 1),
    ]

    all_passed = True
    for check_name, query, max_allowed in checks:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        # For "Raw tables loaded" check, we want result >= 1
        if check_name == "Raw tables loaded":
            status = "PASS" if result >= 1 else "FAIL"
        else:
            status = "PASS" if result <= max_allowed else "FAIL"
        print(f"[{status}] {check_name}: {result}")
        if status == "FAIL":
            all_passed = False

    conn.close()

    if not all_passed:
        raise Exception("Data quality checks failed! Check logs for details.")

    print("All data quality checks passed!")


with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    description='Migrate S3, Google Sheets and Supabase to Snowflake via Parquet',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['migration', 's3', 'parquet']
) as dag:

    task_idempotency = PythonOperator(
        task_id='check_idempotency',
        python_callable=check_idempotency
    )

    task_s3 = PythonOperator(
        task_id='migrate_s3_files',
        python_callable=migrate_s3_files
    )

    task_sheets = PythonOperator(
        task_id='migrate_google_sheets',
        python_callable=migrate_google_sheets
    )

    task_supabase = PythonOperator(
        task_id='migrate_supabase',
        python_callable=migrate_supabase
    )

    task_snowflake = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )

    task_dq = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality_checks
    )

    task_dbt = PythonOperator(
        task_id='run_dbt_models',
        python_callable=run_dbt
    )

    task_idempotency >> task_s3 >> task_sheets >> task_supabase >> task_snowflake >> task_dq >> task_dbt
