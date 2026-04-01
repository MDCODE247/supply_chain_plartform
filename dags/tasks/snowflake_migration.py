import io
import os
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
        region_name=os.getenv('DEST_REGION')
    )


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        role='ACCOUNTADMIN'
    )


def sanitize_name(name):
    return name.upper().replace('-', '_').replace('.', '_').replace(' ', '_')


def load_parquet_to_snowflake(s3, conn, bucket, s3_key, schema, table_name):
    print(f"\n  Loading: s3://{bucket}/{s3_key} → {schema}.{table_name}")

    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(content))

        if df.empty:
            print(f"  SKIPPED: {s3_key} is empty")
            return False

        df.columns = [sanitize_name(col) for col in df.columns]

        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        print(f"  Rows: {len(df)} | Columns: {len(df.columns)}")

        success, chunks, rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema,
            database=os.getenv('SNOWFLAKE_DATABASE'),
            auto_create_table=True,
            overwrite=True
        )

        if success:
            print(f"  Loaded {rows} rows into {schema}.{table_name}")
            return True
        else:
            print(f"  Failed to load {s3_key}")
            return False

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def run():
    print("=" * 40)
    print("  S3 -> Snowflake Migration")
    print("=" * 40)

    DEST_BUCKET = os.getenv('DEST_BUCKET')
    s3 = get_s3_client()

    print("\nConnecting to Snowflake...")
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")
        print("  Connected successfully!")
    except Exception as e:
        print(f"  ERROR: {e}")
        return

    folders = [
        ('raw/', 'RAW'),
        ('sheets/', 'SHEETS'),
        ('supabase/', 'SUPABASE')
    ]

    success_count = 0
    error_count = 0

    for prefix, schema in folders:
        print(f"\nProcessing: {prefix} → {schema}")

        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS {os.getenv('SNOWFLAKE_DATABASE')}.{schema}"
        )

        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=DEST_BUCKET, Prefix=prefix)

        for page in pages:
            if 'Contents' not in page:
                print(f"  No files in {prefix}")
                continue

            for obj in page['Contents']:
                s3_key = obj['Key']
                if not s3_key.endswith('.parquet'):
                    continue

                file_name = s3_key.split('/')[-1].replace('.parquet', '')
                table_name = sanitize_name(file_name)

                success = load_parquet_to_snowflake(
                    s3, conn, DEST_BUCKET, s3_key, schema, table_name
                )

                if success:
                    success_count += 1
                else:
                    error_count += 1

    conn.close()

    print("\n" + "=" * 40)
    print("  Snowflake Migration Complete!")
    print("=" * 40)
    print(f"  Tables Loaded : {success_count}")
    print(f"  Errors        : {error_count}")
    print(f"  Database      : {os.getenv('SNOWFLAKE_DATABASE')}")
    print("=" * 40)
