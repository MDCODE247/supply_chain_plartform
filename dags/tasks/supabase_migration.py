import io
import os
import boto3
import pandas as pd
from sqlalchemy import create_engine, inspect, text


def run():
    print("=" * 40)
    print("  Supabase Migration")
    print("=" * 40)

    s3_dest = boto3.client(
        's3',
        aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
        region_name=os.getenv('DEST_REGION')
    )

    DEST_BUCKET = os.getenv('DEST_BUCKET')

    # Pooler connection - works inside Docker
    url = "postgresql://postgres.jzwdfotpgikjhmmyojok:decommitorbounce@aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"

    print("Connecting to Supabase...")
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  Connected successfully!")
    except Exception as e:
        print(f"  ERROR connecting to Supabase: {e}")
        return

    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='public')
    print(f"Found {len(tables)} tables")

    success_count = 0
    error_count = 0
    skipped_count = 0

    for table_name in tables:
        print(f"\n  Processing: {table_name} ...")

        try:
            df = pd.read_sql_table(table_name, engine, schema='public')

            if df.empty:
                print(f"  SKIPPED: {table_name} is empty")
                skipped_count += 1
                continue

            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str)

            print(f"  Rows: {len(df)} | Columns: {len(df.columns)}")

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            dest_key = f"supabase/{table_name}.parquet"
            s3_dest.put_object(
                Bucket=DEST_BUCKET,
                Key=dest_key,
                Body=parquet_buffer.getvalue()
            )
            print(f"  Saved: s3://{DEST_BUCKET}/{dest_key}")
            success_count += 1

        except Exception as e:
            print(f"  ERROR: {e}")
            error_count += 1

    print("-" * 40)
    print(f"Supabase Migration Done!")
    print(f"  Tables Migrated : {success_count}")
    print(f"  Skipped         : {skipped_count}")
    print(f"  Errors          : {error_count}")
