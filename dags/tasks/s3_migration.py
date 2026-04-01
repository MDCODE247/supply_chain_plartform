import io
import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError


def run():
    print("=" * 40)
    print("  S3 CSV/JSON Migration")
    print("=" * 40)

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

    SOURCE_BUCKET = os.getenv('SOURCE_BUCKET')
    DEST_BUCKET = os.getenv('DEST_BUCKET')
    PREFIX = "raw/"

    paginator = s3_source.get_paginator('list_objects_v2')
    file_count = 0
    skipped_count = 0
    error_count = 0

    try:
        pages = list(paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=PREFIX))
    except ClientError as e:
        print(f"ERROR: Could not access source bucket: {e}")
        return

    for page in pages:
        if 'Contents' not in page:
            print("No files found in source bucket under raw/ folder!")
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

            try:
                response = s3_source.get_object(Bucket=SOURCE_BUCKET, Key=source_key)
                content = response['Body'].read()

                if source_key.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(content))
                else:
                    df = pd.read_json(io.BytesIO(content))

                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                parquet_buffer.seek(0)

                s3_dest.put_object(
                    Bucket=DEST_BUCKET,
                    Key=dest_key,
                    Body=parquet_buffer.getvalue()
                )
                print(f"  Saved: {dest_key}")
                file_count += 1

            except Exception as e:
                print(f"  ERROR: {e}")
                error_count += 1

    print("-" * 40)
    print(f"S3 Migration Done!")
    print(f"  Files Migrated : {file_count}")
    print(f"  Skipped        : {skipped_count}")
    print(f"  Errors         : {error_count}")
