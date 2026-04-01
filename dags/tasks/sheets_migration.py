import io
import os
import boto3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


def run():
    print("=" * 40)
    print("  Google Sheets Migration")
    print("=" * 40)

    s3_dest = boto3.client(
        's3',
        aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
        region_name=os.getenv('DEST_REGION')
    )

    DEST_BUCKET = os.getenv('DEST_BUCKET')
    CREDENTIALS_FILE = '/opt/airflow/dags/credentials.json'
    SHEET_ID = os.getenv('SHEET_ID')
    SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

    if not SHEET_ID:
        print("ERROR: SHEET_ID is missing in .env file")
        return

    if not os.path.exists(CREDENTIALS_FILE):
        print(f"ERROR: credentials.json not found at {CREDENTIALS_FILE}")
        return

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]

    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=scopes
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_records()
    except Exception as e:
        print(f"ERROR connecting to Google Sheets: {e}")
        return

    if not data:
        print("Sheet is empty!")
        return

    df = pd.DataFrame(data)
    print(f"Read {len(df)} rows and {len(df.columns)} columns")

    try:
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        dest_key = f"sheets/{SHEET_NAME}.parquet"
        s3_dest.put_object(
            Bucket=DEST_BUCKET,
            Key=dest_key,
            Body=parquet_buffer.getvalue()
        )
        print(f"Saved to: s3://{DEST_BUCKET}/{dest_key}")
    except Exception as e:
        print(f"ERROR uploading to S3: {e}")
        return

    print("-" * 40)
    print(f"Google Sheets Migration Done!")
    print(f"  Sheet : {SHEET_NAME}")
    print(f"  Rows  : {len(df)}")
