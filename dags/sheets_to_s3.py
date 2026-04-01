import os
import io
import boto3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# Google Sheets config
CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE')
SHEET_ID = os.getenv('SHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

# S3 config
DEST_BUCKET = os.getenv('DEST_BUCKET')
DEST_REGION = os.getenv('DEST_REGION')
DEST_ACCESS_KEY = os.getenv('DEST_ACCESS_KEY')
DEST_SECRET_KEY = os.getenv('DEST_SECRET_KEY')

# S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=DEST_ACCESS_KEY,
    aws_secret_access_key=DEST_SECRET_KEY,
    region_name=DEST_REGION
)

def get_google_sheet_data():
    """Connect to Google Sheets and return data as a DataFrame"""
    print("Connecting to Google Sheets...")

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]

    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)

    # Open the sheet
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)

    # Get all data
    data = worksheet.get_all_records()

    if not data:
        print("ERROR: Sheet is empty or has no data!")
        return None

    df = pd.DataFrame(data)
    print(f"Successfully read {len(df)} rows and {len(df.columns)} columns from Google Sheets")
    return df

def upload_to_s3(df, sheet_name):
    """Convert DataFrame to Parquet and upload to S3"""
    print(f"Converting to Parquet and uploading to S3...")

    # Convert to parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    # Define S3 destination path
    dest_key = f"sheets/{sheet_name}.parquet"

    # Upload to S3
    s3.put_object(
        Bucket=DEST_BUCKET,
        Key=dest_key,
        Body=parquet_buffer.getvalue()
    )

    print(f"Successfully uploaded to s3://{DEST_BUCKET}/{dest_key}")
    return dest_key

def main():
    print("=" * 40)
    print("Google Sheets -> S3 Migration")
    print("=" * 40)

    # Validate env variables
    if not SHEET_ID:
        print("ERROR: SHEET_ID is missing in your .env file")
        return
    if not DEST_BUCKET:
        print("ERROR: DEST_BUCKET is missing in your .env file")
        return
    if not CREDENTIALS_FILE or not os.path.exists(CREDENTIALS_FILE):
        print("ERROR: credentials.json file not found!")
        return

    # Step 1 - Get data from Google Sheets
    df = get_google_sheet_data()
    if df is None:
        return

    # Step 2 - Preview the data
    print("\nData Preview (first 5 rows):")
    print(df.head())
    print(f"\nColumns: {list(df.columns)}")
    print(f"Total Rows: {len(df)}")

    # Step 3 - Upload to S3
    dest_key = upload_to_s3(df, SHEET_NAME)

    print("\n" + "=" * 40)
    print("Migration Complete!")
    print(f"Sheet      : {SHEET_NAME}")
    print(f"Rows       : {len(df)}")
    print(f"Destination: s3://{DEST_BUCKET}/{dest_key}")
    print("=" * 40)

if __name__ == "__main__":
    main()