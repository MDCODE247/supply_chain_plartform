import io
import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

load_dotenv()

# S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('DEST_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('DEST_SECRET_KEY'),
    region_name=os.getenv('DEST_REGION')
)

DEST_BUCKET = os.getenv('DEST_BUCKET')


def get_db_engine():
    """Create a database connection to Supabase"""
    host     = os.getenv('SUPABASE_HOST')
    user     = os.getenv('SUPABASE_USER')
    password = os.getenv('SUPABASE_PASSWORD')
    port     = os.getenv('SUPABASE_PORT', '5432')
    dbname   = os.getenv('SUPABASE_DBNAME', 'postgres')

    print(f"  Connecting to: {host}:{port}/{dbname} as {user}")

    url = (
        f"postgresql://{user}:{password}"
        f"@{host}:{port}/{dbname}"
        f"?sslmode=require"
    )
    engine = create_engine(url)
    return engine


def get_all_tables(engine):
    """List all tables in the public schema"""
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='public')
    return tables


def migrate_table(engine, table_name):
    """Read a table and upload to S3 as parquet"""
    print(f"\n  Processing: {table_name} ...")

    try:
        df = pd.read_sql_table(table_name, engine, schema='public')

        if df.empty:
            print(f"  SKIPPED: {table_name} is empty")
            return False

        print(f"  Rows: {len(df)} | Columns: {len(df.columns)}")

        # Convert UUID and other unsupported types to string
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        dest_key = f"supabase/{table_name}.parquet"
        s3.put_object(
            Bucket=DEST_BUCKET,
            Key=dest_key,
            Body=parquet_buffer.getvalue()
        )

        print(f"  Saved to: s3://{DEST_BUCKET}/{dest_key}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    print("=" * 40)
    print("  Supabase -> S3 Migration")
    print("=" * 40)

    # Validate env variables
    required = ['SUPABASE_HOST', 'SUPABASE_USER', 'SUPABASE_PASSWORD', 'SUPABASE_PORT', 'SUPABASE_DBNAME']
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        print(f"ERROR: Missing in .env file: {', '.join(missing)}")
        return

    # Connect to Supabase
    print("\nConnecting to Supabase database...")
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  Connected successfully!")
    except Exception as e:
        print(f"  ERROR connecting to database: {e}")
        print("  Check your Supabase credentials in .env file")
        return

    # List all tables
    print("\nFetching tables...")
    tables = get_all_tables(engine)

    if not tables:
        print("  No tables found in the database!")
        return

    print(f"\n  Found {len(tables)} tables:")
    for i, table in enumerate(tables, 1):
        print(f"    {i}. {table}")

    # Ask which tables to migrate
    print("\nWhich tables do you want to migrate?")
    print("  Type 'all' to migrate all tables")
    print("  Or type table names separated by commas")
    print("  Example: users, orders, products")
    choice = input("\nYour choice: ").strip()

    if choice.lower() == 'all':
        selected_tables = tables
    else:
        selected_tables = [t.strip() for t in choice.split(',')]

    # Migrate selected tables
    print(f"\nMigrating {len(selected_tables)} table(s)...")
    print("-" * 40)

    success_count = 0
    error_count = 0
    skipped_count = 0

    for table in selected_tables:
        if table not in tables:
            print(f"\n  WARNING: Table '{table}' not found, skipping...")
            skipped_count += 1
            continue

        success = migrate_table(engine, table)
        if success:
            success_count += 1
        else:
            error_count += 1

    print("\n" + "=" * 40)
    print("  Migration Complete!")
    print("=" * 40)
    print(f"  Tables Migrated : {success_count}")
    print(f"  Skipped         : {skipped_count}")
    print(f"  Errors          : {error_count}")
    print(f"  Location        : s3://{DEST_BUCKET}/supabase/")
    print("=" * 40)


if __name__ == "__main__":
    main()