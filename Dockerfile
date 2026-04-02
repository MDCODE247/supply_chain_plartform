FROM apache/airflow:2.8.1

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    pyarrow \
    python-dotenv \
    gspread \
    google-auth \
    psycopg2-binary \
    sqlalchemy \
    snowflake-connector-python \
    dbt-snowflake
