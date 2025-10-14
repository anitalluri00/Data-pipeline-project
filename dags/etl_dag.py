from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data, get_mysql_connection

def run_etl():
    df = extract_data()
    df_transformed = transform_data(df)
    conn = get_mysql_connection()
    load_data(df_transformed, conn)
with DAG(
    "etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    etl_task = PythonOperator(
        task_id="run_etl_task",
        python_callable=run_etl
    )
    etl_task