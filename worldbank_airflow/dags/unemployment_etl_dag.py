# /opt/airflow/dags/unemployment_etl_dag.py

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import ETL functions (asegúrate de que get_data.py esté en /opt/airflow/dags/)
from get_data import run_etl, merge_country_files, country_codes, indicators, date_ranges

# DAG definition
with DAG(
    dag_id='unemployment_etl_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=['worldbank', 'etl']
) as dag:

    # Task 1: Run ETL
    run_etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl,
        op_kwargs={
            'country_codes': country_codes,
            'indicators': indicators,
            'date_ranges': date_ranges
        }
    )

    # Task 2: Merge files for each country
    merge_cz_task = PythonOperator(
        task_id='merge_cz_files',
        python_callable=merge_country_files,
        op_kwargs={'country_code': 'CZ'}
    )

    merge_euu_task = PythonOperator(
        task_id='merge_euu_files',
        python_callable=merge_country_files,
        op_kwargs={'country_code': 'EUU'}
    )

    # Task dependencies
    run_etl_task >> [merge_cz_task, merge_euu_task]
