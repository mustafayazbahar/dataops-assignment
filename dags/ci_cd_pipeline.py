from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dataops_student',
    'start_date': datetime(2023, 11, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('dataops_cleaning_pipeline',
         default_args=default_args,
         schedule_interval='@daily', # Her gun calissin
         catchup=False) as dag:

    # Python Kodu Spark-Client uzerinde calisacak
    # Once gerekli kutuphaneleri yukluyoruz (s3fs, sqlalchemy, psycopg2)
    # Sonra scripti calistiriyoruz.
    
    # Not: Git-Sync kodlari /opt/airflow/dags/repo altina indirir (git-sync ayarina gore degisebilir)
    # Spark-Client'da bu path'in aynisi mount edildi.
    
    cleaning_task = SSHOperator(
        task_id='clean_and_load_data',
        ssh_conn_id='ssh_spark_client', # UI'dan olusturacagiz
        command="""
            pip install pandas s3fs sqlalchemy psycopg2-binary && 
            python3 /opt/airflow/dags/repo/scripts/cleaning_script.py
        """,
        cmd_timeout=300
    )

    cleaning_task