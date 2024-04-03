from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pipeline import pipeline

my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['culpgrant21@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
my_dag = DAG(
    'spotify_dag',
    default_args = my_args,
    description= 'Spotify ETL',
    schedule_interval= '0 14 * * *'
)

# Définition de l'opérateur Python pour exécuter la fonction pipeline
pipeline_task = PythonOperator(
    task_id='spotify_pipeline',
    python_callable=pipeline,
    dag=my_dag
)

# L'opérateur Python de la fonction pipeline est le point de départ de votre DAG
pipeline_task
