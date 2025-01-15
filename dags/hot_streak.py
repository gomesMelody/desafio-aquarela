from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from functions.hot_streak_pyspark import hot_streaks

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    'hot_streak',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)

hot_streaks_task = PythonOperator(
    task_id='hot_streaks',
    python_callable=hot_streaks,
    dag=dag,
)


end = DummyOperator(task_id='end', dag=dag)

start >> hot_streaks_task >> end
