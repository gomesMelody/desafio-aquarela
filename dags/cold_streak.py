from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from functions.cold_streak_function import cold_streaks

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    'cold_streak',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)

cold_streaks_task = PythonOperator(
    task_id='cold_streaks',
    python_callable=cold_streaks,
    dag=dag,
)


end = DummyOperator(task_id='end', dag=dag)

start >> cold_streaks_task >> end
