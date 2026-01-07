from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("Hello from Airflow!")


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # запускаем вручную
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=hello,
    )
