import json

from pendulum import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def extract():
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    order_data_dict = json.loads(data_string)
    return order_data_dict

def transform(**kwargs):
    order_data_dict = kwargs['ti'].xcom_pull(task_ids='extract_data')

    total_order_value = sum(order_data_dict.values())
    return {"total_order_value": total_order_value}

def load(**kwargs):
    total_order_value = kwargs['ti'].xcom_pull(task_ids='transform_data')['total_order_value']

    print(f"Total order value is: {total_order_value:.2f}")

default_args = {
    "retries": 2,
}

with DAG(
    "example_dag_basic_classic",
    schedule_interval="* * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["squad_x", "financeiro"],
) as dag:

    order_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )

    order_summary = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

order_data >> order_summary >> load_data
