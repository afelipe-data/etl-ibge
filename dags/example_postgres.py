from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    "retries": 2,
}

with DAG(
    "example_dag_postgres_classic",
    schedule_interval="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["squad_x", "ti"],
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS new_table(
            custom_id integer NOT NULL, 
            timestamp TIMESTAMP NOT NULL, 
            user_name VARCHAR (50) NOT NULL
        );''',
        postgres_conn_id="postgres_default",
    )

    insert_row = PostgresOperator(
        task_id='insert_row',
        sql='INSERT INTO new_table VALUES(%s, %s, %s)',
        parameters=(1, datetime.now(), 'user_1'),
        postgres_conn_id="postgres_default",
    )

    create_table >> insert_row