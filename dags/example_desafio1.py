from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    "retries": 2,
}

with DAG(
    "example_dag_desafio1",
    schedule_interval="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["squad_x", "ti"],
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS usuarios1 (
            id INTEGER,
            nome VARCHAR(100),
            email VARCHAR(100),
            idade INTEGER
        );''',
        postgres_conn_id="postgres_default",
    )

    insert_usuarios = """
        INSERT INTO usuarios1 (id, nome, email, idade)
        VALUES
            (1, 'Usuario1', 'usuario1@example.com', 25),
            (2, 'Usuario2', 'usuario2@example.com', 30),
            (3, 'Usuario3', 'usuario3@example.com', 28),
            (4, 'Usuario4', 'usuario4@example.com', 35),
            (5, 'Usuario5', 'usuario5@example.com', 22);
    """

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_default',
        sql=insert_usuarios,
        dag=dag,
    )

    create_table >> insert_data