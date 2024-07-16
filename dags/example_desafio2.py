from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    "retries": 2,
}

dados_usuarios = [
    {'id': 1, 'nome': 'Usuario1', 'email': 'usuario1@example.com', 'idade': 25},
    {'id': 2, 'nome': 'Usuario2', 'email': 'usuario2@example.com', 'idade': 30},
    {'id': 3, 'nome': 'Usuario3', 'email': 'usuario3@example.com', 'idade': 28},
    {'id': 4, 'nome': 'Usuario4', 'email': 'usuario4@example.com', 'idade': 35},
    {'id': 5, 'nome': 'Usuario5', 'email': 'usuario5@example.com', 'idade': 22},
]


def insert_rows():
    insert_query = "INSERT INTO usuarios2 (id, nome, email, idade) VALUES "
    values = []

    for usuario in dados_usuarios:
        values.append(f"({usuario['id']}, '{usuario['nome']}', '{usuario['email']}', {usuario['idade']})")

    insert_query += ', '.join(values)

    insert_data = PostgresOperator(
        task_id='insert_usuarios',
        postgres_conn_id='postgres_default',
        sql=insert_query,
        dag=dag,
    )
    insert_data.execute(context=None)

with DAG(
    "example_dag_desafio2",
    schedule_interval="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["squad_x", "ti"],
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS usuarios2 (
            id INTEGER,
            nome VARCHAR(100),
            email VARCHAR(100),
            idade INTEGER
        );''',
        postgres_conn_id="postgres_default",
    )

    insert_users = PythonOperator(
        task_id='insert_users',
        python_callable=insert_rows,
        dag=dag,
    )
    create_table >> insert_users