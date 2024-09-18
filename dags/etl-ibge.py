import os
from airflow import DAG
from datetime import datetime
import pandas as pd
import requests
import json
import boto3
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Lista de estados brasileiros

estados = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 
    'SP', 'SE', 'TO'
]

#Pegando as variáveis de ambiente cadastradas no AIRFLOW

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')


s3_client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key
)

def extrai_estacoes_uf(estado):

    url = f'https://servicodados.ibge.gov.br/api/v1/bdg/estado/{estado}/estacoes'
    data_path = f'/tmp/estacoes_{estado}.csv'

    # Fazer a solicitação GET para a API
    response = requests.get(url)

    # Verificar se a solicitação foi bem-sucedida
    if response.status_code == 200:
        # Parse o JSON retornado
        dados_json = json.loads(response.text)

        # Verificar a estrutura dos dados
        print(json.dumps(dados_json, indent=4))
        
        # Se os dados forem uma lista de dicionários, carregar diretamente em um DataFrame
        if isinstance(dados_json, list):
            df = pd.DataFrame(dados_json)
        elif isinstance(dados_json, dict):
            # Se os dados forem um dicionário, converter para uma lista de dicionários
            # Supondo que o dicionário contém uma chave que tem a lista de dados desejada
            chave_dados = 'resultados'  # Ajuste esta chave conforme a estrutura da resposta da API
            df = pd.DataFrame(dados_json[chave_dados])
              
        # Salvar o DataFrame em um arquivo CSV
        df.to_csv(data_path, index=False)
        s3_client.upload_file(data_path, 'etl-ibge', f"{data_path[5:]}")
    else:
        print(f"Erro ao acessar a API: {response.status_code}")


# Criação da DAG
with DAG(
    'ExtraçãoestaçõesIBGE-UF',
    default_args={
        'owner': 'Afelipe',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Extrai dados da API de estações geológicas do IBGE e salva em um bucket S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['ibge', 's3'],
) as dag:

    # Criação da tarefa com Dynamic Task Mapping

    for estado in estados:
        PythonOperator(
            task_id=f'process_state_{estado}',
            python_callable=extrai_estacoes_uf,
            op_args=[estado],
            dag=dag,
        )

