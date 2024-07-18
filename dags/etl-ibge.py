from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import json
import boto3
#import pymongo
from sqlalchemy import create_engine
from airflow.models import Variable

#Pegando as variáveis de ambiente cadastradas no AIRFLOW

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
#mongo_password = Variable.get('mongo_password')

s3_client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key
)

# Definir default_args
default_args = {
    'owner': 'Andre Felipe',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 16),
}

# Definir a DAG e suas tasks

@dag(default_args=default_args, schedule_interval='@once', catchup=False, description='ETL IBGE', tags=['python','EDD','Postgres'])
def etl_ibge():
    """
    Um flow para obter dados do IBGE de uma base de MongoDB, da API de microrregiões do IBGE, depositar
    no datalake no S3 e no DW PostgreeSQL local
    """
    # @task
    # def extrai_mongo():
    #     data_path = '/tmp/pnadc20203.csv'
    #     client = pymongo.MongoClient(f'mongodb+srv://estudante_igti:{mongo_password}@unicluster.ixhvw.mongodb.net/ibge?retryWrites=true&w=majority')
    #     db = client.ibge
    #     pnad_collec = db.pnadc20203
    #     df = pd.DataFrame(list(pnad_collec.find()))
    #     df.to_csv(data_path, index=False, encoding='utf-8', sep=';')
    #     return data_path

    # @task
    # def data_check(file_name):
    #     df = pd.read_csv(file_name, sep=';')
    #     print(df)

    @task
    def extrai_estacoes_uf(uf):
        # URL da API do IBGE (exemplo: população estimada por município)
        url = "https://servicodados.ibge.gov.br/api/v1/bdg/estado/{uf}/estacoes"
        data_path = '/tmp/estacoes_{uf}.csv'

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
        else:
            print(f"Erro ao acessar a API: {response.status_code}")
            
        return data_path


    @task
    def upload_to_s3(file_name):
        print(f"Got filename: {file_name}")
        print(f"Got object_name: {file_name[5:]}")
        s3_client.upload_file(file_name, 'etl-ibge', f"{file_name[5:]}")
    
    # @task
    # def write_to_postgres(csv_file_path):
    #     engine = create_engine('postgresql://postgres:postgres@postgres:5432/postgres')
    #     df = pd.read_csv(csv_file_path, sep=';')
    #     if csv_file_path == "/tmp/pnadc20203.csv":
    #         df = df.loc[(df.idade >= 20) & (df.idade <= 40) & (df.sexo == 'Mulher')]
    #     df.to_sql(csv_file_path[5:-4], engine, if_exists='replace', index=False, method='multi',chunksize=1000)
    
    # ORQUESTRAÇÃO

    # mongo = extrai_mongo()
    api = extrai_estacoes_uf()
    # checagem = data_check(mongo)
    # upmongo = upload_to_s3(mongo)
    upapi = upload_to_s3(api)
    # wrmongo = write_to_postgres(mongo)
    # wrapi = write_to_postgres(api)

    # checagem >> [upmongo, wrmongo]
    api >> upapi

execucao = etl_ibge()


