# Desafio Final: Configuração de Pipeline com Apache Airflow

Você foi designado para criar um pipeline no Apache Airflow para manipular dados usando um banco de dados PostgreSQL. 

O objetivo é criar uma sequência de tarefas que criem tabelas em um banco de dados, insiram dados nessas tabelas e gerem informações aleatórias através de um operador Python.

Tarefas a serem realizadas:

1. Configuração Inicial:

    * Configure as credenciais do PostgreSQL no Airflow. 
    * Crie uma conexão com o banco de dados.

2. Criar Tabelas no PostgreSQL:

    * Utilize o operador PostgreSQL do Airflow para criar uma tabela no banco de dados.
    * Crie uma tabela de usuarios
    * A tabela usuarios deve ter campos como id, nome, email e idade.

3. Gerar Dados com Python:

    * Crie um operador Python que gere os dados.

4. Inserir Dados nas Tabelas:

    * Use o operador PostgreSQL para inserir dados nas tabelas usuarios e compras
    * Insira pelo menos 5 registros na tabela usuarios


Boa sorte!

## Extra

Pesquise sobre como utilizar o postgres hook no airflow e tente inserir os dados utilizando o hook ao invés do operador.