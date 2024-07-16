BS-0006 - Airflow para times de dados
========

Execute seu projeto localmente
==============================

1. Inicie o Airflow em sua máquina local executando 'astro dev start'.

Este comando irá iniciar 4 containers Docker em sua máquina, cada um para um componente diferente do Airflow:

- Postgres: Banco de dados de metadados do Airflow
- Webserver: O componente do Airflow responsável por renderizar a interface do Airflow
- Scheduler: O componente do Airflow responsável por monitorar e acionar tarefas
- Triggerer: O componente do Airflow responsável por acionar tarefas adiadas

2. Verifique se os 4 containers Docker foram criados executando 'docker ps'.

Observação: Executar 'astro dev start' iniciará seu projeto com o Airflow Webserver exposto na porta 8080 e o Postgres exposto na porta 5432. 

Se você já tiver uma dessas portas alocadas, você pode parar os containers Docker existentes ou alterar a porta

3. Acesse a interface do Airflow para o seu projeto local do Airflow. Para fazer isso, vá para http://localhost:8080/ e faça login com 'admin' tanto para o nome de usuário quanto para a senha.

Você também deve ser capaz de acessar seu banco de dados Postgres em 'localhost:5432/postgres'.
