from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define a DAG (O fluxo de trabalho)
with DAG(
    dag_id='teste_conexao_postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['infraestrutura']
) as dag:

    # Define a Tarefa: Executa um comando SQL simples
    testar_banco = PostgresOperator(
        task_id='select_versao_postgres',
        postgres_conn_id='postgres_landing', # O mesmo nome que você colocou lá na interface!
        sql='SELECT version();'
    )