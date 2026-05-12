from airflow import DAG
import boto3
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
from sqlalchemy import text
import pandas as pd
import requests
import logging
from datetime import datetime

# Configurando o Logger nativo do Airflow
logger = logging.getLogger(__name__)

def extrair_e_carregar_dados():
    """Função que extrai da API e carrega no Postgres com tratamento de erros."""
    try:
        app_id = Variable.get("adzuna_app_id")
        app_key = Variable.get("adzuna_app_key")
    except Exception as e:
        logger.error(f"Falha ao buscar credenciais no Airflow Variables: {e}")
        raise 

    url = "https://api.adzuna.com/v1/api/jobs/br/search/1"
    parametros = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 10,
        "what": "Data Engineer",
        "where": "São Paulo",
        "content-type": "application/json"
    }

    logger.info("Iniciando extração da API da Adzuna...")
    try:
        resposta = requests.get(url, params=parametros, timeout=10)
        resposta.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão ou falha na API da Adzuna: {e}")
        raise

    try:
        dados = resposta.json()
        vagas = dados.get('results', [])
        df = pd.json_normalize(vagas)
        
        if df.empty:
            logger.warning("A API retornou sucesso, mas nenhuma vaga foi encontrada.")
            return

        logger.info(f"Sucesso: {len(vagas)} vagas extraídas da API.")
        
        df_mapeado = df.rename(columns={
            'id': 'adzuna_id', 'title': 'titulo', 'company.display_name': 'empresa',
            'location.display_name': 'localizacao', 'description': 'descricao',
            'salary_min': 'salario_min', 'salary_max': 'salario_max', 'redirect_url': 'url_vaga'
        })
        
        colunas_oficiais = ['adzuna_id', 'titulo', 'empresa', 'localizacao', 'descricao', 'salario_min', 'salario_max', 'url_vaga']
        for col in colunas_oficiais:
            if col not in df_mapeado.columns:
                df_mapeado[col] = None
                
        df_final = df_mapeado[colunas_oficiais].copy()
        df_final['salario_min'] = pd.to_numeric(df_final['salario_min'], errors='coerce')
        df_final['salario_max'] = pd.to_numeric(df_final['salario_max'], errors='coerce')
        df_final['adzuna_id'] = df_final['adzuna_id'].astype(str)
        
    except Exception as e:
        logger.error(f"Erro durante a transformação dos dados (Pandas): {e}")
        raise

    try:
        logger.info("Conectando ao PostgreSQL via Airflow Connection...")
        hook = PostgresHook(postgres_conn_id='postgres_landing_zone_conn')
        engine = hook.get_sqlalchemy_engine()
        
        logger.info("Enviando dados para tabela temporária (Staging)...")
        df_final.to_sql('stg_vagas_temp', con=engine, if_exists='replace', index=False)
        
        query_upsert = """
            INSERT INTO bronze_vagas (adzuna_id, titulo, empresa, localizacao, descricao, salario_min, salario_max, url_vaga)
            SELECT adzuna_id, titulo, empresa, localizacao, descricao, salario_min, salario_max, url_vaga
            FROM stg_vagas_temp
            ON CONFLICT (adzuna_id) DO NOTHING;
        """
        
        with engine.begin() as conn:
            conn.execute(text(query_upsert))
        logger.info("Processo finalizado com sucesso no Banco de Dados.")

    except Exception as e:
        logger.error(f"Erro de Banco de Dados: {e}")
        raise

def exportar_para_datalake():
    """Lê a tabela Bronze do Postgres e envia como arquivo para o MinIO (S3)."""
    try:
        logger.info("Conectando ao PostgreSQL para leitura...")
        hook = PostgresHook(postgres_conn_id='postgres_landing_zone_conn')
        engine = hook.get_sqlalchemy_engine()
        
        df_bronze = pd.read_sql("SELECT * FROM bronze_vagas", con=engine)
        
        if df_bronze.empty:
            logger.warning("A tabela está vazia. Nada para exportar.")
            return

        caminho_local = '/tmp/bronze_vagas.csv'
        df_bronze.to_csv(caminho_local, index=False)
        logger.info(f"Arquivo temporário salvo com {len(df_bronze)} registros.")

        logger.info("Iniciando upload para o Data Lake (MinIO/S3)...")
        import boto3
        from botocore.client import Config
        
        # Conectando ao MinIO local como se fosse a AWS
        s3 = boto3.client('s3',
                          endpoint_url='http://minio-datalake:9000', # Nome do container na rede
                          aws_access_key_id='admin',
                          aws_secret_access_key='admin1234',
                          config=Config(signature_version='s3v4'),
                          region_name='us-east-1')
        
        # O caminho onde o arquivo vai morar dentro do Bucket
        caminho_s3 = 'bronze/vagas/bronze_vagas.csv'
        
        s3.upload_file(caminho_local, 'radar-sp', caminho_s3)
        
        logger.info(f"✅ Upload concluído! Arquivo disponível em s3://radar-sp/{caminho_s3}")

    except Exception as e:
        logger.error(f"Erro ao exportar dados para o MinIO: {e}")
        raise
# --- DEFINIÇÃO ÚNICA DA DAG ---
argumentos_padrao = {
    'owner': 'arthur',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    dag_id='ingestao_adzuna_sp',
    default_args=argumentos_padrao,
    schedule_interval='@daily',
    catchup=False,
    tags=['ingest', 'bronze', 'datalake']
) as dag:

    tarefa_extracao = PythonOperator(
        task_id='extrair_api_e_carregar_bd',
        python_callable=extrair_e_carregar_dados
    )

    tarefa_exportacao_datalake = PythonOperator(
        task_id='exportar_csv_para_minIO',
        python_callable=exportar_para_datalake
    )

    # Definindo a Ordem (O Pipeline)
    tarefa_extracao >> tarefa_exportacao_datalake