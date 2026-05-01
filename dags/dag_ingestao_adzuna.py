from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd
import requests
import logging # <-- NOVA IMPORTAÇÃO
from datetime import datetime

# Configurando o Logger nativo do Airflow
logger = logging.getLogger(__name__)

def extrair_e_carregar_dados():
    """Função que extrai da API e carrega no Postgres com tratamento de erros."""
    
    # 1. BUSCA DE CREDENCIAIS
    try:
        app_id = Variable.get("adzuna_app_id")
        app_key = Variable.get("adzuna_app_key")
    except Exception as e:
        logger.error(f"Falha ao buscar credenciais no Airflow Variables: {e}")
        raise # O raise repassa o erro para o Airflow falhar a Task

    url = "https://api.adzuna.com/v1/api/jobs/br/search/1"
    parametros = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 10,
        "what": "Data Engineer",
        "where": "São Paulo",
        "content-type": "application/json"
    }

    # 2. EXTRAÇÃO DA API COM TRATAMENTO DE REDE
    logger.info("Iniciando extração da API da Adzuna...")
    try:
        # timeout=10 garante que o script não fique travado para sempre se a API estiver lenta
        resposta = requests.get(url, params=parametros, timeout=10)
        resposta.raise_for_status() # Dispara um erro automaticamente se o status não for 200 (OK)
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão ou falha na API da Adzuna: {e}")
        raise

    # 3. TRANSFORMAÇÃO E LIMPEZA
    try:
        dados = resposta.json()
        vagas = dados.get('results', [])
        df = pd.json_normalize(vagas)
        
        if df.empty:
            logger.warning("A API retornou sucesso, mas nenhuma vaga foi encontrada com esses filtros.")
            return # Encerra a função pacificamente, não é um erro crítico.

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

    # 4. CARGA NO BANCO DE DADOS (COM TRATAMENTO DE TRANSAÇÃO)
    try:
        logger.info("Conectando ao PostgreSQL via Airflow Connection...")
        hook = PostgresHook(postgres_conn_id='postgres_landing_zone_conn')
        engine = hook.get_sqlalchemy_engine()
        
        logger.info("Enviando dados para tabela temporária (Staging)...")
        df_final.to_sql('stg_vagas_temp', con=engine, if_exists='replace', index=False)
        
        logger.info("Movendo vagas novas para a Landing Zone (Upsert)...")
        query_upsert = """
            INSERT INTO bronze_vagas (adzuna_id, titulo, empresa, localizacao, descricao, salario_min, salario_max, url_vaga)
            SELECT adzuna_id, titulo, empresa, localizacao, descricao, salario_min, salario_max, url_vaga
            FROM stg_vagas_temp
            ON CONFLICT (adzuna_id) DO NOTHING;
        """
        
        with engine.begin() as conn:
            conn.execute(text(query_upsert))
            
        logger.info("Processo finalizado! Transação com o banco de dados concluída.")

    except Exception as e:
        logger.error(f"Erro de Banco de Dados: Falha ao tentar salvar na Landing Zone: {e}")
        raise


# --- DEFINIÇÃO DA DAG ---
argumentos_padrao = {
    'owner': 'arthur',
    'start_date': datetime(2024, 1, 1),
    'retries': 2, # Aumentamos para 2 retentativas, ideal para oscilações de API
}

with DAG(
    dag_id='ingestao_adzuna_sp',
    default_args=argumentos_padrao,
    schedule_interval='@daily',
    catchup=False,
    tags=['ingest', 'bronze']
) as dag:

    tarefa_extracao = PythonOperator(
        task_id='extrair_api_e_carregar_bd',
        python_callable=extrair_e_carregar_dados
    )