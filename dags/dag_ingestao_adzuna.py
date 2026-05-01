from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd
import requests
from datetime import datetime

def extrair_e_carregar_dados():
    """Função que extrai da API e carrega no Postgres usando recursos nativos do Airflow."""
    
    # 1. Busca as credenciais no cofre do Airflow (TASK-8)
    app_id = Variable.get("adzuna_app_id")
    app_key = Variable.get("adzuna_app_key")

    url = "https://api.adzuna.com/v1/api/jobs/br/search/1"
    parametros = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 10,
        "what": "Data Engineer",
        "where": "São Paulo",
        "content-type": "application/json"
    }

    print("Iniciando extração da API da Adzuna...")
    resposta = requests.get(url, params=parametros)

    if resposta.status_code == 200:
        dados = resposta.json()
        vagas = dados.get('results', [])
        df = pd.json_normalize(vagas)
        
        if not df.empty:
            print(f"✅ {len(vagas)} vagas extraídas da API.")
            
            # --- MAPEAMENTO E LIMPEZA ---
            df_mapeado = df.rename(columns={
                'id': 'adzuna_id',
                'title': 'titulo',
                'company.display_name': 'empresa',
                'location.display_name': 'localizacao',
                'description': 'descricao',
                'salary_min': 'salario_min',
                'salary_max': 'salario_max',
                'redirect_url': 'url_vaga'
            })
            
            colunas_oficiais = ['adzuna_id', 'titulo', 'empresa', 'localizacao', 'descricao', 'salario_min', 'salario_max', 'url_vaga']
            for col in colunas_oficiais:
                if col not in df_mapeado.columns:
                    df_mapeado[col] = None
                    
            df_final = df_mapeado[colunas_oficiais].copy()
            
            # Tipagem rigorosa para o Postgres
            df_final['salario_min'] = pd.to_numeric(df_final['salario_min'], errors='coerce')
            df_final['salario_max'] = pd.to_numeric(df_final['salario_max'], errors='coerce')
            df_final['adzuna_id'] = df_final['adzuna_id'].astype(str)
            
            # 2. Conecta no banco usando a Connection do Airflow (TASK-8)
            print("Conectando ao PostgreSQL via Airflow Connection...")
            hook = PostgresHook(postgres_conn_id='postgres_landing_zone_conn')
            engine = hook.get_sqlalchemy_engine()
            
            # 3. Carga de Dados Inteligente (Staging + ON CONFLICT)
            print("Enviando dados para tabela temporária (Staging)...")
            df_final.to_sql('stg_vagas_temp', con=engine, if_exists='replace', index=False)
            
            print("Movendo vagas novas para a Landing Zone e ignorando duplicatas...")
            query_upsert = """
                INSERT INTO bronze_vagas (adzuna_id, titulo, empresa, localizacao, descricao, salario_min, salario_max, url_vaga)
                SELECT adzuna_id, titulo, empresa, localizacao, descricao, salario_min, salario_max, url_vaga
                FROM stg_vagas_temp
                ON CONFLICT (adzuna_id) DO NOTHING;
            """
            
            # Executa o comando SQL seguro
            with engine.begin() as conn:
                conn.execute(text(query_upsert))
                
            print("✅ Processo finalizado! Vagas novas adicionadas com sucesso.")
            
    else:
        raise Exception(f"Falha na API Adzuna. Status Code: {resposta.status_code}")
            

# --- DEFINIÇÃO DA DAG ---
argumentos_padrao = {
    'owner': 'arthur',
    'start_date': datetime(2024, 1, 1),
    'retries': 1 # Se falhar, tenta de novo 1 vez
}

with DAG(
    dag_id='ingestao_adzuna_sp',
    default_args=argumentos_padrao,
    schedule_interval='@daily', # Agendado para rodar uma vez por dia
    catchup=False,              # Evita que ele tente rodar dias passados de uma vez
    tags=['ingest', 'bronze']
) as dag:

    # Criação da Tarefa
    tarefa_extracao = PythonOperator(
        task_id='extrair_api_e_carregar_bd',
        python_callable=extrair_e_carregar_dados
    )