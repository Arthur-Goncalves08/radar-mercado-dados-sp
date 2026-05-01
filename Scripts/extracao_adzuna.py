import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Abre o "cofre"
load_dotenv()
APP_ID = os.getenv('ADZUNA_APP_ID')
APP_KEY = os.getenv('ADZUNA_APP_KEY')

# Variáveis do Banco de Dados
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

def salvar_no_postgres(df):
    """Função responsável por conectar no banco e salvar a tabela"""
    print(f"\nTentando conectar ao banco de dados em {DB_HOST}...")
    
    # Cria o "motor" de conexão
    string_conexao = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(string_conexao)
    
    # Envia os dados para a tabela 'bronze_vagas'
    df.to_sql('bronze_vagas', con=engine, if_exists='append', index=False)
    
    print("✅ Sucesso! Os dados foram injetados na Landing Zone do PostgreSQL.")

def buscar_vagas_sp():
    url = "https://api.adzuna.com/v1/api/jobs/br/search/1"
    parametros = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 10,
        "what": "Data Engineer",
        "where": "São Paulo",
        "content-type": "application/json"
    }

    print("Iniciando a busca por vagas na Adzuna...")
    resposta = requests.get(url, params=parametros)

    if resposta.status_code == 200:
        dados = resposta.json()
        vagas = dados.get('results', [])
        
        df = pd.json_normalize(vagas)
        
        if not df.empty:
            print(f"✅ {len(vagas)} vagas extraídas da API.")
            
            # --- MAPEAMENTO DE COLUNAS ---
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
            

            # Força as colunas de salário a serem números (float). Se não tiver salário, vira NaN (Not a Number), que o Postgres aceita.
            df_final['salario_min'] = pd.to_numeric(df_final['salario_min'], errors='coerce')
            df_final['salario_max'] = pd.to_numeric(df_final['salario_max'], errors='coerce')
            
            # Converte o ID para string para bater exatamente com o VARCHAR(50) do banco
            df_final['adzuna_id'] = df_final['adzuna_id'].astype(str)
            
            # Chama a função para salvar os dados
            salvar_no_postgres(df_final)
            
    elif resposta.status_code == 403:
        print("❌ Erro 403: Acesso negado. Verifique as credenciais da Adzuna.")
    else:
        print(f"❌ Erro na requisição. Código: {resposta.status_code}")
            
# O GATILHO QUE ESTAVA FALTANDO
if __name__ == "__main__":
    buscar_vagas_sp()