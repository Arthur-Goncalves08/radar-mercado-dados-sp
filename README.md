# 📊 Radar Mercado de Dados (São Paulo) - Data Engineering Pipeline

## 📌 Sobre o Projeto
Este projeto apresenta um pipeline de Engenharia de Dados *End-to-End* construído para extrair, armazenar, processar e analisar dados reais de vagas de emprego na área de tecnologia (com foco em Engenharia de Dados) na cidade de São Paulo. 

O projeto adota a **Arquitetura Medalhão** (Bronze, Silver, Gold) e utiliza as melhores práticas do mercado para orquestração de rotinas, conteinerização e processamento distribuído.

---

## 🚀 Arquitetura e Fluxo de Dados
* **Extração (API):** Consumo da API pública da Adzuna buscando vagas ativas no estado de SP.
* **Landing Zone (Load):** Armazenamento temporário e seguro dos dados brutos no PostgreSQL com tratamento de duplicatas (`Upsert / ON CONFLICT`).
* **Camada Bronze (Data Lake):** Exportação automatizada dos dados da Landing Zone para um repositório de objetos local (MinIO), simulando a nuvem da AWS (S3).
* **Camada Silver (Limpeza):** Processamento com PySpark para remoção de nulos, deduplicação de IDs e padronização textual, salvando no formato de alta performance Parquet.
* **Camada Gold (Analytics):** Enriquecimento dos textos utilizando *Expressões Regulares (Regex)* para identificar ferramentas exigidas (Python, SQL, AWS, etc) e criação de um *Data Mart* agregado por nível de senioridade.
* **Orquestração:** Todo o fluxo de ingestão inicial é agendado e monitorado via Apache Airflow.

---

## 🛠️ Stack Tecnológico
* **Linguagens:** Python, SQL
* **Orquestração:** Apache Airflow
* **Bancos de Dados:** PostgreSQL (Relacional)
* **Object Storage (Data Lake):** MinIO (S3 Compatible)
* **Processamento Big Data:** Apache Spark (PySpark), Pandas
* **Infraestrutura:** Docker, Docker Compose
* **Visualização:** Seaborn, Tableau

---

## 📋 Status do Projeto (Kanban)
* [x] Criação da infraestrutura via Docker Compose (Airflow, Postgres, MinIO).
* [x] Desenvolvimento da DAG de ingestão resiliente com logs estruturados.
* [x] Extração de dados da API Adzuna e injeção segura no PostgreSQL.
* [x] Integração Airflow + AWS Boto3 para exportação de dados.
* [x] Setup do bucket `radar-sp` no MinIO (Camada Bronze).
* [x] Configuração do container PySpark (Jupyter) com integração nativa ao MinIO via S3A.
* [x] **Camada Silver:** Limpeza de strings e remoção de duplicatas com PySpark.
* [x] **Camada Gold:** Extração de ferramentas (SQL, Python, Airflow, etc.) usando Regex.
* [x] **Data Mart:** Agregação dimensional e contagem de ferramentas por nível de experiência.
* [x] **Visualização:** Exportação da modelagem final para dashboards analíticos.

---

## 📈 Principais Insights Extraídos
* **A Base é Inegociável:** Python e SQL dominam as requisições em todos os níveis de experiência, confirmando serem o alicerce da área de dados.
* **O Salto de Senioridade:** A exigência por conhecimentos em infraestrutura em nuvem (AWS/GCP), orquestradores (Airflow) e containers (Docker) cresce exponencialmente nas vagas de nível Pleno e Sênior.
* **Qualidade Importa:** A aplicação rigorosa da limpeza na Camada Silver evitou a dupla contagem de vagas da API, garantindo a integridade absoluta dos indicadores finais.

---

## ⚙️ Como Executar o Projeto Localmente
1. Clone o repositório.
2. Certifique-se de ter o Docker e o Docker Compose instalados.
3. Crie um arquivo `.env` na raiz com suas credenciais de banco.
4. Na raiz do projeto, suba os serviços orquestrados executando: `docker-compose up -d`
5. Acesse o Airflow (`localhost:8080`) e o Jupyter Lab (`localhost:8888`) para acompanhar o pipeline.