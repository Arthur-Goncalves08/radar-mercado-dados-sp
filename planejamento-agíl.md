# 📋 Gestão Ágil e Planejamento (Scrum/Kanban)

Este projeto foi gerenciado simulando um ambiente real de Engenharia de Dados, utilizando a metodologia ágil para dividir a complexidade da arquitetura em entregas de valor contínuas (Sprints) organizadas por Épicos.

## 🎯 Resumo dos Épicos (Backlog do Projeto)

### Épico 1: [INFRA] Configuração de Ambiente e Contêineres
* **Objetivo:** Provisionar uma infraestrutura isolada e escalável sem sobrecarregar a máquina local.
* **Tarefas Concluídas:**
  * `TASK-1`: Desenho da Arquitetura (Airflow, Postgres, MinIO).
  * `TASK-2`: Criação e orquestração do `docker-compose.yml`.
  * `TASK-3`: Configuração de redes internas e conectores (Connections/Variables).
  * `TASK-4`: Testes de persistência de volumes.

### Épico 2: [INGEST] Pipeline de Coleta e Landing Zone
* **Objetivo:** Automatizar a extração segura de dados via API e garantir o armazenamento bruto.
* **Tarefas Concluídas:**
  * `TASK-5`: Desenvolvimento de script Python para consumo da API da Adzuna.
  * `TASK-6`: Modelagem do Schema (DDL) e Tipagem no PostgreSQL.
  * `TASK-7`: Implementação da DAG no Airflow com estratégia de `Upsert` (idempotência).
  * `TASK-8`: Gestão de senhas e credenciais no cofre do Airflow.
  * `TASK-9`: Implementação de observabilidade e logs estruturados (try/except).

### Épico 3: [DATALAKE & PROCESSAMENTO] Camadas Bronze e Silver
* **Objetivo:** Transição do relacional para Big Data, utilizando Object Storage e Processamento Distribuído.
* **Tarefas Concluídas:**
  * `TASK-10`: Integração (Airflow + Boto3) para descarregar o raw data no MinIO (Camada Bronze).
  * `TASK-11`: Configuração do ambiente Jupyter/PySpark conectado via `s3a://`.
  * `TASK-12`: Transformação de dados (Drop Nulls, Deduplicação) e conversão para formato Parquet (Camada Silver).

### Épico 4: [ANALYTICS] Enriquecimento Gold e Data Mart
* **Objetivo:** Aplicar regras de negócio e modelar os dados para o usuário final (Business Intelligence).
* **Tarefas Concluídas:**
  * `TASK-13`: Extração de ferramentas (Python, SQL, Cloud) de textos longos utilizando Expressões Regulares (Regex).
  * `TASK-14`: Agregação de volume de vagas por nível de Senioridade (Data Mart).
  * `TASK-15`: Despivotamento (Melt Pandas) e exportação para visualização em dashboards.

> *Nota: Todo o ciclo de vida deste projeto acompanhou as revisões de código e documentação técnica contínua no `DiarioDeBordo.ipynb`.*