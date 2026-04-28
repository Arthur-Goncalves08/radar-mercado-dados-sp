# 📊 Radar Mercado Dados SP

O **Radar Mercado Dados SP** é um pipeline de dados ponta a ponta projetado para extrair, processar e analisar vagas de emprego na área de dados na cidade de São Paulo. O objetivo é mapear as ferramentas e habilidades mais exigidas pelo mercado local.

## 🏗️ Arquitetura do Projeto
O projeto segue os princípios da Arquitetura Medallion (Bronze, Silver, Gold) e utiliza as seguintes tecnologias:
* **Orquestração:** Apache Airflow
* **Armazenamento (Landing Zone / Bronze):** PostgreSQL
* **Linguagem:** Python (Extração e Tratamento)
* **Infraestrutura:** Docker & Docker Compose

## 🚀 Status Atual: Fase de Infraestrutura Concluída
Nesta primeira etapa, a infraestrutura base foi provisionada e homologada:
* Estrutura de diretórios configurada (`/dags`, `/scripts`, `/sql`, `/config`).
* Ambiente isolado com `docker-compose` (Postgres + Airflow Standalone).
* Comunicação via rede interna do Docker validada.
* Persistência de dados configurada via Volumes.

## ⚙️ Como Executar a Infraestrutura Localmente

1. Clone o repositório:
   ```bash
   git clone [https://github.com/SEU_USUARIO/radar-mercado-dados-sp.git](https://github.com/SEU_USUARIO/radar-mercado-dados-sp.git)