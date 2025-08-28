# 💼 Portfólio de Engenharia de Dados — Paulo Victor

Bem-vindo ao meu portfólio! Aqui você encontrará projetos que desenvolvi ao longo da minha carreira como engenheiro de dados, com foco em automações, pipelines, processamento distribuído e boas práticas em ambientes cloud.

## 📂 Projetos

### 🔁 [Reprocessing EMR](./emr-reprocessing/)
Automação de reprocessamentos em clusters EMR usando AWS Lambda, SQS, Athena e notificações via webhook. 
Este projeto reduz a carga operacional e garante rastreabilidade e governança nos reprocessamentos.

- [Lambda 1 - Verificação de status do cluster](./emr-reprocessing/lambda_1_verifica_status_cluster_reproc.py)
- [Lambda 2 - Inicialização do cluster EMR](./emr-reprocessing/lambda_2_inicia_cluster_emr_reproc.py)
- [Lambda 3 - Monitoramento de subida do cluster](./emr-reprocessing/lambda_3_verifica_cluster_subindo_reproc.py)
- [Lambda 4 - Submissão de etapas de reprocessamento](./emr-reprocessing/lambda_4_submete_etapa_reproc.py)
- [Lambda 5 - Notificação de resultados](./emr-reprocessing/lambda_5_notifica_resultado_reproc.py)
  
### 📊 Monitoramento com Airflow + DataDog
Integração entre Apache Airflow e DataDog para monitoramento em tempo real de DAGs, com alertas automáticos e criação de incidentes via ServiceNow.

### ⚙️ Otimização de Custos com EMR
Uso de máquinas Spot, Task Fleets e Auto Scaling para reduzir custos em clusters EMR de desenvolvimento e produção.

### 📁 Migração de Processos SAS para PySpark
Conversão de processos SAS legados para PySpark, com foco em performance, escalabilidade e integração com o Data Lake.

### 📓 Ambiente Jupyter Notebook Seguro e Escalável na AWS
Provisionamento e configuração de um ambiente Jupyter Notebook seguro, escalável e de alta disponibilidade na AWS, voltado para análises exploratórias, desenvolvimento de modelos de machine learning e experimentações com dados.

**Características:**
- Utilização de EC2, S3, IAM, VPC
- Ambiente interativo e colaborativo com segurança e escalabilidade

### 🛡️ Política Automatizada de Armazenamento e Versionamento no Data Lake
Implantação de política automatizada e auditável de armazenamento e versionamento de dados no Data Lake para otimizar custos, garantir conformidade e permitir controle total sobre movimentações e exclusões.

**Etapas realizadas:**
- Mapeamento de regras existentes e novas
- Configurações de versionamento
- Implementação de novas regras de armazenamento e classes
- Exclusão de versionamentos antigos e arquivos/buckets “zumbis”

## 🧠 Tecnologias e Ferramentas

- **Cloud:** AWS (EMR, Lambda, S3, Athena, Glue, CloudWatch, Secrets Manager), Azure DevOps
- **Orquestração:** Apache Airflow
- **Mensageria:** SQS, SNS
- **Linguagens:** Python, PySpark, SQL, Shell Script
- **Integrações:** GitHub Actions (em breve), ServiceNow, DataDog
- **Outros:** CI/CD, Modelagem de Dados, ETL, Big Data

## 📫 Contato

- [LinkedIn](https://www.linkedin.com/in/paulo-vieira-853a35188/)
- Email: victorpaulo7@hotmail.com
