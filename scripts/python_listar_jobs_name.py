import boto3
from datetime import datetime, timedelta, timezone
import certifi
import ssl
import sys
import os
from botocore.config import Config


# Defina a região
region_name = os.getenv('REGION_NAME', 'sa-east-1')

# Criar uma sessão do Boto3
session = boto3.session.Session()

# Criar clientes com verify=False
glue_client = session.client('glue', region_name=region_name, verify=False)

# --- CONFIGURAÇÕES ---
application_id = "00f8r4eca4jq5431"
data_alvo_str = "2025-05-21"  # Formato: YYYY-MM-DD


# --- CONVERSÃO DE DATA ---
data_alvo = datetime.strptime(data_alvo_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
inicio = int(data_alvo.timestamp() * 1000)
fim = int((data_alvo + timedelta(days=1)).timestamp() * 1000)

# --- CLIENTE BOTO3 ---
try:
    client = boto3.client("emr-serverless", region_name=region_name)
except Exception as e:
    print("❌ Erro ao criar cliente boto3:", e)
    sys.exit(1)

# --- LISTAGEM DOS JOBS ---
try:
    paginator = client.get_paginator("list_job_runs")
    page_iterator = paginator.paginate(applicationId=application_id)

    print(f"\n📋 JobRuns da aplicação {application_id} no dia {data_alvo_str}:\n")

    encontrou = False
    for page in page_iterator:
        for job in page.get("jobRuns", []):
            created_at = job["createdAt"]
            created_ts = int(created_at.timestamp() * 1000)
            if inicio <= created_ts < fim:
                encontrou = True
                print(f"- JobRunName: {job.get('name')} | Status: {job.get('state')} | Iniciado: {created_at}")

    if not encontrou:
        print("⚠️ Nenhum job encontrado para essa data.")

except client.exceptions.AccessDeniedException as e:
    print("❌ Acesso negado! Verifique se a role possui permissão 'emr-serverless:ListJobRuns'.")
except Exception as e:
    print("❌ Erro durante a listagem dos jobs:", e)
