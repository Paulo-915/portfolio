import boto3
import certifi
import urllib3
import os
import time
import csv
from datetime import datetime

# Oculta os avisos de SSL por uso de verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

region_name = os.getenv('REGION_NAME', 'sa-east-1')
session = boto3.session.Session()

# Inicializa clientes
s3_client = boto3.client('s3', region_name=region_name, verify=False)

#s3://chatbot-onboarding-prd/ds-pdi/chromadb/
# Configurações
bucket = 'chatbot-onboarding-prd'
prefix = 'ds-pdi/chromadb/'
dias_disponibilidade = 7
tier_recuperacao = 'Standard'  # Pode ser 'Bulk', 'Standard', ou 'Expedited'

# Lista objetos no bucket com o prefixo especificado
paginator = s3_client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

# Inicia restauração para objetos arquivados
for page in pages:
    for obj in page.get('Contents', []):
        key = obj['Key']
        head = s3_client.head_object(Bucket=bucket, Key=key)
        storage_class = head.get('StorageClass', '')

        if storage_class in ['GLACIER', 'GLACIER_IR', 'DEEP_ARCHIVE']:
            print(f"🔄 Restaurando: {key} (StorageClass: {storage_class})")
            try:
                s3_client.restore_object(
                    Bucket=bucket,
                    Key=key,
                    RestoreRequest={
                        'Days': dias_disponibilidade,
                        'GlacierJobParameters': {'Tier': tier_recuperacao}
                    }
                )
            except Exception as e:
                print(f"⚠️ Erro ao restaurar {key}: {e}")
        else:
            print(f"✅ Ignorado: {key} (StorageClass: {storage_class})")
print("Restauração concluida")