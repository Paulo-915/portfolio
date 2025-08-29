import datetime
import pandas as pd
import os
import certifi
from collections import defaultdict
import urllib3
import boto3
import json 

# Oculta os avisos de SSL por uso de verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurar certificado SSL corretamente para evitar warnings
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

region_name = os.getenv('REGION_NAME', 'sa-east-1')

# Inicialização da sessão Boto3 (com verify=False para contornar problemas de SSL)
session = boto3.session.Session()
emr_client = session.client('emr', region_name=region_name, verify=False)
cloudwatch_client = session.client('cloudwatch', region_name=region_name, verify=False)
sqs_client = session.client('sqs', region_name=region_name, verify=False)
glue_client = session.client('glue', region_name=region_name, verify=False)

# Nome da conexão
connection_name = 'pec-bko-prd'

# Obter detalhes da conexão
response = glue_client.get_connection(Name=connection_name)

# Imprimir tudo formatado
print(json.dumps(response['Connection'], indent=4, default=str))
