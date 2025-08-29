# -*- coding: utf-8 -*-
import boto3
import sys
import certifi
import urllib3
import os

# Oculta os avisos de SSL por uso de verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

region_name = os.getenv('REGION_NAME', 'sa-east-1')
session = boto3.session.Session()
s3_client = session.client('s3', region_name=region_name, verify=False)

# Defina as variáveis diretamente aqui
bucket_name = "alelo-mwaa-datalake-dev"
prefix = "base/dags/dto/conf_dag/"

#alelo-mwaa-datalake-dev base/dags/dto/conf_dag/

##if len(sys.argv) != 3:
##    print("Uso: python recupera_arquivos_pasta_s3.py <bucket> <diretorio>")
##    print("Exemplo: python recupera_arquivos_pasta_s3.py alelo-datalake-tz-in-prd SDDCAR/SDD_report_PAT/")
##    sys.exit(1)
##
##bucket_name = sys.argv[1]
##prefix = sys.argv[2]

print(f"Bucket: {bucket_name}")
print(f"Prefixo (diretório): {prefix}")

delete_markers_to_remove = []
continuation_token = None

while True:
    if continuation_token:
        response = s3_client.list_object_versions(
            Bucket=bucket_name,
            Prefix=prefix,
            ContinuationToken=continuation_token
        )
    else:
        response = s3_client.list_object_versions(
            Bucket=bucket_name,
            Prefix=prefix
        )

    # Coleta todos os delete markers encontrados
    if 'DeleteMarkers' in response:
        for marker in response['DeleteMarkers']:
            print(f"Marcador de exclusão encontrado: {marker['Key']} - VersionId: {marker['VersionId']}")
            delete_markers_to_remove.append({'Key': marker['Key'], 'VersionId': marker['VersionId']})

    # Verifica se há mais páginas de resultados
    if 'NextContinuationToken' in response:
        continuation_token = response['NextContinuationToken']
    else:
        break

if not delete_markers_to_remove:
    print("Nenhum marcador de exclusão encontrado para o prefixo informado.")
    sys.exit(0)

# Remove todos os delete markers encontrados
for marker in delete_markers_to_remove:
    s3_client.delete_object(
        Bucket=bucket_name,
        Key=marker['Key'],
        VersionId=marker['VersionId']
    )
    print(f"Marcador de exclusão removido: {marker['Key']} - VersionId: {marker['VersionId']}")

print("Processo de recuperação concluído para o diretório informado.")