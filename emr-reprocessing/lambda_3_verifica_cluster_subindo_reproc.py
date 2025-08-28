"""
Lambda 3: Verifica o estado do cluster EMR com base no nome prefixado e evento recebido do S3
ou mensagem sqs recebida.
Se o cluster estiver ativo (RUNNING ou WAITING), envia uma mensagem para adicionar um step.
Caso contrário, aguarda o cluster subir antes de prosseguir.

📌 Funcionalidades principais:
- Processa eventos de upload de arquivos JSON no S3.
- Valida e extrai parâmetros obrigatórios do evento recebido.
- Verifica se o cluster EMR já está disponível e ativo.
- Caso o cluster ainda esteja subindo, aguarda até estar pronto.
- Ao final, envia mensagem para a fila que aciona a Lambda responsável por adicionar o step no cluster.

⚙️ Requisitos de ambiente:
- Variável `sqs_queue_url_fila_cluster_add_step_emr_ec2` contendo a URL da fila SQS de destino.
- Permissões para listar e descrever clusters EMR.
- Permissões para enviar mensagens ao SQS.

🎯 Fila de entrada:
- Espera mensagens no formato:
  {
    "cluster_name": "nome_do_cluster",
    "bucket": "nome-do-bucket-s3",
    "key": "caminho/do/arquivo"
  }

📤 Fila de saída:
- Envia mensagem para: `sqs_queue_url_fila_cluster_add_step_emr_ec2`
"""

import logging
import os
import boto3
import json
import time
from datetime import datetime

# =========================
# Variáveis de ambiente
# =========================
sqs_queue_url_fila_cluster_add_step_emr_ec2 = os.getenv('sqs_queue_url_fila_cluster_add_step_emr_ec2')
region_name = os.getenv('region_name')

# =========================
# Sessão e clientes AWS
# =========================
session = boto3.session.Session()
emr_client = session.client('emr', region_name=region_name, verify=False)
sqs_client = session.client('sqs', region_name=region_name, verify=False)

# =========================
# Logger
# =========================
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def envia_mensagem_sqs(url_fila_sqs, payload):
    """
    Envia uma mensagem JSON para a fila SQS especificada.

    Parâmetros:
    - url_fila_sqs (str): URL da fila SQS.
    - payload (dict): Conteúdo da mensagem a ser enviada.
    """
    nome_fila = url_fila_sqs.split('/')[-1]
    print(f"📤 Enviando mensagem para a fila: {nome_fila}")
    print(f"📝 Payload:\n{json.dumps(payload, indent=2)}")

    resposta = sqs_client.send_message(
        QueueUrl=url_fila_sqs,
        MessageBody=json.dumps(payload)
    )

    message_id = resposta.get("MessageId")
    print(f"✅ Mensagem enviada com sucesso! MessageId: {message_id}")


def get_cluster_id_by_name(cluster_name):
    """
    Busca o ID de um cluster EMR ativo a partir do seu nome.

    Parâmetros:
    - cluster_name (str): Nome do cluster.

    Retorna:
    - str ou None: ID do cluster se encontrado, caso contrário None.
    """
    emr_client = boto3.client('emr')
    clusters = emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
    for c in clusters['Clusters']:
        if c['Name'] == cluster_name:
            return c['Id']
    return None


def aguardar_cluster_subir(cluster_id, cluster_name_event):
    """
    Aguarda até que o cluster esteja em estado RUNNING ou WAITING.

    Parâmetros:
    - cluster_id (str): ID do cluster EMR.
    - cluster_name_event (str): Nome do cluster para exibição.
    """
    emr_client = boto3.client('emr')
    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        status = response['Cluster']['Status']['State']
        print(f"Status atual do cluster '{cluster_name_event}': {status}")
        if status in ['RUNNING', 'WAITING']:
            print("✅ Cluster está pronto!")
            break
        elif status in ['TERMINATING','TERMINATED', 'TERMINATED_WITH_ERRORS']:
            print("❌ Cluster terminou com erro ou foi finalizado.")
            break
        else:
            print("⏳ Cluster ainda está iniciando. Aguardando 30 segundos...")
            time.sleep(30)


def lambda_handler(event, context):
    """
    Função principal da Lambda. Processa evento do SQS, verifica o estado do cluster
    e, caso esteja pronto, envia mensagem para fila de steps.

    Parâmetros:
    - event (dict): Evento recebido pela Lambda (normalmente do SQS).
    - context (LambdaContext): Contexto de execução da Lambda.

    Retorna:
    - None
    """
    try:
        if 'Records' in event and 'body' in event['Records'][0]:
            print("🔄 Evento recebido via SQS.")
            body = json.loads(event['Records'][0]['body'])
            print(event)
        elif 'Body' in event:
            print("⚙️ Evento recebido com chave 'Body'.")
            body = json.loads(event['Body'])
            print(event)
        else:
            print("⚙️ Evento recebido diretamente (trigger manual).")
            body = event

        cluster_name_event = body['cluster_name']
        bucket = body.get('bucket')
        key = body.get('key')

    except Exception as e:
        print(f"❌ Erro ao processar evento: {str(e)}")
        raise

    cluster_id = get_cluster_id_by_name(cluster_name_event)
    if not cluster_id:
        print(f"❌ Cluster '{cluster_name_event}' não encontrado ou não está ativo.")
        return

    aguardar_cluster_subir(cluster_id, cluster_name_event)

    paginator = emr_client.get_paginator('list_clusters')
    response_iterator = paginator.paginate(ClusterStates=['RUNNING', 'WAITING'])
    for page in response_iterator:
        for cluster in page['Clusters']:
            if cluster_name_event in cluster['Name']:
                state = cluster['Status']['State']
                cluster_id = cluster['Id']
                print(f"✅ Cluster encontrado: {cluster['Name']} ({cluster_id}) - Estado: {state}")

                if state in ['RUNNING', 'WAITING']:
                    payload = {
                        "cluster_name": cluster_name_event,
                        "bucket": bucket,
                        "key": key
                    }
                    envia_mensagem_sqs(sqs_queue_url_fila_cluster_add_step_emr_ec2, payload)
    else:
        aguardar_cluster_subir(cluster_id, cluster_name_event)