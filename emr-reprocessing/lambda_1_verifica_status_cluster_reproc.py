"""
Lambda 1: Verifica o estado do cluster EMR com base no nome prefixado e evento recebido do S3.
Se o cluster estiver ativo (RUNNING ou WAITING), envia uma mensagem para adicionar um step.
Caso contrário, envia uma mensagem para iniciar o cluster.

Responsabilidades principais:
- Processar eventos de upload de arquivos JSON no S3.
- Validar e extrair parâmetros obrigatórios do arquivo JSON.
- Verificar se o cluster EMR já está disponível.
- Direcionar a execução para a fila correta no SQS:
    - iniciar-cluster-emr → para criar o cluster.
    - fila_cluster_add_step_emr_ec2 → para adicionar um step ao cluster existente.
"""
import os
from collections import defaultdict
import urllib3
import boto3
import json 
import logging


# =========================
# Variáveis de ambiente
# =========================
region_name = os.getenv('region_name')
prefix_cluster_name = os.getenv('prefix_cluster_name')
sqs_queue_url_iniciar_cluster = os.getenv('sqs_queue_url_iniciar_cluster')
sqs_queue_url_fila_cluster_add_step_emr_ec2 = os.getenv('sqs_queue_url_fila_cluster_add_step_emr_ec2')


#sqs_queue_url_iniciar_cluster = "https://sqs.sa-east-1.amazonaws.com/110403322204/iniciar-cluster-emr"
#sqs_queue_url_fila_cluster_add_step_emr_ec2 = "https://sqs.sa-east-1.amazonaws.com/110403322204/fila_cluster_add_step_emr_ec2"
#region_name = os.getenv('REGION_NAME', 'sa-east-1')
#cluster_name = os.getenv('CLUSTER_NAME', 'Data_Lake-REPROC-EMR-TESTE')
#bucket_name = os.getenv('BUCKET_NAME', 'alelo-datalake-stage-prd')

# =========================
# Sessão e clientes AWS
# =========================
# Criar uma sessão do Boto3
session = boto3.session.Session()

# Criar clientes com verify=True para evitar avisos de HTTPS
s3_client = session.client('s3', region_name=region_name, verify=True)
emr_client = session.client('emr', region_name=region_name, verify=True)
cloudwatch_client = session.client('cloudwatch', region_name=region_name, verify=False)
sqs_client = session.client('sqs', region_name=region_name, verify=False)

# =========================
# Logger
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def arquivo_ja_processado(key):
    """
    Verifica se o arquivo já foi processado, ou seja, se existe na pasta de processados.
    Se já foi, apaga o arquivo da pasta 'geral' e encerra.

    Parâmetros:
    ----------
    key : str
        Caminho do arquivo original recebido no evento S3.

    Retorno:
    -------
    bool
        True se o arquivo já foi processado (e foi apagado da geral), False caso contrário.
    """
    bucket = 'alelo-datalake-reproc-prd'
    prefix_processados = 'reprocessamento/processados/'

    nome_arquivo = key.split('/')[-1]
    caminho_verificacao = prefix_processados + nome_arquivo

    print(f"Verificando existência do arquivo: {caminho_verificacao}")

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=caminho_verificacao,
        MaxKeys=1
    )

    if 'Contents' in response and len(response['Contents']) > 0:
        print(f"Arquivo já foi processado: {caminho_verificacao}")

        # Apagar o arquivo da pasta 'geral'
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"Arquivo removido da pasta geral: {key}")
        except Exception as e:
            print(f"Erro ao apagar o arquivo da pasta geral: {str(e)}")

        return True
    else:
        print(f"Arquivo ainda não foi processado: {caminho_verificacao}")
        return False



def envia_mensagem_sqs(url_fila_sqs, payload):
    """
    Envia uma mensagem para a fila SQS especificada.

    Parâmetros:
    ----------
    url_fila_sqs : str
        URL da fila SQS de destino.
    payload : dict
        Conteúdo a ser enviado na mensagem.

    Retorno:
    -------
    None
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


def lambda_handler(event, context):
    """
    Função principal da Lambda.

    1. Recebe eventos S3 com arquivos JSON.
    2. Valida o conteúdo e extrai os parâmetros obrigatórios.
    3. Monta o nome do cluster a partir do prefixo e dados do JSON.
    4. Verifica se o cluster EMR está em estado válido (RUNNING ou WAITING):
        - Se sim: envia payload para a fila SQS que adiciona um step no cluster.
        - Se não: envia payload para a fila SQS que inicializa o cluster.

    Parâmetros:
    ----------
    event : dict
        Evento recebido (geralmente de um trigger do S3).
    context : object, opcional
        Contexto de execução da Lambda (não utilizado diretamente aqui).

    Retorno:
    -------
    dict or str
        Retorno padrão da execução, com status HTTP ou mensagem de sucesso.
    """
    try:
        if 'Records' in event and 's3' in event['Records'][0]:
            print("################################################################")
            print("INICIO DO PROCESSAMENTO")
            print("################################################################")

            # Extrair informações do evento S3
            print("Processando evento S3...")
            print(event)
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            print(f"Bucket: {bucket}")
            print(f"Arquivo: {key}")

            # Baixar o arquivo JSON do S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read().decode('utf-8')

            print("Verificar se o arquivo está vazio...")
            # Verificar se o arquivo está vazio
            if not file_content.strip():
                print("Erro: O arquivo JSON está vazio.")
                return {
                    'statusCode': 400,
                    'body': json.dumps("Erro: O arquivo JSON está vazio.")
                }

            # Carregar o arquivo JSON
            try:
                event_data = json.loads(file_content)
            except json.JSONDecodeError as e:
                print(f"Erro ao carregar o arquivo JSON: {str(e)}")
                return {
                    'statusCode': 400,
                    'body': json.dumps(f"Erro ao carregar o arquivo JSON: {str(e)}")
                }
            
            print("Verificação se o arquivo já foi processado anteriormente...")
            if arquivo_ja_processado(key):
                print("Encerrando execução: arquivo já processado.")
                return  # ou exit(), ou qualquer lógica de encerramento
            else:
                print("Seguindo com o processamento do arquivo.")

            print("Carregando e mapeando parâmetros...")

            # Parâmetros obrigatórios
            parametros_obrigatorios = {
                "nome_solicitante": event_data.get("nome_solicitante"),
                "email_solicitante": event_data.get("email_solicitante"),
                "area_solicitante": event_data.get("area_solicitante"),
                "squad_pertencente": event_data.get("squad_pertencente"),
                "tipo_processo": event_data.get("tipo_processo"),
                "name_reproc": event_data.get("name_reproc"),
                "database_name_table_reproc": event_data.get("database_name_table_reproc"),
                "table_name_reproc": event_data.get("table_name_reproc"),
                "periodo_especifico": event_data.get("periodo_especifico"),
                #"process_servelles": event_data.get("process_servelles"),
                "tipo_frequencia": event_data.get("tipo_frequencia"),
                "s3_path_script_exec": event_data.get("s3_path_script_exec"),
                "path_json_exec": event_data.get("path_json_exec")
            }

            # Parâmetros opcionais (podem ser vazios)
            parametros_opcionais = {
                "lista_datas_especificas": event_data.get("lista_datas_especificas"),
                "data_inicio": event_data.get("data_inicio"),
                "data_fim": event_data.get("data_fim")
            }

            print("Validando campos obrigatórios...")

            # Verifica se há campos obrigatórios ausentes ou vazios
            faltando = [chave for chave, valor in parametros_obrigatorios.items() if not valor]
            if faltando:
                raise ValueError(f"⚠️ Parâmetros obrigatórios ausentes ou vazios no JSON: {', '.join(faltando)}")

            # Junta todos os parâmetros em um único dicionário, se necessário
            parametros = {**parametros_obrigatorios, **parametros_opcionais}

            # Atribuir variáveis para uso posterior
            area_solicitante = parametros['area_solicitante']
            squad_pertencente = parametros['squad_pertencente']
            tipo_processo = parametros['tipo_processo']
            #process_servelles = parametros['process_servelles']
            #periodo_especifico = parametros['periodo_especifico']
            #lista_datas_especificas = parametros['lista_datas_especificas']
            #s3_path_script_exec = parametros['s3_path_script_exec']
            #path_json_exec = parametros['path_json_exec']
            #name_reproc = parametros['name_reproc']
            #nome_solicitante = parametros['nome_solicitante']
            #tipo_frequencia = parametros['tipo_frequencia']
            #data_inicio = parametros['data_inicio']
            #data_fim = parametros['data_fim']

            # Printar parâmetros extraídos
            print("Parâmetros extraídos do arquivo JSON:")
            for chave, valor in parametros.items():
                print(f"{chave}: {valor}")
            print("################################################################")

            # Definir o nome do cluster com base no nome do reprocessamento
            cluster_name = f"{prefix_cluster_name}-{area_solicitante}-{tipo_processo}"

        else:
            #cluster_name = event['cluster_name']
            cluster_name = cluster_name
        
        print(f"Nome do clsuter definido: {cluster_name}")
        print("Iniciando verificação de clusters ativos...")
        paginator = emr_client.get_paginator('list_clusters')
        response_iterator = paginator.paginate(ClusterStates=['RUNNING', 'WAITING'])

        cluster_found = False
        for page in response_iterator:
            for cluster in page['Clusters']:
                if cluster_name == cluster['Name']:  # agora exige igualdade exata
                    state = cluster['Status']['State']
                    cluster_id = cluster['Id']
                    cluster_found = True
                    print(f"✅ Cluster encontrado: {cluster['Name']} ({cluster_id}) - Estado: {state}")

                    if state in ['RUNNING', 'WAITING']:
                        # Define o payload da mensagem
                        payload = {
                            "cluster_name": cluster_name,
                            "bucket": bucket,
                            "key": key
                        }

                        # Função que envia para a fila SQS
                        print("📦 Enviando mensagem para a fila SQS, para adicionar a STEP ao cluster:")
                        envia_mensagem_sqs(sqs_queue_url_fila_cluster_add_step_emr_ec2, payload)

                        print("\n")

                        print("################################################################")

                        return "✅ Finalizado: Cluster submetido para execução com sucesso."
                    else:
                        print("⏳ Cluster em processo de inicialização. Aguardando...")
                        return {"status": "Cluster já subindo. Nada a fazer agora."}

        if not cluster_found:
            print(f"🚀 Cluster '{cluster_name}' não encontrado. Enviando mensagem para Lambda_2 - (inicia_cluster_emr) via SQS...")

            print(" Criando o payload: ")
            # Define o payload da mensagem
            payload = {
                "cluster_name": cluster_name,
                "bucket": bucket,
                "key": key,
                "area_solicitante": area_solicitante,
                "squad_pertencente": squad_pertencente,
                "tipo_processo": tipo_processo
            }

            # Função que envia para a fila SQS
            print("📦 Enviando mensagem para a fila SQS, para fazer a chamada e criação do cluster:")
            
            envia_mensagem_sqs(sqs_queue_url_iniciar_cluster, payload)

            print("################################################################")

    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Erro durante o processamento: {str(e)}")
        }