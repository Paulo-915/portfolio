"""
Lambda 2: Verifica e inicia cluster EMR com base em evento recebido

Esta função Lambda é acionada por um evento do SQS (via evento anterior do S3).
Sua principal responsabilidade é verificar se já existe um cluster EMR com o nome desejado.
Se existir e estiver em execução, apenas imprime o status. Caso contrário, cria um novo cluster
com as configurações padrão definidas via variáveis de ambiente e envia uma mensagem para a próxima
etapa da orquestração via SQS.

📌 Funcionalidades principais:
- Processa eventos recebidos via SQS (formato JSON).
- Extrai o nome do cluster (`cluster_name`) e parâmetros adicionais (`bucket`, `key`).
- Verifica se há clusters EMR em execução com o mesmo nome.
- Caso não haja, cria um novo cluster EMR com:
    • AMI customizada (opcional)
    • Instâncias Master, Core e Task (OnDemand + Spot)
    • Bootstrap scripts (instalação de libs e setup de jobs)
    • Configuração para integração com Glue, Hive, Spark e Iceberg
    • AutoScaling de instâncias (opcional)
    • Tags e configurações de segurança definidas via ambiente
- Após criação, envia mensagem SQS para a fila de controle do próximo step (`sqs_queue_url_subindo_cluster`).

⚙️ Requisitos de ambiente:
- Variáveis obrigatórias como `emr_role`, `emr_ec2_role`, `ec2_keypair`, `s3_log_uri`, `s3_bootstrap_bucket`, entre outras.
- A fila de destino (`sqs_queue_url_subindo_cluster`) deve estar configurada corretamente.

🎯 Fila de entrada:
- Espera mensagens no formato:
  {
    "cluster_name": "nome_do_cluster",
    "bucket": "nome-do-bucket-s3",
    "key": "caminho/do/arquivo"
  }

📤 Fila de saída:
- Envia mensagem para: `sqs_queue_url_subindo_cluster`

🔒 Segurança:
- A função assume que possui permissões para:
    • Criar clusters EMR
    • Ler variáveis de ambiente
    • Enviar mensagens ao SQS
    • Executar ações no CloudWatch (se aplicável)
"""

from __future__ import print_function
import logging
import os
import boto3
from boto3.dynamodb.conditions import Attr
import json
import time
from datetime import datetime

# =========================
# Variáveis de ambiente
# =========================
sns_topic_arn = os.getenv('sns_topic_arn')
s3_bootstrap_bucket = os.getenv('s3_bootstrap_bucket')
s3_log_uri = os.getenv('s3_log_uri')
ec2_keypair = os.getenv('ec2_keypair')
ec2_subnet_id = os.getenv('ec2_subnet_id')
emr_release = os.getenv('emr_release')
emr_role = os.getenv('emr_role')
emr_ec2_role = os.getenv('emr_ec2_role')
emr_custom_ami = os.getenv('emr_custom_ami')
emr_custom_ami_id = os.getenv('emr_custom_ami_id')
instance_type_master = os.getenv('instance_type_master')
instance_type_core = os.getenv('instance_type_core')
environment = os.getenv('environment')
security_conf = os.getenv('security_conf')
argumentos_ec2 = os.getenv('argumentos_ec2')

targetSpotCapacity = int(os.getenv('targetSpotCapacity', '5'))  # valor padrão de 5

timeout_cluster = float(os.getenv('timeout_cluster', '0.5'))  # 0.5 hora = 30 minutos
idle_timeout_seconds = int(timeout_cluster * 60 * 60)


task_types = ['r5.xlarge', 'm5.2xlarge', 'r5a.xlarge', 'r5d.xlarge']

sqs_queue_url_subindo_cluster = os.getenv('sqs_queue_url_subindo_cluster')

region_name = os.getenv('region_name')

# =========================
# Sessão e clientes AWS
# =========================
# Criar uma sessão do Boto3
session = boto3.session.Session()

# Criar clientes com verify=True para evitar avisos de HTTPS
emr_client = session.client('emr', region_name=region_name, verify=True)
cloudwatch_client = session.client('cloudwatch', region_name=region_name, verify=False)
sqs_client = session.client('sqs', region_name=region_name, verify=False)
sns_client = session.client('sns', region_name=region_name, verify=False)

# =========================
# Logger
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

emr_scaling = False

# Do not modify below this line, except for job_flow
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Lambda Function {}'.format(__name__))


def create_cluster(cluster_name_event,area_solicitante,squad_pertencente,tipo_processo):
    """
    Cria um novo cluster EMR com as configurações definidas por variáveis de ambiente.

    Parâmetros:
    - cluster_name_event (str): Nome do cluster a ser criado.

    Retorna:
    - dict: Resposta da API do EMR com os detalhes do cluster criado.
    """
    logger.info('There is no Cluster created to execute the jobs')
    logger.info('We are going to create a new one to run the jobs.')
    
    # JSON
    args = {
        "Name": cluster_name_event,
        "LogUri": f"s3://{s3_log_uri}",
        "ReleaseLabel": emr_release,
    }
    
    if emr_custom_ami:
        args.update({
            "CustomAmiId": emr_custom_ami_id
        })
        
    args.update({
        "Instances": {
            "InstanceFleets": [
                {
                    'Name': 'Master instance fleet',
                    'InstanceFleetType': 'MASTER',
                    'TargetOnDemandCapacity': 1,
                    'TargetSpotCapacity': 0,
                    'InstanceTypeConfigs': [
                        {
                            'InstanceType': str(instance_type_master),
                            'WeightedCapacity': 1
                        }
                    ]
                },
                {
                    'Name': 'Core instance fleet',
                    'InstanceFleetType': 'CORE',
                    'TargetOnDemandCapacity': 1,
                    'TargetSpotCapacity': 0,
                    'InstanceTypeConfigs': [
                        {
                            'InstanceType': str(instance_type_core),
                            'WeightedCapacity': 1,
                            "EbsConfiguration": {
                                "EbsOptimized": True
                            }
                        }
                    ]
                },
                {
                    'Name': 'Task instance fleet',
                    'InstanceFleetType': 'TASK',
                    'TargetOnDemandCapacity': 0,
                    'TargetSpotCapacity': targetSpotCapacity,
                    'InstanceTypeConfigs': []
                }
            ],
            "Ec2KeyName": ec2_keypair,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2SubnetId": ec2_subnet_id
        },
        "BootstrapActions": [
            {
                'Name': 'Install Libs and Bootstrap Scripts',
                'ScriptBootstrapAction': {
                    #'Path': f's3://{s3_bootstrap_bucket}/dataops/airflow/setup/install_libs.sh',
                    'Path': f's3://{argumentos_ec2}/scripts/setup/install_libs.sh',
                    'Args': [f'{argumentos_ec2}']
                }
            },
            {
                'Name': 'Boostrap Setup_jobs',
                'ScriptBootstrapAction': {
                    #'Path': f's3://{s3_bootstrap_bucket}/dataops/airflow/setup/setup_jobs.sh',
                    'Path': f's3://{argumentos_ec2}/scripts/setup/setup_jobs_s3.sh',
                    'Args': [f'{argumentos_ec2}']
                }
            }
        ],
        "StepConcurrencyLevel": 10,
        "Applications": [
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Ganglia'},
            {'Name': 'HCatalog'},
            {'Name': 'Spark'}
        ],
        "Configurations": [
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            },
            {
                "Classification": "yarn-site",
                "Properties": {
                    "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler",
                    "yarn.scheduler.fair.user-as-default-queue": "false",
                    "yarn.scheduler.fair.sizebasedweight": "true",
                    "yarn.scheduler.fair.allow-undeclared-pools": "false",
                    "yarn.scheduler.fair.preemption": "true"
                }
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.locality.wait": "2s",
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.dynamicAllocation.executorIdleTimeout": "20s",
                    "spark.dynamicAllocation.cachedExecutorIdleTimeout": "20s",
                    "spark.dynamicAllocation.schedulerBacklogTimeout": "20s",
                    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout": "20s",
                    "spark.shuffle.service.enabled": "true",
                    "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
                    "spark.sql.catalog.AwsDataCatalog.warehouse": "s3://alelo-datalake-raw-dev/iceberg/",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.AwsDataCatalog": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.AwsDataCatalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                    "spark.sql.catalog.AwsDataCatalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "iceberg-defaults",
                "Properties": {
                    "iceberg.enabled": "true"
                }
            }
        ],
        "VisibleToAllUsers": True,
        "JobFlowRole": emr_ec2_role,
        "ServiceRole": emr_role,
        "Tags": [
            {'Key': 'Role', 'Value': 'EMR Data Lake'},
            {'Key': 'environment', 'Value': environment},
            {'Key': 'Label', 'Value': cluster_name_event},
            {'Key': 'Name', 'Value': cluster_name_event},
            {'Key': 'business_unit', 'Value': 'datalake'},
            {'Key': 'ambiente', 'Value': environment},
            {'Key': 'cc', 'Value': '91050101'},
            {'Key': 'area', 'Value': 'CID'},
            {'Key': 'trem', 'Value': 'dados'},
            {'Key': 'projeto', 'Value': 'datalake'},
            {'Key': 'repo', 'Value': 'False'},
            {'Key': 'iac', 'Value': 'False'},
            {'Key': 'os_family', 'Value': 'linu'},
            {'Key': 'area_solicitante', 'Value': area_solicitante if area_solicitante else ''},
            {'Key': 'squad_pertencente', 'Value': squad_pertencente if squad_pertencente else ''},
            {'Key': 'tipo_processo', 'Value': tipo_processo if tipo_processo else ''}

        ],
        "SecurityConfiguration": security_conf,
        "AutoTerminationPolicy": {
            'IdleTimeout': idle_timeout_seconds
        }
    })

    #INSTANCE_COUNT_CORE_NODE = 1
    #INSTANCE_COUNT_TASK_NODE = 1

    core_scaling = 3
    task_scaling = 12

    if emr_scaling:
        args['ManagedScalingPolicy'] = {
            'ComputeLimits': {
                'UnitType': "InstanceFleetUnits",
                'MinimumCapacityUnits': core_scaling,           # total mínimo (core + 3 task)
                'MaximumCapacityUnits': core_scaling + task_scaling,
                'MaximumOnDemandCapacityUnits': 1,
                'MaximumCoreCapacityUnits': 1
            }
        }

    
    for type in task_types:
                args['Instances']['InstanceFleets'][2]['InstanceTypeConfigs'].append(
                        {
                            'InstanceType': type,
                            'WeightedCapacity': 1,
                            "EbsConfiguration": {
                                "EbsOptimized": True
                            }
                        }
                )

    # Create new EMR cluster
    emr_launch_message = f'Launching new EMR cluster: {cluster_name_event}'
    logger.info(emr_launch_message)
    #send_notification(
    #    sns_arn=sns_topic_arn,
    #    subject=f'Datalake:{environment} Create EMR Cluster message',
    #    message=emr_launch_message
    #)

    try:
        response = emr_client.run_job_flow(**args)
        return response
    except Exception as e:
        logger.error(f"RunJobFlow Exception: {e}")
        #send_notification(
        #    sns_arn=sns_topic_arn,
        #    subject=f'Datalake:{environment} Create EMR Cluster Error',
        #    message=f'Lambda Create EMR Cluster Error\nError message: {e}'
        #)
        raise e

# =========================
# Logger
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def print_status_cluster(cluster_name_event):
    """
    Verifica se há um cluster EMR com o nome fornecido em estado ativo e imprime seu status.

    Parâmetros:
    - cluster_name_event (str): Nome do cluster a ser verificado.
    """
    emr_client = boto3.client('emr')
    # Busca todos os clusters em estados ativos
    clusters = emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
    cluster_id = None
    status = None

    for c in clusters['Clusters']:
        if c['Name'] == cluster_name_event:
            cluster_id = c['Id']
            break

    if cluster_id:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        status = response['Cluster']['Status']['State']
        print(f"⚠️ O cluster '{cluster_name_event}' está atualmente com status: {status}.")
    else:
        print(f"❌ Cluster '{cluster_name_event}' não encontrado.")


def envia_mensagem_sqs(url_fila_sqs, payload):
    """
    Envia uma mensagem JSON para uma fila SQS especificada.
    
    Parâmetros:
    - url_fila_sqs (str): URL da fila SQS de destino.
    - payload (dict): Dicionário que será convertido em JSON como corpo da mensagem.
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
    Função principal da Lambda acionada por eventos do SQS ou diretamente.

    Processa o evento recebido, verifica se o cluster já está em execução, e inicia a criação de cluster EMR se necessário.
    Após isso, envia uma mensagem para a próxima etapa via SQS.

    Parâmetros:
    - event (dict): Evento recebido (pode vir do SQS ou diretamente).
    - context (object): Informações de contexto da execução Lambda.

    Retorna:
    - str: Mensagem indicando o status da execução.
    """
    try:
        # Evento vindo do SQS
        if 'Records' in event and 'body' in event['Records'][0]:
            print("🔄 Evento recebido via SQS.")
            print(event)
            body_str = event['Records'][0]['body']
            body = json.loads(body_str)
        # Evento com chave 'Body'
        elif 'Body' in event:
            print("⚙️ Evento recebido com chave 'Body'.")
            body = json.loads(event['Body'])
            print(event)
        # Evento manual (direto)
        else:
            print("⚙️ Evento recebido diretamente (trigger ou execução manual).")
            body = event

        cluster_name_event = body['cluster_name']
        print(f"cluster_name_event: {cluster_name_event}")

        area_solicitante = body['area_solicitante']
        print(f"area_solicitante: {area_solicitante}")

        squad_pertencente = body['squad_pertencente']
        print(f"squad_pertencente: {squad_pertencente}")

        tipo_processo = body['tipo_processo']
        print(f"tipo_processo: {tipo_processo}")

        bucket = body.get('bucket')
        key = body.get('key')

    except Exception as e:
        print(f"❌ Erro ao extrair o : {str(e)}")
        raise

    clusters = list()

    paginator = emr_client.get_paginator('list_clusters')

    page_iterator = paginator.paginate(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])

    for page in page_iterator:
        if page['Clusters']:
            clusters.append(page['Clusters'])

    cluster_list = []
    for cluster in clusters:
        for name in cluster:
            cluster_list.append(name['Name'])

    # Reorganizando a lógica
    if cluster_name_event in cluster_list:
        return print_status_cluster(cluster_name_event)
    
    # Caso nenhum cluster esteja rodando e não seja um cluster conhecido'
    print(f"❌ Nenhum cluster encontrado")
    print(f"Submetendo um novo cluster: '{cluster_name_event}' para execução.")
    create_cluster(cluster_name_event,area_solicitante,squad_pertencente,tipo_processo)

    print("Enviando mensagem para Lambda_3 - (subindo-cluster-emr) via SQS...")
    
    print(" Criando o payload: ")
    # Define o payload da mensagem
    payload = {
        "cluster_name": cluster_name_event,
        "bucket": bucket,
        "key": key
    }

    # Função que envia para a fila SQS
    envia_mensagem_sqs(sqs_queue_url_subindo_cluster, payload)

    return f"✅ Finalizado: Novo cluster {cluster_name_event} foi submetido para criação com sucesso. Aguardando o início."