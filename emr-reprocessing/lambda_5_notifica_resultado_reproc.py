import json
import urllib3
import re
from datetime import datetime, timedelta
import os
import boto3
import logging
import time
from zoneinfo import ZoneInfo

# Variáveis de ambiente
webhook_url = os.getenv('webhook_url')

region_name = os.getenv('region_name')
database = os.getenv('database')
table = os.getenv('table')
outputLocation_s3 = os.getenv('outputLocation_s3')

# Criar uma sessão do Boto3
session = boto3.session.Session()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicialização da sessão Boto3 (com verify=False para contornar problemas de SSL)
emr_client = session.client('emr', region_name=region_name, verify=False)
s3_client = session.client('s3', region_name=region_name, verify=True)
athena_client = session.client('athena', region_name=region_name, verify=True)


# Configuração do webhook
http = urllib3.PoolManager()

def envia_notificacao(mensagem, tentativas=2, delay=2):
    headers = {'Content-Type': 'application/json'}
    payload = {'text': mensagem}
    for tentativa in range(1, tentativas + 1):
        try:
            resp = http.request(
                "POST",
                webhook_url,
                body=json.dumps(payload),
                headers=headers
            )
            print({
                "message": mensagem,
                "status_code": resp.status,
                "response": resp.data.decode('utf-8'),
            })
            return
        except Exception as e:
            print(f"Tentativa {tentativa} de envio de notificação falhou: {e}")
            if tentativa < tentativas:
                time.sleep(delay)
    print("❌ Todas as tentativas de envio de notificação falharam.")


def extrai_nome_cluster(mensagem):
    """
    Extrai o nome do cluster a partir da mensagem do evento.
    """
    match = re.search(r'cluster (.+?) \(', mensagem)
    return match.group(1) if match else "Nome do Cluster Não Identificado"

def formata_mensagem(cluster_name, cluster_id, step_name, step_id, step_state, event_time):
    """
    Formata a mensagem de notificação com base no estado do step.
    """
    # Link para o cluster no console da AWS
    cluster_link = f"https://sa-east-1.console.aws.amazon.com/emr/home?region=sa-east-1#/clusterDetails/{cluster_id}"

    # Formata a data/hora do evento e ajusta para UTC-3
    data_hora_alerta = datetime.strptime(event_time, '%Y-%m-%dT%H:%M:%SZ') - timedelta(hours=3)
    data_hora_alerta_str = data_hora_alerta.strftime('%Y-%m-%d %H:%M:%S')

    if step_state == "COMPLETED":
        mensagem = (
            f"📢✅ **Sucesso!** ✅\n\n" 
            f"**{step_name}** no cluster **{cluster_name}** foi concluído com sucesso. 😃😎\n\n"
            f"🆔 **ID do Cluster:** {cluster_id}\n\n"
            f"🆔 **ID do Step:** {step_id}\n\n"
            f"🔗 **Link do Cluster:** [Acesse o cluster no console da AWS]({cluster_link})\n\n"
            f"🕒 **Data/Hora do Evento:** {data_hora_alerta_str}"
        )
    elif step_state == "CANCELLED":
        mensagem = (
            f"📢🚨❌ **Cancelado!** ❌🚨\n\n"
            f"**{step_name}** no cluster **{cluster_name}** foi cancelado. 😔😔\n\n"
            f"🆔 **ID do Cluster:** {cluster_id}\n\n"
            f"🆔 **ID do Step:** {step_id}\n\n"
            f"🔗 **Link do Cluster:** [Acesse o cluster no console da AWS]({cluster_link})\n\n"
            f"🕒 **Data/Hora do Evento:** {data_hora_alerta_str}"
        )
    elif step_state == "FAILED":
        mensagem = (
            f"📢❌ **Falha!** ❌\n\n"
            f"**{step_name}** no cluster **{cluster_name}** finalizou com falha. 😢😢\n\n"
            f"🆔 **ID do Cluster:** {cluster_id}\n\n"
            f"🆔 **ID do Step:** {step_id}\n\n"
            f"🔗 **Link do Cluster:** [Acesse o cluster no console da AWS]({cluster_link})\n\n"
            f"🕒 **Data/Hora do Evento:** {data_hora_alerta_str}"
        )
    else:
        mensagem = (
            f"⚠️ **Estado não monitorado:** **{step_name}** no cluster **{cluster_name}** está no estado **{step_state}**.\n\n"
            f"🆔 **ID do Cluster:** {cluster_id}\n\n"
            f"🆔 **ID do Step:** {step_id}\n\n"
            f"🔗 **Link do Cluster:** [Acesse o cluster no console da AWS]({cluster_link})\n\n"
            f"🕒 **Data/Hora do Evento:** {data_hora_alerta_str}"
        )

    return mensagem


def atualiza_table(event, athena_client, database, table, outputLocation_s3):
    """
    Atualiza o registro do step na tabela de monitoramento do Athena, apenas se o registro existir.
    Retorna True se o UPDATE foi executado, False caso contrário.
    """
    print(f"Preparando para realizar o update nos dados na tabela {database}.{table} no Athena")
    print(event)

    detail = event.get("detail", {})
    state = detail.get("state")
    step_id = detail.get("stepId")
    message = detail.get("message", "")

    # Inicializa variáveis
    start_time_step = None
    end_time_step = None
    tempo_execucao = None

    # Expressões regulares para diferentes estados
    start_time_match = re.search(r"started running at ([\d\-: ]+) UTC", message)
    end_time_match = re.search(r"(completed execution|at) ([\d\-: ]+) UTC", message)
    tempo_execucao_match = re.search(r"took (\d+) minutes", message)

    # Conversão e atribuição
    if start_time_match:
        start_time_step = start_time_match.group(1)
    if end_time_match:
        end_time_step = end_time_match.group(2)
    if tempo_execucao_match:
        total_minutes = int(tempo_execucao_match.group(1))
        horas = total_minutes // 60
        minutos = total_minutes % 60
        tempo_execucao = f"{horas}h {minutos}min" if horas else f"{minutos}min"

    # Parse para datetime com timezone UTC
    start_time_utc = datetime.strptime(start_time_step, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("UTC")) if start_time_step else None
    end_time_utc = datetime.strptime(end_time_step, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("UTC")) if end_time_step else None

    # Converter para America/Sao_Paulo
    start_time_step = start_time_utc.astimezone(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S") if start_time_utc else None
    end_time_step = end_time_utc.astimezone(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S") if end_time_utc else None

    dt_update = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d %H:%M:%S')

    # Antes do UPDATE, verifica se o registro existe
    select_query = f"SELECT COUNT(*) as total FROM {database}.{table} WHERE id_solicitacao = '{step_id}'"
    print(f"Query Athena (verificação antes do update):\n{select_query}")

    try:
        response = athena_client.start_query_execution(
            QueryString=select_query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": outputLocation_s3}
        )
        query_execution_id = response['QueryExecutionId']

        # Espera a execução do SELECT
        timeout = 60
        start_time = time.time()
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            if time.time() - start_time > timeout:
                print("Tempo limite excedido ao aguardar o SELECT.")
                return False
            time.sleep(2)

        if status == 'SUCCEEDED':
            result_output = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            total = int(result_output['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
            print(f"Registros encontrados para atualizar: {total}")

            if total == 0:
                print("Nenhum registro encontrado para atualizar. O UPDATE não será executado.")
                return False
            else:
                print("Registros encontrados. O UPDATE será executado.")
        else:
            print("Não foi possível verificar se o registro existe antes do update.")
            return False

        # Monta a query de UPDATE
        query = f"""
            UPDATE {database}.{table}
            SET 
                step_status = '{state}',
                start_time_step = {f"TIMESTAMP '{start_time_step}'" if start_time_step else "NULL"},
                end_time_step = {f"TIMESTAMP '{end_time_step}'" if end_time_step else "NULL"},
                tempo_execucao = {f"'{tempo_execucao}'" if tempo_execucao else "NULL"},
                dt_update = TIMESTAMP '{dt_update}'
            WHERE 
                id_solicitacao = '{step_id}'
        """
        print(f"Query Athena:\n{query}")
        print(f"Output S3: {outputLocation_s3}")

        # Execução da query de UPDATE no Athena
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": outputLocation_s3}
        )
        query_execution_id = response['QueryExecutionId']
        print(f"Query Athena submetida. Execution ID: {query_execution_id}")

        # Aguardar execução
        timeout = 200
        start_time = time.time()
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

            if time.time() - start_time > timeout:
                print("Tempo limite excedido ao aguardar a execução da query.")
                return False

            time.sleep(2)

        if status == 'SUCCEEDED':
            print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
            return True
        elif status == 'FAILED':
            reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Motivo não informado')
            print(f"❌ ERRO NA EXECUÇÃO DA QUERY: {reason}")
            return False
        else:
            print(f"⚠️ Query foi cancelada ou não concluída. Status final: {status}")
            return False

    except Exception as e:
        print(f"❌ Erro ao executar a query no Athena: {str(e)}")
        return False


def lambda_handler(event, context):
    """
    Função Lambda para processar eventos do EMR e enviar notificações.
    """
    try:
        # Exibe o evento recebido
        print("Evento recebido:")
        print(event)
        #print(f"Evento recebido: {json.dumps(event, indent=2)}")

        # Extraia informações do evento
        message = event['detail']['message']
        step_name = event['detail']['name']

        # Verifica se o nome do cluster contém "Data_Lake-REPROC-EMR"
        if "Data_Lake-REPROC-EMR" not in message:
            print("Cluster não é 'Data_Lake-REPROC-EMR'. Encerrando o Lambda.")
            return  # Encerra o Lambda

        # Verifica se o nome do step começa com "Carga_Historica" ou "Reprocessamento"
        if not (step_name.startswith("Carga_Historica") or step_name.startswith("Reprocessamento")):
            print("Step não é 'Carga_Historica' ou 'Reprocessamento'. Encerrando o Lambda.")
            return  # Encerra o Lambda


        # Extraia outras informações do evento
        cluster_id = event['detail']['clusterId']
        step_id = event['detail']['stepId']
        step_state = event['detail']['state']
        event_time = event['time']

        # Extrai o nome do cluster da mensagem
        cluster_name = extrai_nome_cluster(message)

        # Verifica o estado do step e executa ações na ordem correta
        if step_state in ["COMPLETED", "CANCELLED", "FAILED"]:
            print("Atualizando tabela no Athena...")
            atualizado_tabela = atualiza_table(event, athena_client, database, table, outputLocation_s3)
        
            if atualizado_tabela:
                print("Tabela atualizada com sucesso. Enviando notificação...")
                mensagem = formata_mensagem(cluster_name, cluster_id, step_name, step_id, step_state, event_time)
                envia_notificacao(mensagem)
            else:
                print("Erro ao atualizar tabela. Notificação não será enviada.")

        else:
            print(f"Estado do step não monitorado: {step_state}")
    except Exception as e:
        # Trata outros erros genéricos
        mensagem_erro = (
            f"⚠️ **Erro inesperado ao processar o evento do EMR.**\n\n"
            f"Detalhes do erro: {str(e)}\n"
            f"Evento recebido: {json.dumps(event, indent=2)}"
        )
        print(mensagem_erro)
        envia_notificacao(mensagem_erro)