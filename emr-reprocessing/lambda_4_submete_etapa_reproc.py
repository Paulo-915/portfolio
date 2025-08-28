"""
Lambda 4: Submete etapa de reprocessamento no EMR e registra monitoramento no Athena

Esta função Lambda é acionada por evento SQS (ou manualmente) e tem como objetivo:
- Baixar o JSON de parâmetros do S3.
- Validar e extrair todos os parâmetros necessários para o reprocessamento.
- Montar e submeter comandos Spark Submit para execução no EMR, usando um shell script dinâmico.
- Registrar o início do reprocessamento em uma tabela de monitoramento no Athena.

📌 Funcionalidades principais:
- Processa eventos recebidos via SQS, S3 ou execução manual.
- Extrai e valida parâmetros obrigatórios e opcionais do JSON.
- Monta o comando Spark Submit de acordo com os parâmetros recebidos.
- Copia o shell de reprocessamento do S3 para o cluster EMR.
- Submete dois steps ao EMR: (1) cópia do shell, (2) execução do shell com os parâmetros.
- Monitora o status dos steps submetidos.
- Insere um registro de monitoramento no Athena com todos os detalhes do reprocessamento.

⚙️ Requisitos de ambiente:
- Variáveis obrigatórias como `REGION_NAME`, `s3_studio`, `s3_raw`, `s3_stage`, `s3_analytcs`, `database_raw`, `database_stage`, `database_analtyics`, `environment`, `database`, `table`, `outputLocation_s3`.
- O shell de reprocessamento deve estar disponível no S3.
- O cluster EMR deve estar ativo e acessível.

🎯 Evento de entrada:
- Espera mensagens no formato:
  {
    "bucket": "nome-do-bucket-s3",
    "key": "caminho/do/arquivo",
    "cluster_name": "nome_do_cluster"
  }
  O arquivo JSON referenciado deve conter todos os parâmetros obrigatórios.

📤 Registro de saída:
- Insere um registro na tabela de monitoramento do Athena com todos os detalhes do reprocessamento.

🔒 Segurança:
- A função assume que possui permissões para:
    • Ler arquivos do S3
    • Submeter steps ao EMR
    • Executar queries no Athena
    • Ler variáveis de ambiente
"""

# LAMBDA 4: Submete etapa de reprocessamento no EMR e registra monitoramento no Athena
import os
import boto3
import json 
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

# Variáveis de ambiente
region_name = os.getenv('REGION_NAME')
s3_studio = os.getenv('s3_studio')
s3_raw = os.getenv('s3_raw')
s3_stage = os.getenv('s3_stage')
s3_analytcs = os.getenv('s3_analytcs')
s3_tz_in = os.getenv('s3_tz_in')
s3_tz_out = os.getenv('s3_tz_out')
database_raw = os.getenv('database_raw')
database_stage = os.getenv('database_stage')
database_analtyics = os.getenv('database_analtyics')
environment = os.getenv('environment')

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


def mover_arquivo_para_processados(bucket_name, key):
    novo_key = os.path.join("processados", os.path.basename(key))
    print(f"Movendo arquivo para: {novo_key}")

    # Copia o arquivo para o novo diretório
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': key},
        Key=novo_key
    )

    # Remove o arquivo original
    s3_client.delete_object(Bucket=bucket_name, Key=key)

    print("Arquivo movido com sucesso.")


def mover_arquivo_para_processados(bucket_name, key):
    novo_key = f"reprocessamento/processados/{os.path.basename(key)}"
    print(f"Movendo arquivo para: {novo_key}")

    # Copia o arquivo
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': key},
        Key=novo_key
    )

    # Remove o original
    s3_client.delete_object(Bucket=bucket_name, Key=key)

    print("✅ Arquivo movido com sucesso.")

def lambda_handler(event, context):
    """
    Função principal da Lambda.
    Processa o evento recebido, valida parâmetros, monta e executa comandos Spark Submit no EMR,
    monitora os steps e registra o início do reprocessamento no Athena.
    """
    try:
        print("################################################################")
        print("INICIO DO PROCESSAMENTO")
        print("################################################################")

        print("Processando evento S3 ou evento manual...")
        print(event)

        # Evento vindo do SQS
        if 'Records' in event and 'body' in event['Records'][0]:
            print("🔄 Evento recebido via SQS.")
            body_str = event['Records'][0]['body']
            body = json.loads(body_str)
        # Evento com chave 'Body'
        elif 'Body' in event:
            print("⚙️ Evento recebido com chave 'Body'.")
            body = json.loads(event['Body'])
        # Evento manual (direto)
        else:
            print("⚙️ Evento recebido diretamente (trigger ou execução manual).")
            body = event

        print("Processando dados do evento...")
        print(body)

        # Extrai parâmetros do evento
        bucket_name = body['bucket']
        key = body['key']
        cluster_name_event = body['cluster_name']

        print(f"Bucket: {bucket_name}")
        print(f"Arquivo: {key}")

        # Baixar o arquivo JSON do S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        file_content = response['Body'].read().decode('utf-8')

        print("Verificar se o arquivo está vazio...")
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

        # Junta todos os parâmetros em um único dicionário
        parametros = {**parametros_obrigatorios, **parametros_opcionais}

        # Atribuir variáveis para uso posterior
        nome_solicitante = parametros['nome_solicitante']
        email_solicitante = parametros['email_solicitante']
        area_solicitante = parametros['area_solicitante']
        squad_pertencente = parametros['squad_pertencente']
        tipo_processo = parametros['tipo_processo']
        name_reproc = parametros['name_reproc']
        database_name_table_reproc = parametros['database_name_table_reproc']
        table_name_reproc = parametros['table_name_reproc']
        periodo_especifico = parametros['periodo_especifico']
        lista_datas_especificas = parametros['lista_datas_especificas']
        tipo_frequencia = parametros['tipo_frequencia']
        data_inicio = parametros['data_inicio']
        data_fim = parametros['data_fim']
        s3_path_script_exec = parametros['s3_path_script_exec']
        path_json_exec = parametros['path_json_exec']

        # Printar parâmetros extraídos
        print("Parâmetros extraídos do arquivo JSON:")
        for chave, valor in parametros.items():
            print(f"{chave}: {valor}")
        print("################################################################")

        # Montar o comando spark-submit
        print("Montar o comando spark-submit:")

        spark_submit_command = (
            f"spark-submit --jars=s3://{s3_studio}/install_libs/jars/* "
            f"--py-files=s3://{s3_studio}/scripts/common/*.py s3://{s3_studio}/{s3_path_script_exec} "
            f"\"{{\\\"process_date\\\": \\\"$i\\\", \\\"database\\\": {{\\\"raw\\\": \\\"{database_raw}\\\", \\\"stage\\\": \\\"{database_stage}\\\", \\\"analytics\\\": \\\"{database_analtyics}\\\"}}, "
            f"\\\"bucket\\\": {{\\\"raw\\\": \\\"{s3_raw}\\\", \\\"stage\\\": \\\"{s3_stage}\\\", \\\"analytics\\\": \\\"{s3_analytcs}\\\", \\\"tz-in\\\": \\\"{s3_tz_in}\\\", \\\"tz-out\\\": \\\"{s3_tz_out}\\\"}}, "
            f"\\\"environment\\\": \\\"{environment}\\\", \\\"log_group\\\": \\\"airflow-alelo-airflow-base-DATALAKE-{environment}-Task\\\", \\\"task_id\\\": \\\"{tipo_processo}_{name_reproc}\\\", \\\"dag_id\\\": \\\"{tipo_processo}_{name_reproc}\\\", \\\"log_stream\\\": \\\"dag_id={tipo_processo}_{name_reproc}\\\"}}\" "
            f"\"{{\\\"bucket\\\":\\\"{s3_studio}\\\",\\\"json_parameter\\\":\\\"{path_json_exec}\\\"}}\""
        )

        #if process_servelles == "True":
        #    print(f"process_servelles :{process_servelles}")
        #    spark_submit_command = (
        #        f"spark-submit --jars=s3://alelo-emrstudio-datalake-prd/install_libs/jars/* "
        #        f"--py-files=s3://alelo-emrstudio-datalake-prd/scripts/common/*.py s3://alelo-emrstudio-datalake-prd/{s3_path_script_exec} "
        #        f"\"{{\\\"process_date\\\": \\\"$i\\\", \\\"database\\\": {{\\\"raw\\\": \\\"db_hub_bddiprd1_raw_prd\\\", \\\"stage\\\": \\\"db_hub_bddiprd1_stage_prd\\\", \\\"analytics\\\": \\\"db_hub_bddiprd1_analytics_prd\\\"}}, "
        #        f"\\\"bucket\\\": {{\\\"raw\\\": \\\"alelo-datalake-raw-prd\\\", \\\"stage\\\": \\\"alelo-datalake-stage-prd\\\", \\\"analytics\\\": \\\"alelo-datalake-analytics-prd\\\", \\\"tz-in\\\": \\\"alelo-datalake-tz-in-prd\\\", \\\"tz-out\\\": \\\"alelo-datalake-tz-out-prd\\\"}}, "
        #        f"\\\"environment\\\": \\\"prd\\\", \\\"log_group\\\": \\\"airflow-alelo-airflow-base-DATALAKE-PRD-Task\\\", \\\"task_id\\\": \\\"{tipo_processo}\\\", \\\"dag_id\\\": \\\"{tipo_processo}\\\", \\\"log_stream\\\": \\\"dag_id={tipo_processo}\\\"}}\" "
        #        f"\"{{\\\"bucket\\\":\\\"alelo-emrstudio-datalake-prd\\\",\\\"json_parameter\\\":\\\"{path_json_exec}\\\"}}\""
        #    )
#
        #else:
        #    print(f"process_servelles :{process_servelles}")
        #    spark_submit_command = (
        #        f"spark-submit --jars=s3://alelo-emrstudio-datalake-prd/install_libs/jars/* "
        #        f"--py-files=s3://alelo-emrstudio-datalake-prd/scripts/common/*.py "
        #        f"s3://alelo-emrstudio-datalake-prd/{s3_path_script_exec} "
        #        f"'{{\"process_date\": \"20250702\", \"database\": {{\"raw\": \"db_hub_bddiprd1_raw_prd\", \"stage\": \"db_hub_bddiprd1_stage_prd\", \"analytics\": \"db_hub_bddiprd1_analytics_prd\"}}, "
        #        f"\"bucket\": {{\"raw\": \"alelo-datalake-raw-prd\", \"stage\": \"alelo-datalake-stage-prd\", \"analytics\": \"alelo-datalake-analytics-prd\", \"tz-in\": \"alelo-datalake-tz-in-prd\", \"tz-out\": \"alelo-datalake-tz-out-prd\"}}, "
        #        f"\"environment\": \"prd\", \"log_group\": \"airflow-alelo-airflow-base-DATALAKE-PRD-Task\", "
        #        f"\"task_id\": \"spark_transformation_gfk_report_orders_bi\", \"dag_id\": \"{tipo_processo}\", \"log_stream\": \"{tipo_processo}\"}}' "
        #        f"'{{\"bucket\":\"alelo-emrstudio-datalake-prd\",\"json_parameter\":\"{path_json_exec}\"}}'"
        #    )

        print("Comando Spark Submit montado:")
        print(spark_submit_command)
        print("################################################################")

        # Listar clusters e encontrar o correto
        print("Listando clusters EMR disponiveis...")
        clusters = emr_client.list_clusters(ClusterStates=['WAITING', 'RUNNING'])
        cluster_id = None

        for cluster in clusters['Clusters']:
            if cluster['Name'] == cluster_name_event:  # Verifica se o nome é exatamente igual
                cluster_id = cluster['Id']
                print(f"Cluster encontrado: {cluster['Name']} (ID: {cluster_id})")
                break

        if not cluster_id:
            raise RuntimeError(f"Cluster com o nome exato '{cluster_name_event}' nao encontrado.")

        # Montar o comando para copiar o script do S3
        print("Montando comando para copiar o script do S3...")
        criar_shell_padrao = [
            'bash', '-c',
            f'aws s3 cp s3://{s3_studio}/scripts/reprocessamento/shell_padrao_reproc.sh /home/hadoop/shell_padrao_reproc_{name_reproc}.sh && '
            'sleep 20 && '
            f'[[ -f /home/hadoop/shell_padrao_reproc_{name_reproc}.sh ]] && chmod +x /home/hadoop/shell_padrao_reproc_{name_reproc}.sh'
        ]

        #criar_shell_padrao = [
        #    'bash', '-c',
        #    f'aws s3 cp s3://{s3_stage}/dataops/airflow/scripts/jobs_parameter/migracao/pv/shell_padrao_reproc_teste_2.sh /home/hadoop/shell_padrao_reproc_{name_reproc}.sh && '
        #    'sleep 20 && '
        #    f'[[ -f /home/hadoop/shell_padrao_reproc_{name_reproc}.sh ]] && chmod +x /home/hadoop/shell_padrao_reproc_{name_reproc}.sh'
        #]

        print(f"Comando de copia montado: {criar_shell_padrao}")
        print("################################################################")

        # Submeter a etapa de cópia ao cluster EMR
        print("Submetendo etapa para copiar o script no cluster EMR...")
        copy_step = {
            'Name': f'Criacao Script Shell para o {tipo_processo} - {name_reproc}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': criar_shell_padrao
            }
        }

        response_copy = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[copy_step]
        )
        step_id_copy = response_copy['StepIds'][0]
        print(f"Etapa de copia submetida com sucesso. Step ID: {step_id_copy}")
        print("################################################################")

        # Verificar o status da etapa de cópia
        print("Verificando o status da etapa de copia...")
        while True:
            step_status = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id_copy)
            state = step_status['Step']['Status']['State']
            print(f"Status da etapa de copia: {state}")
            
            if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(10)

        if state != 'COMPLETED':
            reason = step_status['Step']['Status'].get('FailureDetails', {}).get('Reason', 'Motivo não especificado')
            print(f"Etapa de copia falhou com o estado: {state}. Motivo: {reason}")
            raise RuntimeError(f"Etapa de copia falhou com o estado: {state}. Motivo: {reason}")

        # Submeter a etapa para executar o script
        print("Submetendo etapa para executar o script no cluster EMR...")
        print(f"Periodo_especifico: {periodo_especifico}")

        if periodo_especifico == "True":
            name_step = f'{tipo_processo}: lista_datas_especificas - {nome_solicitante}'
            executar_comand = [
                f'/home/hadoop/shell_padrao_reproc_{name_reproc}.sh',
                periodo_especifico,
                f'\"\"',  # data_inicio vazio
                f'\"\"',  # data_fim vazio
                tipo_frequencia,
                spark_submit_command,
                lista_datas_especificas
            ]
        else:
            name_step = f'{tipo_processo}: {name_reproc} de {data_inicio} até {data_fim} - {nome_solicitante}'
            executar_comand = [
                f'/home/hadoop/shell_padrao_reproc_{name_reproc}.sh',
                periodo_especifico,
                f'{data_inicio}',
                f'{data_fim}',
                f'{tipo_frequencia}',
                spark_submit_command
            ]
            
        execute_step = {
            'Name': name_step,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': executar_comand
            }
        }

        response_execute = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[execute_step]
        )
        step_id_execute = response_execute['StepIds'][0]
        print(f"Etapa de execucao submetida com sucesso. Step ID: {step_id_execute}")
        print("################################################################")

        # 🔍 Verificar o status da etapa de execução
        print("Verificando o status da etapa de execução...")

        start_time = datetime.utcnow()
        while True:
            step_status = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id_execute)
            state = step_status['Step']['Status']['State']
            print(f"Status da etapa de execução: {state}")

            if state in ['RUNNING', 'FAILED', 'CANCELLED']:
                print(f"Etapa em {state}")
                break
            time.sleep(10)

        # 💥 Se falhou, printa o motivo
        if state != 'RUNNING':
            reason = step_status['Step']['Status'].get('FailureDetails', {}).get('Reason', 'Motivo não especificado')
            print(f"Etapa de execução falhou com o estado: {state}. Motivo: {reason}")

        dt_insert = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d %H:%M:%S')

        cluster_link = f"https://sa-east-1.console.aws.amazon.com/emr/home?region=sa-east-1#/clusterDetails/{cluster_id}"

        print(f"Etapa de execução submetida com sucesso.")
        print("Iniciando o processo de montagem para carga na tabela de monitoramento")
        # 🧾 Preparar dados do evento
        event = {
            "id_solicitacao": step_id_execute,
            "nome_solicitante": nome_solicitante,
            "email_solicitante": email_solicitante,
            "area_solicitante": area_solicitante,
            "squad_pertencente": squad_pertencente,
            "tipo_processo": tipo_processo,
            "name_reproc": name_reproc,
            "database_name_table_reproc": database_name_table_reproc,
            "table_name_reproc": table_name_reproc,
            "periodo_especifico": periodo_especifico,
            "lista_datas_especificas": lista_datas_especificas,
            "tipo_frequencia" : tipo_frequencia,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "s3_path_script_exec": s3_path_script_exec,
            "path_json_exec":path_json_exec,
            "bucket_original" : bucket_name,
            "key_original": key,
            "cluster_id": cluster_id,
            "cluster_link": cluster_link,
            "step_id": step_id_execute,
            "step_submit": spark_submit_command,
            "step_status": None,
            "start_time_step": None,
            "end_time_step": None,
            "tempo_execucao": None, 
            "cluster_name": cluster_name_event,
            "dt_insert": dt_insert,
            "dt_update": None
        }

        print(f"Preparando para inserir os dados na tabela {database}.{table} no Athena")

        # 🛠 Construir e executar query no Athena
        query = f"""
            INSERT INTO {database}.{table} 
            (
                id_solicitacao, 
                nome_solicitante,
                email_solicitante,
                area_solicitante,
                squad_pertencente,
                tipo_processo, 
                name_reproc,
                database_name_table_reproc,
                table_name_reproc,
                periodo_especifico, 
                lista_datas_especificas,
                tipo_frequencia,
                data_inicio, 
                data_fim, 
                s3_path_script_exec, 
                path_json_exec,
                bucket_original, 
                key_original, 
                cluster_id, 
                cluster_link,
                step_id,
                step_submit,
                step_status,
                start_time_step, 
                end_time_step, 
                tempo_execucao,
                cluster_name, 
                dt_insert,
                dt_update
            )
            VALUES (
                '{event["id_solicitacao"]}',
                '{event["nome_solicitante"]}',
                '{event["email_solicitante"]}',
                '{event["area_solicitante"]}',
                '{event["squad_pertencente"]}',
                '{event["tipo_processo"]}',
                '{event["name_reproc"]}',
                '{event["database_name_table_reproc"]}',
                '{event["table_name_reproc"]}',
                {f"'{event['periodo_especifico']}'" if event.get("periodo_especifico") else "NULL"},
                {f"'{event['lista_datas_especificas']}'" if event.get("lista_datas_especificas") else "NULL"},
                '{event["tipo_frequencia"]}',
                {f"DATE '{event['data_inicio']}'" if event.get("data_inicio") else "NULL"},
                {f"DATE '{event['data_fim']}'" if event.get("data_fim") else "NULL"},
                '{event["s3_path_script_exec"]}',
                '{event["path_json_exec"]}',
                '{event["bucket_original"]}',
                '{event["key_original"]}',
                '{event["cluster_id"]}',
                '{cluster_link}',
                '{event["step_id"]}',
                '{event["step_submit"]}',
                {f"'{event['step_status']}'" if event.get("step_status") else "NULL"},
                {f"TIMESTAMP '{event['start_time_step']}'" if event.get("start_time_step") else "NULL"},
                {f"TIMESTAMP '{event['end_time_step']}'" if event.get("end_time_step") else "NULL"},
                {event["tempo_execucao"] if event.get("tempo_execucao") is not None else "NULL"},
                '{event["cluster_name"]}',
                TIMESTAMP '{event["dt_insert"]}',
                {f"TIMESTAMP '{event['dt_update']}'" if event.get("dt_update") else "NULL"}
            )
        """

        timeout=200
        print(f"Database: {database}")
        print(f"Tabela: {table}")
        print(f"Output S3: {outputLocation_s3}")
        print(f"Query Athena:\n{query}")

        try:
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": outputLocation_s3}
            )
            query_execution_id = response['QueryExecutionId']
            print(f"Query Athena submetida. Execution ID: {query_execution_id}")

            print("Aguardando execução da query...")
            start_time = time.time()

            while True:
                result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = result['QueryExecution']['Status']['State']

                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break

                if time.time() - start_time > timeout:
                    print("Tempo limite excedido ao aguardar a execução da query.")
                    return

                time.sleep(2)

            if status == 'SUCCEEDED':
                print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
            elif status == 'FAILED':
                reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Motivo não informado')
                print(f"❌ ERRO NA EXECUÇÃO DA QUERY: {reason}")
            else:
                print(f"⚠️ Query foi cancelada ou não concluída. Status final: {status}")

        except Exception as e:
            print(f"❌ Erro ao executar a query no Athena: {str(e)}")
            return {
                'statusCode': 200,
                'body': json.dumps('Jobs submetidos com sucesso.')
            }
        # Chamada da função após o processamento
        mover_arquivo_para_processados(bucket_name, key)
        print("Arquivo movido para a pasta de processados com sucesso.")
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Erro durante o processamento: {str(e)}")
        }