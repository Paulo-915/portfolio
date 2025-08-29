import boto3
import os
import certifi

# Defina a região
region_name = os.getenv('REGION_NAME', 'sa-east-1')

# Criar uma sessão do Boto3
session = boto3.session.Session()

# Criar clientes com verify=False
glue_client = session.client('glue', region_name=region_name, verify=False)
secrets_manager_client = session.client('secretsmanager', region_name=region_name, verify=False)

sqs_client = boto3.client('sqs', verify=certifi.where())

def listar_mensagens_sqs(queue_url):
    """
    Lista as mensagens de uma fila SQS e imprime aquelas que foram enviadas hoje.
    
    :param queue_url: URL da fila SQS.
    """
    # Cria o cliente SQS
    print("Criando cliente SQS...")
    sqs_client = boto3.client('sqs', verify=False)

    try:
        # Recebe mensagens da fila
        print(f"Recebendo mensagens da fila com URL '{queue_url}'...")
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # Número máximo de mensagens a receber (até 10)
            WaitTimeSeconds=5,       # Tempo de espera para mensagens (long polling)
            AttributeNames=['All']   # Inclui todos os atributos da mensagem
        )

        # Verifica se há mensagens na fila
        if 'Messages' in response:
            print(f"Mensagens encontradas na fila:")
            for message in response['Messages']:
                # Obtém o corpo e os atributos da mensagem
                message_id = message['MessageId']
                body = message['Body']
                attributes = message.get('Attributes', {})

                # Verifica o atributo SentTimestamp (timestamp de envio da mensagem)
                sent_timestamp = attributes.get('SentTimestamp')
                if sent_timestamp:
                    # Converte o timestamp para uma data legível
                    sent_time = datetime.fromtimestamp(int(sent_timestamp) / 1000, tz=timezone.utc)
                    print(f"Mensagem ID: {message_id}")
                    print(f"Corpo: {body}")
                    print(f"Enviada em: {sent_time}")

                    # Verifica se a mensagem foi enviada hoje
                    today = datetime.now(timezone.utc).date()
                    if sent_time.date() == today:
                        print("✅ Esta mensagem foi enviada hoje.")
                    else:
                        print("❌ Esta mensagem não foi enviada hoje.")
                else:
                    print(f"Mensagem ID: {message_id} não contém o atributo 'SentTimestamp'.")
                print("-" * 50)
        else:
            print("Nenhuma mensagem encontrada na fila.")

    except Exception as e:
        print(f"Erro ao acessar a fila SQS: {str(e)}")

if __name__ == "__main__":
    # URL da fila SQS
    fila_sqs_url = "https://sqs.sa-east-1.amazonaws.com/110403322204/airflow-retry-dags-queue-prd"
    listar_mensagens_sqs(fila_sqs_url)