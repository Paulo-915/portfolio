import boto3
from botocore.config import Config

# Configuração personalizada para o cliente boto3
boto3_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)

# Inicializa o cliente SNS com a configuração personalizada e desabilita a validação SSL
sns = boto3.client('sns', region_name='sa-east-1', config=boto3_config, verify=False)

def envia_sms(mensagem, numero):
    """
    Envia um SMS usando o Amazon SNS.

    Args:
        mensagem (str): A mensagem a ser enviada.
        numero (str): O número de telefone no formato E.164 (ex: +55 para Brasil).
    """
    try:
        response = sns.publish(
            PhoneNumber=numero,
            Message=mensagem
        )
        print(f"SMS enviado para {numero}: {response['MessageId']}")
    except Exception as e:
        print(f"Erro ao enviar SMS: {str(e)}")

if __name__ == "__main__":
    # Mensagem e número de telefone para teste
    mensagem = "Teste de envio de SMS via Amazon SNS"
    numero = "+5511916419223"  # Substitua pelo número de telefone no formato E.164 (ex: +55 para Brasil)

    envia_sms(mensagem, numero)