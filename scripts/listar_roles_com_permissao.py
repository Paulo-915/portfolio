import os
import boto3

# Defina a região
region_name = os.getenv('REGION_NAME', 'sa-east-1')

# Criar uma sessão do Boto3
session = boto3.session.Session()

# Criar clientes com verify=False
glue_client = session.client('glue', region_name=region_name, verify=False)
secrets_manager_client = session.client('secretsmanager', region_name=region_name, verify=False)


def listar_roles_com_permissao(acao_procurada):
    """
    Lista roles no IAM que possuem a permissão especificada atrelada.
    
    :param acao_procurada: A permissão que você deseja procurar (ex: 's3:ListBucket').
    """
    iam_client = boto3.client('iam', verify=False)  # Desabilita validação SSL

    try:
        # Lista todas as roles no IAM
        roles = iam_client.list_roles()
        print(f"Procurando roles com a permissão '{acao_procurada}'...")

        for role in roles['Roles']:
            role_name = role['RoleName']
            print(f"Verificando role: {role_name}")

            # Lista as políticas atreladas à role
            attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)
            for policy in attached_policies['AttachedPolicies']:
                policy_name = policy['PolicyName']
                policy_arn = policy['PolicyArn']

                # Obtém os detalhes da política
                policy_version = iam_client.get_policy(PolicyArn=policy_arn)['Policy']['DefaultVersionId']
                policy_document = iam_client.get_policy_version(PolicyArn=policy_arn, VersionId=policy_version)

                # Verifica se a permissão especificada está presente
                statements = policy_document['PolicyVersion']['Document']['Statement']
                if not isinstance(statements, list):
                    statements = [statements]

                for statement in statements:
                    actions = statement.get('Action', [])
                    if isinstance(actions, str):
                        actions = [actions]

                    if acao_procurada in actions or '*' in actions or acao_procurada.split(':')[0] + ':*' in actions:
                        print(f"Role '{role_name}' possui a permissão '{acao_procurada}' na política '{policy_name}'")
                        break

    except Exception as e:
        print(f"Erro ao listar roles ou políticas: {str(e)}")


if __name__ == "__main__":
    # Defina a permissão que você deseja procurar
    permissao_procurada = "s3:ListBucket"  # Você pode alterar para outra permissão, como "ec2:DescribeInstances"
    listar_roles_com_permissao(permissao_procurada)