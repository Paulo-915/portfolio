import os
import boto3

# Variáveis de ambiente
region_name = os.getenv('REGION_NAME', 'sa-east-1')

# Criar uma sessão do Boto3
session = boto3.session.Session()

# Inicializa o cliente do Glue usando a sessão configurada
glue_client = session.client('glue', region_name=region_name, verify=True)

def buscar_data_connections_por_url_ou_usuario(filtro_url=None, filtro_usuario=None):
    """
    Busca por padrão no campo 'Connection URL' ou 'Username' nas Data Connections do AWS Glue.

    Args:
        filtro_url (str): Padrão a ser buscado no campo 'Connection URL' ou 'JDBC_CONNECTION_URL'.
        filtro_usuario (str): Padrão a ser buscado no campo 'USERNAME'.

    Returns:
        list: Lista de conexões que atendem ao(s) critério(s) especificado(s).
    """
    try:
        print("Buscando Data Connections no Glue...")
        connections_filtradas = []

        paginator = glue_client.get_paginator('get_connections')
        for page in paginator.paginate():
            connections = page.get('ConnectionList', [])

            for connection in connections:
                connection_name = connection.get('Name')
                props = connection.get('ConnectionProperties', {})

                url_match = filtro_url and (
                    filtro_url.lower() in props.get('CONNECTION_URL', '').lower() or
                    filtro_url.lower() in props.get('JDBC_CONNECTION_URL', '').lower()
                )

                user_match = filtro_usuario and (
                    filtro_usuario.lower() in props.get('USERNAME', '').lower()
                )

                # Adiciona se qualquer filtro for atendido
                if url_match or user_match:
                    connections_filtradas.append(connection_name)

        return connections_filtradas

    except Exception as e:
        print(f"Erro ao buscar Data Connections no Glue: {str(e)}")
        return []

if __name__ == "__main__":
    filtro_url = None        # Pode ser None se não quiser filtrar por URL
    filtro_usuario = "USDATACATALOG"   # Pode ser None se não quiser filtrar por usuário

    connections_encontradas = buscar_data_connections_por_url_ou_usuario(filtro_url, filtro_usuario)

    if connections_encontradas:
        print("Data Connections encontradas:")
        for connection in connections_encontradas:
            print(f"- {connection}")
    else:
        print("Nenhuma Data Connection encontrada com os critérios informados.")
