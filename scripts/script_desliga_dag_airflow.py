import boto3
import requests
import datetime
import pandas as pd
import os
import certifi
from collections import defaultdict
import urllib3
import boto3
import json 

# Oculta os avisos de SSL por uso de verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurar certificado SSL corretamente para evitar warnings
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

region_name = os.getenv('REGION_NAME', 'sa-east-1')

# Inicialização da sessão Boto3 (com verify=False para contornar problemas de SSL)
session = boto3.session.Session()
emr_client = session.client('emr', region_name=region_name, verify=False)
cloudwatch_client = session.client('cloudwatch', region_name=region_name, verify=False)
sqs_client = session.client('sqs', region_name=region_name, verify=False)
glue_client = session.client('glue', region_name=region_name, verify=False)

class AirflowDagManager:
    def __init__(self, env_name, region_name):
        self.env_name = env_name
        self.region_name = region_name

    def __get_session_info__(self):
        mwaa = boto3.client('mwaa', region_name=self.region_name, verify=False)
        response = mwaa.create_web_login_token(Name=self.env_name)
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]
        login_url = f"https://{web_server_host_name}/aws_mwaa/login"
        login_payload = {"token": web_token}
        response = requests.post(login_url, data=login_payload, timeout=30)
        if response.status_code == 200:
            return web_server_host_name, response.cookies["session"]
        else:
            raise Exception(f"Failed to log in: HTTP {response.status_code}")

    def set_dag_state(self, dag_list, pause=True):
        web_server_host_name, session_cookie = self.__get_session_info__()
        action = "pausada" if pause else "ativada"
        for dag_id in dag_list:
            url = f"https://{web_server_host_name}/api/v1/dags/{dag_id}"
            response = requests.patch(
                url,
                headers={'Content-Type': 'application/json'},
                cookies={"session": session_cookie},
                json={"is_paused": pause},
                timeout=30
            )
            if response.status_code == 200:
                print(f"DAG '{dag_id}' {action} com sucesso.")
            else:
                print(f"Erro ao alterar estado da DAG '{dag_id}': {response.status_code}")
                print(response.text)

if __name__ == "__main__":
    manager = AirflowDagManager(env_name="alelo-airflow-base-DATALAKE-PRD", region_name="sa-east-1")
    
    #dags_para_alterar = ["dto_valida_daal_pv_aiub", "bie_auto_alpha_movimientos_ing_dly"]
    
    dags_para_alterar = [
        "b2b_acse_syspu_ing_dly_2",
        "ben_acse_sysac_ing_dly_2",
        "ben_acse_sysbc_ing_dly_2",
        "ecs_acse_sysep_ing_dly_2",
        "prev_acse_sysfp_ing_dly_2",
        "prev_acse_sysrp_ing_dly_2",
        "emi_fis_replica_ing_dly",
        "b2b_gba_acelera_ing_dly",
        "ben_acse_tb_alelo_tudo_cartao_ing",
        "ben_acse_cadastros_iceberg_ing_dly_2",
        "ben_acse_cadastros_ing_dly_2",
        "ben_acse_cadastros_ing_dly_3",
        "ben_acse_cadastros_ing_dly_4",
        "ben_acse_digital_wallet_ing_dly",
        "ben_acse_ing_dly_2",
        "ben_acse_parameter_ing_dly_4",
        "ben_acse_sysbc_ing_dly_3",
        "ben_acse_sysfe_ing_dly_06",
        "ben_acse_tx_transacao_prev_days_trf_dly",
        "ems_trf_dly_6",
        "crs_sale_contr_naip_mig_ing_dly",
        "bie_acse_fat_ing_dly_2",
        "bie_acse_comcad_cadastros_ing_dly_2",
        "bil_acse_sddcar_ing_dly",
        "bie_acse_cadcta_ing_dly",
        "bie_acse_dpctca_ing_dly",
        "bie_acse_cadite_ing_dly",
        "bie_acse_cadped_ing_dly",
        "bie_acse_cob_ing_dly"
    ]

    # Pausar = True
    # Reativas = False
    manager.set_dag_state(dags_para_alterar, pause=False)
