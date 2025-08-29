import os
import json
import csv

# Caminho da pasta onde os arquivos JSON estão localizados (busca recursiva)
pasta = r'C:\repos_lake\data-lake-emrserverless\scripts\jobs_parameter'

# String que queremos procurar
conexao_alvo = 'datalake-acelera-dataguard-spark-conn'

# Lista para armazenar os caminhos dos arquivos que contêm a string
arquivos_encontrados = []

# Lista para armazenar os pares (dag_name, table_name)
dags_e_tabelas = []

# Percorre todos os arquivos na pasta e subpastas
for root, dirs, files in os.walk(pasta):
    for nome_arquivo in files:
        if nome_arquivo.endswith('.json'):
            caminho_arquivo = os.path.join(root, nome_arquivo)
            try:
                with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                    conteudo = json.load(f)
                    if conexao_alvo in json.dumps(conteudo):
                        arquivos_encontrados.append(caminho_arquivo)

                        # Extrai o nome do arquivo sem extensão
                        nome_base = os.path.splitext(nome_arquivo)[0]

                        # Divide o nome do arquivo no primeiro hífen
                        if '-' in nome_base:
                            dag_name, table_name = nome_base.split('-', 1)
                        else:
                            dag_name, table_name = nome_base, ''

                        dags_e_tabelas.append((dag_name, table_name))
            except Exception as e:
                print(f"Erro ao processar {caminho_arquivo}: {e}")

# Caminho de saída desejado para o .txt
caminho_saida_txt = r'C:\Users\9002484\OneDrive - Elo Participações LTDA\Documentos\VALENTE\arquivos_com_conexao.txt'

# Salva os nomes dos arquivos encontrados em um arquivo .txt
with open(caminho_saida_txt, 'w', encoding='utf-8') as f:
    for caminho in arquivos_encontrados:
        f.write(caminho + '\n')

# Caminho de saída desejado para o .csv
caminho_saida_csv = r'C:\Users\9002484\OneDrive - Elo Participações LTDA\Documentos\VALENTE\dags_e_tabelas.csv'

# Salva os pares dag_name e table_name em um arquivo .csv
with open(caminho_saida_csv, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['dag_name', 'table_name'])
    writer.writerows(dags_e_tabelas)

print(f"Busca concluída. Os arquivos encontrados foram salvos em:\n- {caminho_saida_txt}\n- {caminho_saida_csv}")

