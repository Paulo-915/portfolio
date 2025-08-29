
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import avg, col, udf, round, count
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
import datetime

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SuperScriptEstudoPySpark").getOrCreate()

# Parte 1: Manipulação de Dados
# ------------------------------

# 1. Criação de DataFrame
data = [
    ("Alice", 34, "Data Scientist"),
    ("Bob", 45, "Data Engineer"),
    ("Cathy", 29, "Data Analyst"),
    ("David", 35, "Data Scientist")
]
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", StringType(), True)
])
df = spark.createDataFrame(data, schema=schema)

# Exibir o DataFrame original
df.show()

# 2. Filtragem e Seleção
# Selecionar apenas as colunas "Name" e "Age"
df_selected = df.select("Name", "Age")

# Filtrar as linhas onde a idade é maior que 30
df_filtered = df_selected.filter(df_selected["Age"] > 30)
df_filtered = df_selected.where(col("Age") > 30 & (col("Name") == "Bob")) #outro exemplo

# 3. Agrupamento e Agregação
# Agrupar por "Occupation" e calcular a média de idade
df_grouped = df.groupBy("Occupation").agg(avg("Age").alias("Average_Age"))

# 4. Ordenação
# Ordenar pela média de idade em ordem decrescente
df_sorted = df_grouped.orderBy("Average_Age", ascending=False)

# Exibir resultado da ordenação
df_sorted.show()

# 4. UDF para categorizar idades
def age_category(age):
    if age < 30:
        return "Jovem"
    elif 30 <= age <= 40:
        return "Adulto"
    else:
        return "Senior"

age_category_udf = udf(age_category, StringType())
df_with_category = df.withColumn("AgeCategory", age_category_udf(df["Age"]))

# 5. Funções de janela para calcular diferença entre idade e média da ocupação
window_spec = Window.partitionBy("Occupation")
df_with_avg = df_with_category.withColumn("Average_Age", avg("Age").over(window_spec))
df_final = df_with_avg.withColumn("Age_Diff", round(col("Age") - col("Average_Age"), 2))

# Parte 3: Performance e Otimização
# ----------------------------------

# Particionamento por Occupation
df_partitioned = df_final.repartition("Occupation")
df_partitioned.write.mode("overwrite").partitionBy("Occupation").parquet("output/occupation_partitioned")

# Broadcast Join
# DataFrame pequeno com descrições de ocupações
occupation_desc = [
    ("Data Scientist", "Especialista em ciência de dados"),
    ("Data Engineer", "Engenheiro de dados"),
    ("Data Analyst", "Analista de dados")
]
schema_desc = StructType([
    StructField("Occupation", StringType(), True),
    StructField("Description", StringType(), True)
])
df_desc = spark.createDataFrame(occupation_desc, schema=schema_desc)

# Join com broadcast
#Join com broadcast: força o Spark a replicar um DataFrame pequeno (df_desc) em todos os nós para acelerar
#  o join com um DataFrame grande (df_final), evitando shuffle.
df_joined = df_final.join(broadcast(df_desc), on="Occupation", how="left")

# Parte 4: Integração com Outras Tecnologias
# ------------------------------------------

# Leitura de CSV e escrita em Parquet
csv_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", StringType(), True)
])

# Simulação de leitura de CSV (substitua 'input.csv' pelo caminho real)
# df_csv = spark.read.csv("input.csv", header=True, schema=csv_schema)
# df_csv.write.mode("overwrite").parquet("output/data.parquet")

# Integração com Hadoop HDFS (substitua os caminhos conforme necessário)
#PySpark se integra nativamente com o HDFS, permitindo leitura e escrita de arquivos diretamente no sistema distribuído. 
# Isso é possível porque o Spark roda sobre o Hadoop ou pode acessar o HDFS via URI (hdfs://).

# df_hdfs = spark.read.csv("hdfs://namenode:8020/user/data/input.csv", header=True)
# df_hdfs.write.mode("overwrite").parquet("hdfs://namenode:8020/user/data/output.parquet")

# Parte 5: Problema de Caso - Processamento de Logs
# --------------------------------------------------

# Simulação de dados de log
log_data = [
    (datetime.datetime(2023, 9, 1, 10, 0), "user1", "login"),
    (datetime.datetime(2023, 9, 1, 10, 5), "user2", "click"),
    (datetime.datetime(2023, 9, 1, 10, 10), "user1", "logout"),
    (datetime.datetime(2023, 9, 1, 10, 15), "user3", "click"),
    (datetime.datetime(2023, 9, 1, 10, 20), "user2", "logout"),
    (datetime.datetime(2023, 9, 1, 10, 25), "user1", "login"),
    (datetime.datetime(2023, 9, 1, 10, 30), "user1", "click"),
    (datetime.datetime(2023, 9, 1, 10, 35), "user2", "click"),
    (datetime.datetime(2023, 9, 1, 10, 40), "user3", "logout"),
    (datetime.datetime(2023, 9, 1, 10, 45), "user1", "logout")
]
log_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True)
])
df_logs = spark.createDataFrame(log_data, schema=log_schema)

# Contagem de ações por usuário
df_action_count = df_logs.groupBy("user_id").agg(count("*").alias("action_count"))

# Top 10 usuários mais ativos
df_top_users = df_action_count.orderBy("action_count", ascending=False).limit(10)

# Salvamento em CSV
df_top_users.write.mode("overwrite").csv("output/top_users.csv", header=True)

# Exibição final
df_final.show()
df_joined.show()
df_top_users.show()
