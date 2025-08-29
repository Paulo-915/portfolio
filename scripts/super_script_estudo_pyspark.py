
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import avg, col, udf, round, count
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
import datetime

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SuperScriptEstudoPySpark").getOrCreate()

# Parte 1: Criação e Manipulação
# --------------------------------

# 1. Criação do DataFrame
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

# 2. Agrupamento por Occupation e cálculo da média de idade
df_grouped = df.groupBy("Occupation").agg(avg("Age").alias("Average_Age"))

# 3. Ordenação pela média de idade em ordem decrescente
df_sorted = df_grouped.orderBy("Average_Age", ascending=False)

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
