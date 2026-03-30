from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# responde:
# horarios mais fortes
# dias mais fortes

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Agg_Tempo_Gold")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "4")

# lendo arquivo fato_pedidos
df = spark.read.parquet("s3a://gold/fato_pedidos/")

# agrupando por id_produto
df_tempo = (
    df
    .groupBy("hora_dia", "dia_semana")
    .count()
)

# particionando para melhor distribuição no write
df_tempo = df_tempo.repartition(4)

# save to gold
(
    df_tempo.write
    .mode("overwrite")
    .parquet("s3a://gold/agg_tempo/")
)