from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# responde:
# comportamento do cliente
# frquencia de compra

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Agg_Usuarios_Gold")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "4")

# lendo arquivo fato_pedidos
df = spark.read.parquet("s3a://gold/fato_pedidos/")

# agrupando por id_produto
df_usuarios = (
    df
    .groupBy("id_pedido")
    .agg(
        count("id_pedido").alias("total_pedidos")
    )
)

# particionando para melhor distribuição no write
df_usuarios = df_usuarios.repartition(4)

# save to gold
(
    df_usuarios.write
    .mode("overwrite")
    .parquet("s3a://gold/agg_usuarios/")
)