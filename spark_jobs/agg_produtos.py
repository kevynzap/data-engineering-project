from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# responde:
# produtos mais vendidos
# taxa de recompra

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Agg_Produtos_Gold")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "4")

# lendo arquivo fato_pedidos
df = spark.read.parquet("s3a://gold/fato_pedidos/")

# agrupando por id_produto
df_produtos = (
    df
    .groupBy("id_produto", "nome_produto")
    .agg(
        count("*").alias("totas_vendas"),
        avg("flag_recompra").alias("taxa_recompra")
    )
)

# particionando para melhor distribuição no write
df_produtos = df_produtos.repartition(4)

# save to gold
(
    df_produtos.write
    .mode("overwrite")
    .parquet("s3a://gold/agg_produtos/")
)