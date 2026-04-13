from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Fato_Pedido_Gold")
    #.master("spark://spark-master:7077")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "4")

# leitura do arquivo parquet na silver
aisles = (spark.read.format("delta").load("s3a://silver/aisles/"))
departments = (spark.read.format("delta").load("s3a://silver/departments/"))
order_products = (spark.read.format("delta").load("s3a://silver/order_products/"))
orders = (spark.read.format("delta").load("s3a://silver/orders/"))
products = (spark.read.format("delta").load("s3a://silver/products/"))


# join tabs
df_fato = (
    order_products
    .join(orders, "id_pedido")
    .join(broadcast(products), "id_produto")
    .join(broadcast(departments), "id_departamento")
    .join(broadcast(aisles), "id_corredor")
    .select(
        "id_pedido",
        "id_usuario",
        "id_produto",
        "nome_produto",
        "nome_departamento",
        "corredor",
        "hora_dia",
        "dia_semana",
        "nome_dia_semana",
        "flag_recompra"
    )
)

# particionando para melhor distribuição no write
df_fato = df_fato.repartition(4)

# salvando na gold
(
    df_fato.write
    .mode("overwrite")
    .parquet("s3a://gold/fato_pedidos/")
)