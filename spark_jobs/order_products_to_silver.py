from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Order_Products")
    .getOrCreate()
)

# leitura do arquivo parquet bronze
order_products = (
    spark.read
    .format("parquet")
    .load("s3a://bronze/order_products/")
)

# tratando dados
order_products = (
    order_products
    .withColumnsRenamed(
        {
            "order_id": "id_pedido",
            "product_id": "id_produto",
            "add_to_cart_order": "ordem_add_carrinho",
            "reordered": "flag_recompra"
        }
    )
    .select(
        col("id_pedido"),
        col("id_produto"),
        col("ordem_add_carrinho"),
        col("flag_recompra"),
        col("dt_carga")
    )
)

# escrever arquivo no minio
(
    order_products.write
    .mode("overwrite")
    .parquet("s3a://silver/order_products/")
)

# encerrando sparksession
spark.stop()