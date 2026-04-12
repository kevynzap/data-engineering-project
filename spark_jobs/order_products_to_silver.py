from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Order_Products")
    .getOrCreate()
)

# variaveis
path_input = "s3a://bronze/order_products/"
path_output = "s3a://silver/order_products/"

# leitura do arquivo parquet bronze
order_products = (
    spark.read
    .format("parquet")
    .load(path_input)
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

# aumentar o partition para reduzir o shuffle
order_products = order_products.repartition(4)

# verificar se a tabela ja existe
if DeltaTable.isDeltaTable(spark, path_output):

    delta_table = DeltaTable.forPath(spark, path_output)
    
    join = (
        order_products.alias("source") 
        .join(
            delta_table.toDF().alias("target"),
            """
            target.id_pedido = source.id_pedido
            AND target.id_produto = source.id_produto
            """,
            how="left_anti"
        )
    )

    (
        join.write
        .format("delta")
        .mode("append")
        .save(path_output)
    )

else:
    (
        order_products.write 
        .format("delta")
        .mode("overwrite")
        .save(path_output)
    )

# encerrando sparksession
spark.stop()