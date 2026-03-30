from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Order_Products")
    .getOrCreate()
)

# leitura do arquivo csv
order_products = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load("/opt/airflow/input_data/order_products/order_products__prior.csv")
)

# adicionando data de carga
order_products = (
    order_products
    .withColumn("dt_carga", lit(datetime.now().strftime("%Y-%m-%d")))
)

# escrever arquivo no minio
(
    order_products.write
    .mode("overwrite")
    .parquet("s3a://bronze/order_products/")
)

# encerrando sparksession
spark.stop()