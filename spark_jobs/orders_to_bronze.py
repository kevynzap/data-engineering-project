from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Orders")
    .getOrCreate()
)

# leitura do arquivo csv
orders = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load("/opt/airflow/input_data/orders/orders.csv")
)

# adicionando data de carga
orders = (
    orders
    .withColumn("dt_carga", lit(datetime.now().strftime("%Y-%m-%d")))
)

# escrever arquivo no minio
(
    orders.write
    .mode("append")
    .parquet("s3a://bronze/orders/")
)

# encerrando sparksession
spark.stop()