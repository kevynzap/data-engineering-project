from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Products")
    .getOrCreate()
)

# leitura do arquivo csv
products = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load("/opt/airflow/input_data/products/products.csv")
)

# adicionando data de carga
products = (
    products
    .withColumn("dt_carga", lit(datetime.now().strftime("%Y-%m-%d")))
)

# escrever arquivo no minio
(
    products.write
    .mode("overwrite")
    .parquet("s3a://bronze/products/")
)

# encerrando sparksession
spark.stop()