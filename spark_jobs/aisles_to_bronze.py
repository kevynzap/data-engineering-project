from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Aisles")
    .getOrCreate()
)


# leitura do arquivo csv
aisles = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load("/opt/airflow/input_data/aisles/aisles.csv")
)

# adicionando data de carga
aisles = (
    aisles
    .withColumn("dt_carga", lit(datetime.now().strftime("%Y-%m-%d")))
)

# escrever arquivo particionado anomes
(
    aisles.write
    .format("parquet")
    .mode("append")
    .save("s3a://bronze/aisles/")
)

# encerrando sparksession
spark.stop()
