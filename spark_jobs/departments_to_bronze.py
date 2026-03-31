from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Departments")
    .getOrCreate()
)

# leitura do arquivo csv
departments = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load("/opt/airflow/input_data/departments/departments.csv")
)

# adicionando data de carga
departments = (
    departments
    .withColumn("dt_carga", lit(datetime.now().strftime("%Y-%m-%d")))
)

# escrever arquivo no minio
(
    departments.write
    .mode("append")
    .parquet("s3a://bronze/departments/")
)

# encerrando sparksession
spark.stop()