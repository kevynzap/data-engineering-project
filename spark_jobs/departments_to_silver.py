from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Departments_Silver")
    .getOrCreate()
)

# lendo arquivo parquet na bronze
departments = (
    spark.read
    .format("parquet")
    .load("s3a://bronze/departments/")
)

# adicionando data de carga
departments = (
    departments
    .withColumnsRenamed(
        {
            "department_id": "id_departamento",
            "department": "nome_departamento"
        }
    )
    .select(
        col("id_departamento"),
        lower(trim(col("nome_departamento"))).alias("nome_departamento"),
        col("dt_carga")
    )
)

# escrever arquivo no minio
(
    departments.write
    .mode("overwrite")
    .parquet("s3a://silver/departments/")
)

# encerrando sparksession
spark.stop()