from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 
from delta.tables import DeltaTable

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Departments_Silver")
    .getOrCreate()
)

# variaveis
path_input = "s3a://bronze/departments/"
path_output = "s3a://silver/departments/"


# lendo arquivo parquet na bronze
departments = (
    spark.read
    .format("parquet")
    .load(path_input)
)

# deduplicação
departments_dedup = departments.dropDuplicates(["department_id", "department"])

# adicionando data de carga
departments = (
    departments_dedup
    .withColumnsRenamed(
        {
            "department_id": "id_departamento",
            "department": "nome_departamento"
        }
    )
    .select(
        col("id_departamento"),
        lower(trim(col("nome_departamento"))).alias("nome_departamento"),
        #col("dt_carga")
    )
)

# verifica se a tabela ja existe
if DeltaTable.isDeltaTable(spark, path_output):

    delta_table = DeltaTable.forPath(spark, path_output)

    (
        delta_table.alias("target")
        .merge(
            departments.alias("source"),
            "target.id_departamento = source.id_departamento"
        )
        .whenMatchedUpdate(
            condition="target.nome_departamento != source.nome_departamento",
            set={
                "nome_departamento = source.nome_departamento"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # escrever arquivo no minio
    (
        departments.write
        .format("delta")
        .mode("overwrite")
        .parquet(path_output)
    )

# encerrando sparksession
spark.stop()