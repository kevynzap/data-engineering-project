from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Products_Silver")
    .getOrCreate()
)

# variaveis
path_input = "s3a://bronze/products/"
path_output = "s3a://silver/products/"

# leitura do arquivo csv
products = (
    spark.read
    .format("parquet")
    .load(path_input)
)

# adicionando data de carga
products = (
    products
    .withColumnsRenamed(
        {
            "product_id":"id_produto",
            "product_name":"nome_produto",
            "aisle_id":"id_corredor",
            "department_id":"id_departamento"
        }
    )
    .withColumns(
        {
            "id_corredor_invalid_format": when(col("id_corredor").cast("integer").isNull(), lit(1)).otherwise(lit(0)),
            "id_departamento_invalid_format": when(col("id_departamento").cast("integer").isNull(), lit(1)).otherwise(lit(0))
        }
    )
    .filter(
        ~((col("id_corredor_invalid_format") == 1)
        | (col("id_departamento_invalid_format") == 1))
    )
)

# verifica se a tabela ja existe
if DeltaTable.isDeltaTable(spark, path_output):

    delta_table = DeltaTable.forPath(spark, path_output)

    (
        delta_table.alias("target")
        .merge(
            products.alias("source"),
            "target.id_produto = source.id_produto"
        )
        .whenMatchedUpdate()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # escrever arquivo no minio
    (
        products.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .parquet(path_output)
    )

# encerrando sparksession
spark.stop()