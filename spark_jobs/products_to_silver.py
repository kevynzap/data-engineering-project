from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Products_Silver")
    .getOrCreate()
)

# leitura do arquivo csv
products = (
    spark.read
    .format("parquet")
    .load("s3a://bronze/products/")
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
            "id_corredor_invalid_format": when(col("id_corredor").try_cast("integer").isNull(), lit(1)).otherwise(lit(0)),
            "id_departamento_invalid_format": when(col("id_departamento").try_cast("integer").isNull(), lit(1)).otherwise(lit(0))
        }
    )
    .filter(
        ~((col("id_corredor_invalid_format") == 1)
        | (col("id_departamento_invalid_format") == 1))
    )
)

# escrever arquivo no minio
(
    products.write
    .mode("overwrite")
    .parquet("s3a://silver/products/")
)

# encerrando sparksession
spark.stop()