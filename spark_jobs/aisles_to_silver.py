from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Ailes_Silver")
    .getOrCreate()
)

# leitura do arquivo parquet na bronze
aisles = (
    spark.read
    .format("parquet")
    .load("s3a://bronze/aisles/")
)

# tratando dados
aisles = (
    aisles 
    .withColumnsRenamed(
        {
            "aisle_id":"id_corredor",
            "aisle":"corredor"
        }
    )
    .select(
        col("id_corredor"), 
        lower(trim(col("corredor"))).alias("corredor")
        #col("dt_carga")
    )
)

# save do silver
(
    aisles.write
    .format("parquet")
    .mode("overwrite")
    .save("s3a://silver/aisles/")
)

# encerrando sparksession
spark.stop()