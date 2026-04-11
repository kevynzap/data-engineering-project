from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Ailes_Silver")
    .getOrCreate()
)

# variaveis
path = "s3a://silver/aisles/"

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

# verifica se a tabela já existe
if DeltaTable.isDeltaTable(spark, path):
    
    delta_table = DeltaTable.forPath(spark, path)

    (
        delta_table.alias("target")
        .merge(
            aisles.alias("source"),
            "target.id_corredor = source.id_corredor"
        )
        .whenMatchedUpdate(
            condition="target.corredor != source.corredor",
            set={
                "corredor": "source.corredor"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        aisles.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save("s3a://silver/aisles/")
    )

# encerrando sparksession
spark.stop()