from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 

# criando sessao sparksession
spark = (
    SparkSession.builder
    .appName("Orders")
    .getOrCreate()
)

# variaveis
path_input = "s3a://bronze/orders/"
path_output = "s3a://silver/orders/"

# overwrite dinamico
spark.conf.set("spark.sql.sources.partitionOverWriteMode", "dynamic")

# leitura do arquivo parquet
orders = (
    spark.read
    .format("parquet")
    .load(path_input)
)

# adicionando data de carga
orders = (
    orders
    .withColumnsRenamed(
        {
            "order_id":"id_pedido",
            "user_id":"id_usuario",
            "eval_set":"tipo_conjunto",
            "order_number":"numero_pedido",
            "order_dow":"dia_semana",
            "order_hour_of_day":"hora_dia",
            "days_since_prior_order":"dias_desde_ultimo_pedido"
        }
    )
    .withColumns(
        {
            "dias_desde_ultimo_pedido": 
                when(col("dias_desde_ultimo_pedido").isNull(), 0.0)
                .otherwise(col("dias_desde_ultimo_pedido"))
            ,"is_first_order": 
                when(col("dias_desde_ultimo_pedido").isNull(), lit(1))
                .otherwise(lit(0))
            ,"nome_dia_semana":
                when(col("dia_semana") == 0, "domingo")
                .when(col("dia_semana") == 1, "segunda")
                .when(col("dia_semana") == 2, "terca")
                .when(col("dia_semana") == 3, "quarta")
                .when(col("dia_semana") == 4, "quinta")
                .when(col("dia_semana") == 5, "sexta")
                .when(col("dia_semana") == 6, "sabado")
        } 
    )
)

# escrever arquivo no minio
(
    orders.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("dt_carga")
    .parquet(path_output)
)

# encerrando sparksession
spark.stop()