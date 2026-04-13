from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime 
import pandas as pd 

# criando uma sessao sparksession
spark = (
    SparkSession.builder
    .appName("Aisles_Report")
    .getOrCreate()
)

# variaveis
path_input = "/opt/airflow/input_data/aisles/aisles.csv"
path_output = "s3a://bronze/aisles/report/"


# leitura do arquivo csv
aisles = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load(path_input)
)

# report 
total_records = aisles.count()  # qtd
schema = [(f.name, str(f.dataType)) for f in aisles.schema.fields]  # schema da tab
duplicates = aisles.count() - aisles.dropDuplicates().count()   # dados duplicados

print("ESTATISTICA BASICA")
aisles.describe().show()

# save dict report
report = {
    "table": "aisles",
    "layer": "bronze",
    "dt_execucao": datetime.now().isoformat(),
    "total_records": total_records,
    "duplicates": duplicates,
    "columns": len(aisles.columns)
}

df = pd.DataFrame(report)
print(df)