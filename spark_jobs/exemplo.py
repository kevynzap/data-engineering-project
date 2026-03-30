from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Airflow Spark Test").getOrCreate()

data = [("Kevyn", 30), ("Alice", 25)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

df.show()

print("Job executado com sucesso!")

spark.stop()