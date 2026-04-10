from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import datetime

# get variables airflow minio
access_key = Variable.get("minio_access")
secret_key = Variable.get("minio_secret")

# parametros de configuração do spark 
config = {
    "spark.master": "spark://spark-master:7077",
    "spark.jars": "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": access_key,
    "spark.hadoop.fs.s3a.secret.key": secret_key,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    #"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    #"spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

silver_aisles = Dataset("s3a://silver/aisles/")

with DAG(
    dag_id="aisles_to_silver",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags = ["aisles", "extract", "silver"]
) as dag:

    # lê o script pyspark para ler e salvar da pasta input_data (.csv) para output_data (parquet)
    run_spark = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark_jobs/aisles_to_silver.py",
        conf=config,
        outlets=[silver_aisles],
        verbose=True
    )
