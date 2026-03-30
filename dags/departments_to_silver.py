from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset
from datetime import datetime 

# get variables airflow minio
access_key = Variable.get("minio_access")
secret_key = Variable.get("minio_secret")

# parametros de configuração do spark
config = {
    "spark.master": "spark://spark-master:7077",
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.4.3,com.amazonaws:aws-java-sdk-bundle:1.12.797",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": access_key,
    "spark.hadoop.fs.s3a.secret.key": secret_key,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

silver_departments = Dataset("s3a://silver/departments/")

@dag(
    dag_id = "departments_to_silver",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["departaments", "extract", "silver"]
)

def main ():

    run_spark = SparkSubmitOperator(
        task_id="run_spark_departaments",
        application="/opt/spark_jobs/departments_to_silver.py",
        conf=config,
        outlets=[silver_departments],
        verbose=True
    )

main()