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

silver_orders = Dataset("s3a://silver/orders/")
silver_products = Dataset("s3a://silver/products/")
silver_order_products = Dataset("s3a://silver/order_products/")
silver_aisles = Dataset("s3a://silver/aisles/")
silver_departments = Dataset("s3a://silver/departments/")


@dag(
    dag_id = "gold_fato_pedido",
    start_date = datetime(2026, 1, 1),
    schedule=[
        silver_orders,
        silver_products,
        silver_order_products,
        silver_aisles,
        silver_departments
    ],
    catchup = False,
    tags = ["fato_pedido", "analytics", "gold"]
)

def main ():

    run_spark = SparkSubmitOperator(
        task_id="run_spark_orders",
        application="/opt/spark_jobs/fato_pedidos.py",
        conf=config,
        verbose=True
    )

main()