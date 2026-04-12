from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime 

# get variables airflow minio
access_key = Variable.get("minio_access")
secret_key = Variable.get("minio_secret")

# parametros de configuração do spark
config = {
    "spark.master": "spark://spark-master:7077",
    
    # dependencias usando packages (gerencia automaticamente) - nao precisa add delta-storage
    #"spark.jars.packages": ",".join([
    #    "org.apache.hadoop:hadoop-aws:3.3.4",
    #    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    #    "io.delta:delta-spark_2.12:3.1.0"
    #]),

    # dependencias usando dockerfile ADD (gerencia manual) - precisa add delta-storage
    "spark.jars": ",".join([
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/jars/delta-spark_2.12-3.1.0.jar",
        "/opt/spark/jars/delta-storage-3.1.0.jar"
    ]),
    
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": access_key,
    "spark.hadoop.fs.s3a.secret.key": secret_key,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
}


@dag(
    dag_id = "order_products_to_bronze",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["order_products", "extract", "bronze"]
)

def main ():

    wait_for_file = FileSensor(
        task_id="wait_for_order_products_file",
        filepath="input_data/order_products/*.csv", 
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule", #"poke" -> trava worker
        fs_conn_id="fs_default"
    )

    run_spark = SparkSubmitOperator(
        task_id="run_spark_order_products",
        application="/opt/spark_jobs/order_products_to_bronze.py",
        conf=config,
        verbose=True
    )

    order_products_silver = TriggerDagRunOperator(
        task_id="trigger_order_products_to_silver",
        trigger_dag_id="order_products_to_silver"
    )

    wait_for_file >> run_spark >> order_products_silver

main()