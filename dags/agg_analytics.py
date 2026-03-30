from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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


@dag(
    dag_id = "gold_analytics",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["agg_produtos","agg_tempo", "agg_usuarios", "analytics", "gold"]
)

def main ():

    run_spark_produtos = SparkSubmitOperator(
        task_id="run_spark_agg_produtos",
        application="/opt/spark_jobs/agg_produtos.py",
        conf=config,
        verbose=True
    )

    run_spark_tempo = SparkSubmitOperator(
        task_id="run_spark_agg_tempo",
        application="/opt/spark_jobs/agg_tempo.py",
        conf=config,
        verbose=True
    )
    
    run_spark_usuarios = SparkSubmitOperator(
        task_id="run_spark_agg_usuarios",
        application="/opt/spark_jobs/agg_usuarios.py",
        conf=config,
        verbose=True
    )
    
    run_spark_produtos >> run_spark_tempo >> run_spark_usuarios

main()