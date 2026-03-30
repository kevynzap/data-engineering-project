from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_submit_operator_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark_jobs/exemplo.py",
        conf={
            "spark.master": "spark://spark-master:7077",
        },
        verbose=True
    )