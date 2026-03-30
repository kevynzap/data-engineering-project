from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="spark_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark_jobs/exemplo.py
        """
    )