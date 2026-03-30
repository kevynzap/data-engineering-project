from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime 

@dag(
    dag_id="faker_data_test",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["faker", "test", "data"]
)

def main():

    @task(task_id="faker_data")
    def run_data_fake():
        run_spark = BashOperator(
            task_id="run_spark_job",
            bash_command="""
            spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark_jobs/test2.py
            """
        )
    
    run_data_fake()

main()

