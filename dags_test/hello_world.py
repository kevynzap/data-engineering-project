from airflow.decorators import dag, task
from datetime import datetime 

# Define uma dag
@dag(
    dag_id="dag_test",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["example", "test"]
)

def main():

    # define a task
    @task 
    def say_hello():
        print(f"Hello World!! I'm in Airflow!")

    say_hello()

dag = main()