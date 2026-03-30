from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime 
import pandas as pd 
import logging

bucket_name = "bronze"
object_key = "test/teste.csv"
tmp_file = "/tmp/teste.csv"

@dag(
    dag_id="minio_test_conn",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["minio", "test", "conn"]
)

def minio_test_conn():

    @task(task_id="dataframe")
    def data():
        data = [
            {
                "nome":"kevyn",
                "idade":31,
                "cidade":"guarulhos"
            }
        ]
        df = pd.DataFrame(data)
        return df 
    
    @task(task_id="load")
    def load(data):
        logging.info(f"Dados do dataframe: {data}")
        #return data.head()
    
    @task(task_id="load_csv_minio")
    def load_minio(data):
        
        data.to_csv(tmp_file, index=False)

        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=tmp_file,
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )
    
    
    data1 = data()
    load1 = load(data1)
    load2 = load_minio(data1)

dag = minio_test_conn()