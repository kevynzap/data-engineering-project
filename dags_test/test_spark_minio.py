from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql.functions import *
from datetime import datetime 
from faker import Faker
import os



bucket_name = "bronze"
object_key = "test/test2.csv"
tmp_file = "/tmp/teste2.csv"

@dag(
    dag_id="test_spark_minio",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["test", "spark", "minio"]
)

def main():

    # abrindo uma sessao spark
    #spark = SparkSession.builder.appName("faker_data").getOrCreate()

    @task(task_id="creata_fake_data")
    def create_fake_data(num_users):

        #abrindo uma sessao
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("faker_data").getOrCreate()
        
        # faker BR
        fake = Faker('pt_BR')
        data = []

        for _ in range(num_users):
            data.append({
                'nome': fake.name(),
                'email': fake.email(),
                'cpf': fake.cpf(),
                'dt_nascimento': fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
                'endereco': fake.address(),
                'cidade': fake.city(),
                'estado': fake.state_abbr(),
                'profissao': fake.job(),
                'empresa': fake.company(),
                'email_empresa': fake.company_email(),
                'dt_carga': datetime.now().strftime("%Y%m%d")
            })

        # cria um dataframe
        df = spark.createDataFrame(data)

        # força o spark a gerar um unico arquivo
        df.coalesce(1).write.mode("overwrite").csv("/opt/airflow/output_data/output", header=True)

        # encerra a sessao spak na task
        spark.stop()

        return "/opt/airflow/output_data/output"
    

    @task(task_id="load_minio")
    def load_minio(path):

        # busca o caminho onde se encontra o arquivo
        files = os.listdir("/opt/airflow/output_data/output")
        csv_file = [f for f in files if f.endswith(".csv")][0]
        full_path = f"{path}/{csv_file}"

        # hook para conectar e carregar o arquivo no minio
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=full_path,
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )

    load_minio(create_fake_data(1000))

main()
    
