from faker import Faker
from pyspark.sql import SparkSession
from datetime import datetime 

# abrindo uma sessao spark
spark = SparkSession.builder.appName("Faker_Data").getOrCreate()

def create_fake_user_data(num_users):
    
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

    return spark.createDataFrame(data)
    

# gerar uma base de teste com 100 registros
df = create_fake_user_data(5)

df.show()

# salvar na raiz
(
    df 
    .write
    .format("csv")
    .mode("overwrite")
    .option("header", "true")
    .option("mergeSchema", "true")
    .save("/opt/airflow/outputs/output.csv")
)
#df.write.csv("/opt/airflow/outputs/output.csv", header=True)

print("Arquivo gerado com sucesso!")

spark.stop()
    

