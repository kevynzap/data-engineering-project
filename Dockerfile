FROM astrocrpublic.azurecr.io/astronomer/astro-runtime:11.7.0
#astrocrpublic.azurecr.io/runtime:3.1-14 

USER root

# download do java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# copiar o spark local 
#COPY ./spark/spark-3.5.1-bin-hadoop3.tgz /tmp/spark.tgz    # usado no offline
RUN apt-get update && apt-get install -y wget && \
    wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -O /tmp/spark.tgz

# extrair
RUN tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# dependencias do s3a do minIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Delta Lake (Spark 3.5)
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar /opt/spark/jars/

# (Opcional, mas recomendado dependendo do erro)
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar /opt/spark/jars/

# permissao para o astro na root
RUN chown -R astro:astro /opt/spark && chmod -R 755 /opt/spark

# variaveis de ambiente
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

USER astro