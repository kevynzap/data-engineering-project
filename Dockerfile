FROM astrocrpublic.azurecr.io/astronomer/astro-runtime:11.7.0
#astrocrpublic.azurecr.io/runtime:3.1-14

USER root

# download do java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

## instalacao do spark 4.1.1 baixado
#COPY spark-4.1.1-bin-hadoop3.tgz /tmp/
#
#RUN tar -xzf /tmp/spark-4.1.1-bin-hadoop3.tgz -C /opt/ && \
#    mv /opt/spark-4.1.1-bin-hadoop3 /opt/spark

# variaveis
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# baixar e instalar spark
RUN curl -fSL --retry 5 --retry-delay 5 \
    -o /tmp/spark.tgz \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# dependencias do s3a do minIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.797/aws-java-sdk-bundle-1.12.797.jar /opt/spark/jars/

# permissao para o astro na root
RUN chown -R astro:astro /opt/spark && \
    chmod -R 755 /opt/spark

# variaveis de ambiente
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

USER astro
