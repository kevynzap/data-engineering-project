FROM astrocrpublic.azurecr.io/runtime:3.1-14

USER root

# download do java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# instalacao do spark 4.1.1 baixado
COPY spark-4.1.1-bin-hadoop3.tgz /tmp/

RUN tar -xzf /tmp/spark-4.1.1-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-4.1.1-bin-hadoop3 /opt/spark

# dependencias do s3a do minIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.3/hadoop-aws-3.4.3.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.797/aws-java-sdk-bundle-1.12.797.jar /opt/spark/jars/


ENV PATH="/opt/spark/bin:${PATH}"

USER astro