FROM apache/airflow:2.6.2-python3.10 as airflow

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==4.1.1 pyspark==3.3.1


FROM bitnami/spark:3.3 as spark

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean;

RUN rm -r /opt/bitnami/spark/jars && \
    curl https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz | \
    tar --extract --gzip --strip=1 --directory /opt/bitnami/spark/ spark-3.3.1-bin-hadoop3/jars/
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.231/aws-java-sdk-bundle-1.12.231.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.231.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar
RUN curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar --output /opt/bitnami/spark/jars/jets3t-0.9.4.jar
