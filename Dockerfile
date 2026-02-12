FROM apache/airflow:2.9.0-python3.11

USER root

# Install Java and system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download and install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    chmod -R 755 ${SPARK_HOME}

# Download required JAR files for S3/MinIO support
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar && \
    # Kafka JARs for Spark Streaming
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -o ${SPARK_HOME}/jars/kafka-clients-3.5.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o ${SPARK_HOME}/jars/commons-pool2-2.11.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    # PostgreSQL JDBC driver for Spark
    curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
    -o ${SPARK_HOME}/jars/postgresql-42.7.1.jar

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins && \
    chown -R airflow:root /opt/airflow && \
    chmod -R 777 /opt/airflow

USER airflow

# Install Python packages
RUN pip install --upgrade pip &&\
    pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.9.0 \
    apache-airflow-providers-postgres==5.11.0 \
    apache-airflow-providers-amazon==8.20.0 \
    apache-airflow-providers-apache-kafka==1.4.0 \
    pyspark==3.5.1 \
    pandas==2.2.0 \
    boto3==1.34.34 \
    s3fs==2024.2.0 \
    requests==2.31.0 \
    kafka-python-ng==2.2.2 \
    confluent-kafka==2.3.0 \
    minio==7.2.3 \
    psycopg2-binary==2.9.9 \
    fastapi==0.109.0 \
    uvicorn==0.27.0 \
    websockets==12.0 \
    google-generativeai

WORKDIR /opt/airflow