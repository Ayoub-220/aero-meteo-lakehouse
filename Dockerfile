FROM apache/airflow:2.9.2-python3.11

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow