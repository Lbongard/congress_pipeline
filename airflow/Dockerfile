ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.8.3-python3.11
FROM ${AIRFLOW_IMAGE_NAME}



COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt



USER root

RUN apt-get update && \
    apt-get install -y wget unzip && \
    apt-get install -y gnupg && \
    apt-get install -y curl && \
    apt-get install -y python3-dev && \
    apt-get install -y apt-transport-https && \
    apt-get install -y ca-certificates && \
    apt-get install -y software-properties-common && \
    rm -rf /var/lib/apt/lists/*
