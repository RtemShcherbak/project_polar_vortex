FROM apache/airflow:2.9.3

USER root

ENV AIRFLOW_HOME=/opt/airflow

RUN apt-get update && apt-get install -y \
    build-essential \
    libeccodes-dev \
    libnetcdf-dev \
    && apt-get clean

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/airflow/dags /opt/airflow/logs

WORKDIR /opt/airflow
