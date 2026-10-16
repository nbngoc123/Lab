FROM apache/airflow:3.3.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

COPY infrastructure/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

