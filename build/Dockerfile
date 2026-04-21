ARG AIRFLOW_VERSION=3.2.0
FROM apache/airflow:${AIRFLOW_VERSION}

RUN uv pip install apache-airflow==${AIRFLOW_VERSION} playwright

USER root
RUN apt update && apt install -y ffmpeg && playwright install-deps chromium

USER airflow
RUN playwright install chromium
