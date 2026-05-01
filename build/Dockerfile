ARG AIRFLOW_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /opt/airflow
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project --active --inexact

USER root
RUN apt update && apt install -y ffmpeg && playwright install-deps chromium

USER airflow
RUN playwright install chromium
