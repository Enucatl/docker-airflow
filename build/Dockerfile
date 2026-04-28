ARG AIRFLOW_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /opt/airflow
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project --active

USER root
RUN apt update && apt install -y ffmpeg && playwright install-deps chromium

USER airflow
RUN playwright install chromium
