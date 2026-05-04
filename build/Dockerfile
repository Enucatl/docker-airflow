ARG AIRFLOW_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /opt/airflow
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project --active --inexact
COPY --chown=airflow:0 airflow_utilities ./airflow_utilities

USER root
RUN apt update && apt install -y ffmpeg && playwright install-deps chromium

USER airflow
RUN uv pip install --target "$(python -c 'import site; print(site.getusersitepackages())')" --no-deps ./airflow_utilities
RUN rm -rf ./airflow_utilities
RUN playwright install chromium
