ARG AIRFLOW_VERSION=2.10.3
FROM apache/airflow:${AIRFLOW_VERSION}
RUN pip install apache-airflow==${AIRFLOW_VERSION} playwright
USER root
RUN playwright install-deps chromium
USER airflow
RUN playwright install chromium
