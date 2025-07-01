from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="vault_postgres_example",
    start_date=datetime(2025, 6, 30),
    schedule=None,
    catchup=False,
    tags=["vault", "postgres"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_my_table",
        conn_id="data",
        sql="""
            CREATE TABLE IF NOT EXISTS my_data (
                id SERIAL PRIMARY KEY,
                value VARCHAR(255)
            );
        """,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_sample_data",
        conn_id="data",
        sql="INSERT INTO my_data (value) VALUES ('Hello from Airflow');",
    )

    create_table >> insert_data
