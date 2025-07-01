from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="vault_postgres_example",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["vault", "postgres"],
) as dag:
    create_table = PostgresOperator(
        task_id="create_my_table",
        postgres_conn_id="data",
        sql="""
            CREATE TABLE IF NOT EXISTS my_data (
                id SERIAL PRIMARY KEY,
                value VARCHAR(255)
            );
        """,
    )

    insert_data = PostgresOperator(
        task_id="insert_sample_data",
        postgres_conn_id="data",  # Airflow fetches this from Vault!
        sql="INSERT INTO my_data (value) VALUES ('Hello from Airflow');",
    )

    create_table >> insert_data
