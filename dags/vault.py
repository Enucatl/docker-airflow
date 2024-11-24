from datetime import timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    "start_date": "2024-11-22",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
}

requirements = [
    "apache-airflow",
    "hvac",
]

venv_cache_path = "/opt/airflow/venv"

with DAG(
    "vault",
    default_args=default_args,
    description="Renew Vault token if expiring soon",
    schedule="@daily",
    catchup=False,
) as dag:

    @task.virtualenv(
        task_id="check_and_renew_token",
        requirements=requirements,
        venv_cache_path=venv_cache_path,
    )
    def check_and_renew_token() -> None:
        import hvac
        from airflow.hooks.base import BaseHook

        from common.ssl import verify

        """
        Check the Vault token expiration and renew if necessary.
        """
        # Get the Vault connection using BaseHook
        conn = BaseHook.get_connection("vault")

        # Extract the current token from the connection
        current_token = conn.password
        if not current_token:
            raise ValueError("No token found in Vault connection")

        # Initialize Vault client
        vault_client = hvac.Client(url=conn.host, token=current_token, verify=verify)

        # Look up token information
        token_info = vault_client.auth.token.lookup_self()

        # Calculate remaining time
        ttl = token_info["data"]["ttl"]
        expires_in_days = ttl / (24 * 3600)  # Convert seconds to days
        print(f"{expires_in_days=}")

        if expires_in_days < 5:
            # Renew the token
            renewal_response = vault_client.auth.token.renew_self()
            new_token = renewal_response["auth"]["client_token"]

            # Update the connection with the new token
            conn.password = new_token

            # Save the updated connection
            conn.set_connection()

    check_and_renew_token_task = check_and_renew_token()
