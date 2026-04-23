# Agent Operations

## Airflow task tests

When testing Airflow DAG tasks, always run `airflow tasks test` from the `airflow-cli` Docker Compose container, not from the host shell. Use the container runtime so the task executes with the same Python environment, mounted DAGs, and Airflow configuration as production.

Example:
```bash
docker compose exec -T airflow-cli airflow tasks test <dag_id> <task_id> <logical_date_or_run_id>
```
