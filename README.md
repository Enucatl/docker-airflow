# docker-airflow

## What is this repo for?

- Build a custom docker image with extra dependencies
- Contain all the DAGs needed for my automations

## Extra dependencies in the image

- runtime dependencies are installed from `uv.lock`
- python `playwright` for browser automation

## Security baseline

This compose project uses the shared [docker-compose-security-baseline](https://github.com/Enucatl/docker-compose-security-baseline) for common container hardening defaults, including capabilities, no-new-privileges, memory/swap, and PID limits.

## SMTP in Vault

Airflow reads connections from the Vault path configured in `docker-compose.yml`:
`kv/airflow/connections/<conn_id>`.

To copy the Gmail SMTP username and password from `kv/puppet` into the Airflow
connection `smtp_default`, run:

```bash
SMTP_USERNAME="$(vault kv get -field=smtp_sasl_username kv/puppet)"
SMTP_PASSWORD="$(vault kv get -field=smtp_sasl_password kv/puppet)"

vault kv put kv/airflow/connections/smtp_default \
  conn_type="smtp" \
  host="smtp.gmail.com" \
  port="587" \
  login="${SMTP_USERNAME}" \
  password="${SMTP_PASSWORD}" \
  extra='{"disable_ssl": true}'
```

This creates the `smtp_default` connection Airflow uses for SMTP authentication.
The alert sender and recipient are set in the DAG code.

## Telegram in Vault

Airflow reads connections from the Vault path configured in `docker-compose.yml`:
`kv/airflow/connections/<conn_id>`.

To store a Telegram bot token and chat ID for the `telegram_default` connection, run:

```bash
vault kv put kv/airflow/connections/telegram_default \
  conn_type="telegram" \
  host="<chat_id>" \
  password="<bot_token>"
```

`password` stores the bot token and `host` stores the chat, channel, or group ID.
