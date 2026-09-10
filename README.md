# Scheduled automation runners

Short-lived Python 3.14 containers scheduled by Puppet-managed systemd timers.
Each timer runs `docker compose run --build --rm --no-deps <service>` from
`/opt/docker/airflow`. Only `postgres-outputs` is long-lived; runner services use
the Compose `runner` profile and do not start with a normal `docker compose up -d`.

## Layout

- `packages/` — installable uv packages (`uv_build`), each using a `src/` layout: shared libs and one package per pipeline
  - `automation-core` — Vault, Postgres, SMTP, Telegram
  - `common` — Loki/SSL/Suricata helpers
  - `automation` — CLI entrypoint (`python -m automation.cli`)
  - `exam`, `download-zanzara`, `puppet-release-watch`, `cyber-analyst`,
    `operations-analyst`, `podcast-statistics` — pipeline implementations
- `images/<pipeline>/` — per-pipeline Dockerfile + thin Compose app project (`package = false`)
- `config/` — Postgres init and operations-analyst repository manifest
- `scripts/` — operational helpers

This is a uv workspace (`packages/*`, `images/*`) with a single root `uv.lock`.

## Pipelines

| Service | Package | Schedule (UTC, via Puppet) |
|---------|---------|----------------------------|
| `exam` | `exam` | hourly `*:00:00` |
| `download-zanzara` | `download-zanzara` | daily `00:00:00` |
| `cyber-analyst` | `cyber-analyst` | monthly `01 03:00:00` |
| `operations-analyst` | `operations-analyst` | Friday `02:00:00` |
| `podcast-statistics` | `podcast-statistics` | every 15 min `*:00/15:00` |
| `puppet-release-watch` | `puppet-release-watch` | image/CI only (no systemd timer) |

Manual run example:

```bash
systemctl start automation-exam.service
```

## Vault preflight

Connections remain at `kv/airflow/connections/<connection_id>`. Processes
authenticate with `/run/secrets/fullchain`, `/run/secrets/key`, and `VAULT_CACERT`.

```bash
docker compose run --build --rm --no-deps exam preflight
```

## Operations analyst

Runs Friday at 02:00 UTC. Synchronizes repositories from
`config/operations-repositories.json`, analyzes operational errors, and emails
diagnoses. LLM endpoint: Vault connection `operations_analyst_openrouter`.

## Cutover checks

```bash
systemctl daemon-reload
systemctl list-timers 'automation-*'
docker compose ps --services --filter status=running
```

The idle service list must contain only `postgres-outputs`.
