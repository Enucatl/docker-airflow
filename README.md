# Scheduled automation runners

The four production automations run as short-lived Python 3.14 containers. Each
pipeline directory under `images/` has its own Dockerfile, `pyproject.toml`,
lockfile, image, and dependency environment:

Shared Vault, notification, SMTP, and PostgreSQL client code is packaged as
`packages/automation-core`. The library has no lockfile; each runnable project
locks the resolved version of the local package and its transitive dependencies.

- `exam`: Playwright/Chromium and PostgreSQL
- `download-zanzara`: requests and ffmpeg
- `puppet-release-watch`: requests
- `cyber-analyst`: PostgreSQL, LangGraph/LangChain, and OpenTelemetry

Only `postgres-outputs` is long-lived. The runner services are behind the
`runner` Compose profile and do not start with a normal `docker compose up -d`.
Puppet manages the UTC systemd timers. A normal manual run uses, for example:

```bash
systemctl start automation-exam.service
```

Every service runs `docker compose run --build --rm --no-deps`, so changed
source, lockfile, or image instructions are rebuilt before execution.

## Vault preflight

Connections remain at `kv/airflow/connections/<connection_id>`. Every process
authenticates with `/run/secrets/fullchain`, `/run/secrets/key`, and
`VAULT_CACERT`; tokens are neither stored nor renewed. Before cutover, build an
image and check all required connection payloads without business side effects:

```bash
docker compose run --build --rm --no-deps exam preflight
```

## Cyber analyst cutover watermark

Before stopping Airflow, capture the final successful scheduled logical date:

```bash
BOUNDARY="$(docker compose exec -T postgres psql -U airflow -d airflow -Atc \
  "SELECT max(logical_date) FROM dag_run WHERE dag_id = 'cyber_analyst' AND run_type = 'scheduled' AND state = 'success'")"
test -n "$BOUNDARY"
```

After deploying the new Compose file and starting `postgres-outputs`, seed that
captured value idempotently:

```bash
scripts/seed-cyber-watermark "$BOUNDARY"
```

The monthly runner refuses to run without a seed and advances the boundary only
after the report email is delivered successfully.

## Cutover checks

```bash
systemctl daemon-reload
systemctl list-timers 'automation-*'
docker compose ps --services --filter status=running
```

The idle service list must contain only `postgres-outputs`. Once the watermark
is seeded and timers are installed, stop the former Airflow stack and remove its
metadata PostgreSQL, Redis, log, and virtualenv volumes. Preserve
`postgres-outputs-volume`.
