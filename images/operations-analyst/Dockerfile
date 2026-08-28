FROM python:3.14-slim

ENV PATH=/opt/repo/images/operations-analyst/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /opt/repo

RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /opt/repo \
    --shell /usr/sbin/nologin automation && apt-get update && \
    apt-get install --yes --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY packages/automation-core ./packages/automation-core
COPY images/operations-analyst/pyproject.toml images/operations-analyst/uv.lock ./images/operations-analyst/
RUN uv sync --project images/operations-analyst --frozen --no-dev --no-install-project
COPY automation ./automation
COPY dags/common ./common
COPY config/operations-repositories.json ./config/operations-repositories.json
USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
