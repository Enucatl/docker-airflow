FROM python:3.14-slim

ENV PATH=/opt/repo/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/repo

RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /opt/repo \
    --shell /usr/sbin/nologin automation

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
COPY packages/automation-core/pyproject.toml ./packages/automation-core/
COPY packages/automation/pyproject.toml ./packages/automation/
COPY packages/download-zanzara/pyproject.toml ./packages/download-zanzara/
COPY images/download-zanzara/pyproject.toml ./images/download-zanzara/
RUN uv sync --project images/download-zanzara --frozen --no-dev --no-install-workspace \
    && apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY packages ./packages
RUN uv sync --project images/download-zanzara --frozen --no-dev --no-install-project

USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
