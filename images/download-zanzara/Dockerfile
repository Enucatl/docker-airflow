FROM python:3.14-slim

ENV PATH=/opt/repo/images/download-zanzara/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/repo

RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /opt/repo \
    --shell /usr/sbin/nologin automation

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY packages/automation-core ./packages/automation-core
COPY images/download-zanzara/pyproject.toml images/download-zanzara/uv.lock ./images/download-zanzara/
RUN uv sync --project images/download-zanzara --frozen --no-dev --no-install-project \
    && apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY automation ./automation
USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
