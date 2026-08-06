FROM python:3.14-slim

ENV PATH=/opt/repo/images/exam/.venv/bin:$PATH \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/repo

RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /opt/repo \
    --shell /usr/sbin/nologin automation

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY packages/automation-core ./packages/automation-core
COPY images/exam/pyproject.toml images/exam/uv.lock ./images/exam/
RUN uv sync --project images/exam --frozen --no-dev --no-install-project \
    && uv run --project images/exam playwright install-deps chromium \
    && rm -rf /var/lib/apt/lists/* \
    && uv run --project images/exam playwright install chromium

COPY automation ./automation
COPY dags/common ./common
USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
