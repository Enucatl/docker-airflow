FROM python:3.14-slim

ENV PATH=/opt/repo/images/cyber-analyst/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/repo

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY packages/automation-core ./packages/automation-core
COPY images/cyber-analyst/pyproject.toml images/cyber-analyst/uv.lock ./images/cyber-analyst/
RUN uv sync --project images/cyber-analyst --frozen --no-dev --no-install-project

COPY automation ./automation
COPY dags/common ./common
COPY docs ./docs
USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
