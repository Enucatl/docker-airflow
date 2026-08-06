FROM python:3.14-slim

ENV PATH=/opt/repo/images/puppet-release-watch/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/repo

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY packages/automation-core ./packages/automation-core
COPY images/puppet-release-watch/pyproject.toml images/puppet-release-watch/uv.lock ./images/puppet-release-watch/
RUN uv sync --project images/puppet-release-watch --frozen --no-dev --no-install-project

COPY automation ./automation
USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
