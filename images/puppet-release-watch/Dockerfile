FROM python:3.14-slim

ENV PATH=/opt/repo/images/puppet-release-watch/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/repo

RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /opt/repo \
    --shell /usr/sbin/nologin automation

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY images/puppet-release-watch/pyproject.toml ./images/puppet-release-watch/
RUN uv sync --project images/puppet-release-watch --frozen --no-dev --no-install-project

USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
