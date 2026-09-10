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
COPY packages ./packages
COPY images/cyber-analyst/pyproject.toml ./images/cyber-analyst/
RUN uv sync --project images/cyber-analyst --frozen --no-dev --no-install-project

USER 50000:0
ENTRYPOINT ["python", "-m", "automation.cli"]
CMD ["main"]
