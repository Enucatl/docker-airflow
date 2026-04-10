# docker-airflow

## What is this repo for?

- Build a custom docker image with extra dependencies
- Contain all the DAGs needed for my automations

## Extra dependencies in the image

- python `playwright` for browser automation

## Security baseline

This compose project uses the shared [docker-compose-security-baseline](https://github.com/Enucatl/docker-compose-security-baseline) for common container hardening defaults, including capabilities, no-new-privileges, memory/swap, and PID limits.
