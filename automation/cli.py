from __future__ import annotations

import argparse
import logging
import os
from automation_core.clients import notify_failure
from automation_core.connections import VaultConnections


def dispatch(name: str, vault: VaultConnections) -> None:
    from automation.pipelines import run_pipeline

    run_pipeline(name, vault)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["main", "preflight"],
    )
    args = parser.parse_args()
    pipeline = os.environ["AUTOMATION_PIPELINE"]
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    vault: VaultConnections | None = None
    try:
        vault = VaultConnections()
        if args.command == "preflight":
            vault.preflight()
        else:
            dispatch(pipeline, vault)
    except Exception as error:
        logging.exception("Pipeline %s failed", pipeline)
        if vault is not None and args.command != "preflight":
            try:
                notify_failure(vault, pipeline, error)
            except Exception:
                logging.exception("Could not send failure notification")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
