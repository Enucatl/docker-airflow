from __future__ import annotations

import argparse
import logging
import os

from automation_core.clients import notify_failure
from automation_core.connections import VaultConnections


def run_pipeline(name: str, vault: VaultConnections) -> None:
    if name == "exam":
        from exam import run
    elif name == "download-zanzara":
        from download_zanzara import run
    elif name == "puppet-release-watch":
        from puppet_release_watch import run
    elif name == "cyber-analyst":
        from cyber_analyst import run
    elif name == "operations-analyst":
        from operations_analyst import run
    elif name == "podcast-statistics":
        from podcast_statistics.pipeline import run
    else:
        raise ValueError(f"Unknown pipeline: {name}")
    run(vault)


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
            run_pipeline(pipeline, vault)
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
