from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging
from pathlib import Path
import subprocess

import requests

from automation_core.connections import VaultConnections

ARCHIVE = Path("/scratch/archive/zanzara")


def scheduled_date_range(now: datetime) -> tuple[datetime, datetime]:
    end = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return end - timedelta(days=1), end


def download_day(target_date: datetime) -> None:
    date = target_date.strftime("%y%m%d")
    mp3 = ARCHIVE / f"{date}-lazanzara.mp3"
    opus = ARCHIVE / f"{date}-lazanzara.opus"
    if opus.exists():
        logging.info("Opus file already exists: %s", opus)
        return
    response = requests.get(
        f"https://podcast-radio24.ilsole24ore.com/radio24_audio/{target_date:%Y}/{mp3.name}",
        stream=True,
        timeout=60,
    )
    if response.status_code in (404, 521):
        logging.info(
            "No episode for %s: HTTP %s", target_date.date(), response.status_code
        )
        return
    response.raise_for_status()
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    try:
        with mp3.open("wb") as output:
            for chunk in response.iter_content(chunk_size=8192):
                output.write(chunk)
        subprocess.run(
            [
                "ffmpeg",
                "-n",
                "-loglevel",
                "error",
                "-i",
                str(mp3),
                "-ac",
                "1",
                "-c:a",
                "libopus",
                "-b:a",
                "40k",
                "-vbr",
                "on",
                "-map_metadata",
                "0",
                str(opus),
            ],
            check=True,
        )
        mp3.unlink()
    except Exception:
        mp3.unlink(missing_ok=True)
        opus.unlink(missing_ok=True)
        raise


def run(_vault: VaultConnections) -> None:
    start, end = scheduled_date_range(datetime.now(UTC))
    current = start
    while current <= end:
        download_day(current)
        current += timedelta(days=1)
