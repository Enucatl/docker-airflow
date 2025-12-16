from airflow.sdk import task, Param, DAG

from common.defaults import default_args


with DAG(
    "download_zanzara",
    default_args=default_args,
    description="Downloads and converts La Zanzara episodes to Opus",
    schedule="@daily",
    params={
        "date_from": Param(default=None, type=["null", "string"], format="date", description="Start Date (YYYY-MM-DD). Leave empty for scheduled run."),
        "date_to": Param(default=None, type=["null", "string"], format="date", description="End Date (YYYY-MM-DD). Leave empty for scheduled run."),
    },
    render_template_as_native_obj=True
) as dag:


    @task.virtualenv(
        task_id="download",
        requirements=[
            "requests",
        ],
    )
    def download(data_interval_start, data_interval_end, params):
        """
        Determines if this is a Scheduled run or a Manual Range run.
        """
        import requests
        import logging
        import subprocess
        import shutil
        from pathlib import Path
        from datetime import datetime, timedelta, timezone

        from airflow.exceptions import AirflowSkipException

        def download_and_convert_single_day(target_date: datetime, overwrite: bool = False) -> None:
            """
            Helper function to handle the download and conversion for a single date.
            """
            date_str = target_date.strftime("%y%m%d")
            year_str = target_date.strftime("%Y")
            
            # Define paths
            OUTPUT_FOLDER = Path("/scratch/archive/zanzara")
            OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
            mp3_filename = f"{date_str}-lazanzara.mp3"
            opus_filename = f"{date_str}-lazanzara.opus"
            
            mp3_path = OUTPUT_FOLDER / mp3_filename
            opus_path = OUTPUT_FOLDER / opus_filename

            # 1. Check if final OPUS exists. If so, skip to save bandwidth/cpu.
            if opus_path.exists() and not overwrite:
                logging.info(f"Opus file already exists: {opus_path}. Skipping.")
                raise AirflowSkipException

            # 2. Download MP3
            url = f"https://podcast-radio24.ilsole24ore.com/radio24_audio/{year_str}/{mp3_filename}"
            logging.info(f"Requesting: {url}")
            
            response = requests.get(url, stream=True)
            
            if response.status_code in [404, 521]:
                logging.warning(f"{response.status_code=}")
                raise AirflowSkipException
            
            response.raise_for_status()

            # Write MP3 temporarily
            with open(mp3_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Downloaded MP3: {mp3_path}")

            # 3. Convert to Opus using FFmpeg with long arguments for clarity
            cmd = [
                "ffmpeg",
                "-n",                       # Do not overwrite output if it exists
                "-loglevel", "error",       # Suppress logs
                "-i", str(mp3_path),        # Input file
                "-ac", "1",                 # Audio Channels: 1 (Downmix to Mono)
                "-c:a", "libopus",  # Audio Codec (Long for -c:a)
                "-b:a", "40k",    # Audio Bitrate (Long for -b:a)
                "-vbr", "on",               # Variable Bit Rate
                "-map_metadata", "0",       # Copy metadata from input to output
                str(opus_path)              # Output file
            ]

            logging.info(f"Running conversion: {' '.join(cmd)}")
            try:
                subprocess.run(cmd, check=True)
                logging.info(f"Conversion successful: {opus_path}")
                
                # 4. Cleanup MP3
                mp3_path.unlink()
                logging.info("Deleted temporary MP3 file.")
                
            except subprocess.CalledProcessError as e:
                logging.error(f"FFmpeg failed: {e}")
                # Clean up partial files if necessary
                if mp3_path.exists(): mp3_path.unlink()
                if opus_path.exists(): opus_path.unlink()
                raise e

        date_from = params.get("date_from")
        date_to = params.get("date_to")

        print(f"{date_from=}, {date_to=}")

        # CASE A: Manual Trigger with Date Range
        if date_from is not None:
            if date_to is None:
                date_to = data_interval_end
            logging.info(f"Manual Mode detected. Processing range: {date_from} to {date_to}")
            current_date = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            while current_date <= date_to:
                try:
                    result = download_and_convert_single_day(current_date)
                except AirflowSkipException:
                    logging.info(f"{current_date=} skipped")
                current_date += timedelta(days=1)

        # CASE B: Standard Scheduled Run (Single Day)
        else:
            logging.info(f"Scheduled Mode detected. Processing single date: {data_interval_start}")
            
            result = download_and_convert_single_day(data_interval_start)
            
            if result == "SKIPPED":
                # In single day mode, we WANT the task to show as Skipped (Pink) in UI
                raise AirflowSkipException(f"No episode found for {target_date.date()}")

    download()
