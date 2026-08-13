import logging
import os
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path

import imageio_ffmpeg

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
FFMPEG_TIMEOUT = int(os.getenv("FFMPEG_TIMEOUT", "300"))


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ffmpeg_worker")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(processName)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        LOG_DIR / "worker.log",
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def convert_to_60fps(input_file: str, output_file: str) -> None:
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()

    cmd = [
        ffmpeg_exe,
        "-y",
        "-hide_banner",
        "-nostdin",
        "-i",
        input_file,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        "minterpolate=fps=60",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "medium",
        "-crf",
        "18",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        output_file,
    ]

    # Захватываем и stdout, и stderr
    result = subprocess.run(
        cmd,
        timeout=FFMPEG_TIMEOUT,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        # Склеиваем вывод ошибки
        err = (result.stderr + "\n" + result.stdout).strip()
        if not err:
            err = f"FFmpeg exited with code {result.returncode} but produced no output."
        raise RuntimeError(err)


def worker_main(job_queue, result_queue) -> None:
    logger = setup_logging()
    logger.info("Worker process started")

    while True:
        job = job_queue.get()

        if job is None:
            logger.info("Stop signal received")
            break

        job_id = job["job_id"]
        input_path = job["input_path"]
        output_path = job["output_path"]

        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            convert_to_60fps(input_path, output_path)

            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": True,
                    "output_path": output_path,
                }
            )
            logger.info("Job %s completed", job_id)

        except subprocess.TimeoutExpired:
            msg = f"FFmpeg timeout after {FFMPEG_TIMEOUT}s"
            logger.exception("Job %s timeout", job_id)
            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": False,
                    "error": msg,
                }
            )

        except FileNotFoundError:
            msg = "FFmpeg not found in worker container"
            logger.exception("Job %s failed: %s", job_id, msg)
            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": False,
                    "error": msg,
                }
            )

        except Exception as exc:
            # Теперь сюда прилетит RuntimeError с полным текстом ошибки FFmpeg
            msg = str(exc)
            if len(msg) > 2000:
                msg = msg[:2000]
            logger.exception("Job %s unexpected error", job_id)
            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": False,
                    "error": msg,
                }
            )
