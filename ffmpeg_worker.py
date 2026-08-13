import logging
import os
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path

import imageio_ffmpeg

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
FFMPEG_TIMEOUT = int(os.getenv("FFMPEG_TIMEOUT", "1800"))


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

    # Легкие настройки для диагностики
    cmd = [
        ffmpeg_exe,
        "-y",
        "-hide_banner",
        "-nostdin",
        "-i", input_file,
        
        "-map", "0:v:0",
        "-map", "0:a?",
        
        # Уменьшаем размер до 640px по ширине, чтобы сэкономить RAM
        "-vf", "scale=640:-2,minterpolate=fps=60",
        
        "-c:v", "libx264",
        "-preset", "ultrafast",  # Максимально быстрый пресет
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-threads", "1",         # Ограничиваем 1 потоком CPU
        
        "-c:a", "aac",
        "-b:a", "128k",
        
        "-movflags", "+faststart",
        output_file,
    ]

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=FFMPEG_TIMEOUT,
        text=True,
        errors="replace",
    )

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()

        raise RuntimeError(
            f"FFmpeg exit code: {result.returncode}\n"
            f"{stderr[-6000:] or 'FFmpeg produced no stderr'}"
        )


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

        except Exception as exc:
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
