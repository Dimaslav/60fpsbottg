import logging
import os
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
FFMPEG_TIMEOUT = int(os.getenv("FFMPEG_TIMEOUT", "300"))
FFMPEG_PRESET = os.getenv("FFMPEG_PRESET", "fast")
FFMPEG_CRF = os.getenv("FFMPEG_CRF", "20")
FALLBACK_CRF = os.getenv("FALLBACK_CRF", "28")
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(45 * 1024 * 1024)))


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


def _run_ffmpeg(cmd: list[str]) -> None:
    subprocess.run(
        cmd,
        check=True,
        timeout=FFMPEG_TIMEOUT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def convert_to_60fps(input_file: str, output_file: str) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
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
        FFMPEG_PRESET,
        "-crf",
        FFMPEG_CRF,
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        output_file,
    ]
    _run_ffmpeg(cmd)


def recompress_if_oversized(output_file: str, logger: logging.Logger) -> None:
    """Если результат не влезает в лимит отправки — пережимает сильнее.

    Кодирование идёт из уже готового 60 FPS-файла, так что повторная
    интерполяция кадров не нужна и второй проход заметно дешевле первого.
    """
    size = os.path.getsize(output_file)
    if size <= MAX_UPLOAD_BYTES:
        return

    logger.info(
        "Output %.1f MB exceeds %.1f MB, recompressing with crf=%s",
        size / (1024 * 1024),
        MAX_UPLOAD_BYTES / (1024 * 1024),
        FALLBACK_CRF,
    )

    tmp_file = output_file + ".small.mp4"
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-i",
        output_file,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "veryfast",
        "-crf",
        FALLBACK_CRF,
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        tmp_file,
    ]
    _run_ffmpeg(cmd)
    os.replace(tmp_file, output_file)


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

        logger.info("Job %s started", job_id)

        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            convert_to_60fps(input_path, output_path)

            try:
                recompress_if_oversized(output_path, logger)
            except Exception:
                logger.exception(
                    "Job %s fallback recompression failed, keeping original output", job_id
                )

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

        except subprocess.CalledProcessError as exc:
            err = (exc.stderr or "ffmpeg error").strip()
            if len(err) > 2000:
                err = err[:2000]

            logger.exception("Job %s failed: %s", job_id, err)
            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": False,
                    "error": err,
                }
            )

        except FileNotFoundError:
            msg = "FFmpeg not found"
            logger.exception("Job %s failed: %s", job_id, msg)
            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": False,
                    "error": msg,
                }
            )

        except Exception as exc:
            msg = str(exc)
            logger.exception("Job %s unexpected error", job_id)
            result_queue.put(
                {
                    "job_id": job_id,
                    "ok": False,
                    "error": msg,
                }
            )
