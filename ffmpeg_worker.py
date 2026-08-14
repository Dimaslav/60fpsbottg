import logging
import os
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path

import imageio_ffmpeg

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
FFMPEG_TIMEOUT = int(os.getenv("FFMPEG_TIMEOUT", "1800"))
# Целевая высота видео (1080 = FullHD, 720 = HD). Ширина подстроится автоматически.
TARGET_HEIGHT = int(os.getenv("TARGET_HEIGHT", "1080"))


def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log = logging.getLogger("ffmpeg_worker")
    log.setLevel(logging.INFO)

    if log.handlers:
        return log

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

    log.addHandler(file_handler)
    log.addHandler(console_handler)
    return log


# Сначала объявляем функцию, потом уже вызываем её!
logger = setup_logging()


def log_system_resources() -> None:
    """Пытается прочитать лимиты памяти cgroup для диагностики OOM."""
    try:
        with open("/proc/meminfo", "r") as f:
            logger.info("Memory info before FFmpeg:\n%s", f.read())
    except Exception:
        pass

    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory.events",
    ]:
        try:
            with open(path) as f:
                logger.info("%s = %s", path, f.read().strip())
        except Exception:
            pass


def convert_to_60fps(input_file: str, output_file: str) -> None:
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()

    # Формируем строку фильтров:
    # 1. scale=-2:1080:flags=lanczos — увеличиваем до 1080p (ширина -2 значит "авто с четным числом")
    # 2. minterpolate — интерполяция до 60 кадров
    video_filter = f"scale=-2:{TARGET_HEIGHT}:flags=lanczos,minterpolate=fps=60:mi_mode=mci:mc_mode=obmc:me_mode=bidir:me=hexbs:search_param=8"

    cmd = [
        ffmpeg_exe,
        "-y",
        "-hide_banner",
        "-nostdin",
        "-i", input_file,

        "-map", "0:v:0",
        "-map", "0:a?",

        "-vf", video_filter,

        "-c:v", "libx264",
        "-preset", "ultrafast",  # Максимально быстрый пресет для экономии CPU/RAM
        "-crf", "21",
        "-pix_fmt", "yuv420p",
        "-threads", "1",         # Ограничиваем потоки

        "-c:a", "aac",
        "-b:a", "128k",

        "-movflags", "+faststart",
        output_file,
    ]

    log_system_resources()

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
        full_error = f"FFmpeg exit code: {result.returncode}\n{stderr[-6000:] or 'FFmpeg produced no stderr'}"
        
        # Пишем полный лог в файл
        logger.error(full_error)

        if result.returncode == -9:
            # Пользовательское сообщение для OOM
            raise RuntimeError(
                "Серверу не хватило оперативной памяти для обработки этого видео. "
                "Попробуйте видео меньшего разрешения или обратитесь к администратору."
            )
        else:
            raise RuntimeError(full_error)


def worker_main(job_queue, result_queue) -> None:
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
