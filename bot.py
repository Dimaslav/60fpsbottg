import asyncio
import logging
import multiprocessing as mp
import os
import queue as sync_queue
import shutil
import threading
import time
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from ffmpeg_worker import worker_main

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "10"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE_BYTES", str(50 * 1024 * 1024)))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(45 * 1024 * 1024)))
JOB_ROOT = Path(os.getenv("JOB_ROOT", "/tmp/tg60fps_jobs"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
STALE_JOB_TTL_SECONDS = int(os.getenv("STALE_JOB_TTL_SECONDS", str(24 * 60 * 60)))

active_users: set[int] = set()
active_users_lock = threading.Lock()

pending_jobs: dict[str, asyncio.Future] = {}
pending_lock = threading.Lock()


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if root.handlers:
        return

    fmt = logging.Formatter(
        "%(asctime)s | %(processName)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        LOG_DIR / "bot.log",
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    root.addHandler(file_handler)
    root.addHandler(console_handler)


setup_logging()
logger = logging.getLogger("bot")


class VideoDocumentFilter(filters.MessageFilter):
    def filter(self, message) -> bool:
        return bool(
            message.document
            and message.document.mime_type
            and message.document.mime_type.startswith("video/")
        )


VIDEO_DOCUMENT = VideoDocumentFilter()


def acquire_user_slot(user_id: int) -> bool:
    with active_users_lock:
        if user_id in active_users:
            return False
        active_users.add(user_id)
        return True


def release_user_slot(user_id: int) -> None:
    with active_users_lock:
        active_users.discard(user_id)


def cleanup_job_dir(job_dir: Path | None) -> None:
    if job_dir and job_dir.exists():
        shutil.rmtree(job_dir, ignore_errors=True)


def cleanup_stale_job_dirs() -> None:
    JOB_ROOT.mkdir(parents=True, exist_ok=True)
    now = time.time()

    for child in JOB_ROOT.iterdir():
        if not child.is_dir():
            continue
        try:
            age = now - child.stat().st_mtime
            if age > STALE_JOB_TTL_SECONDS:
                shutil.rmtree(child, ignore_errors=True)
        except FileNotFoundError:
            pass


def resolve_future(future: asyncio.Future, result: dict) -> None:
    if not future.done():
        future.set_result(result)


def result_listener(loop, result_queue, stop_event) -> None:
    listener_logger = logging.getLogger("bot.listener")

    while True:
        try:
            result = result_queue.get(timeout=1)
        except sync_queue.Empty:
            if stop_event.is_set():
                break
            continue

        if result is None:
            break

        job_id = result.get("job_id")
        with pending_lock:
            future = pending_jobs.pop(job_id, None)

        if future is None:
            listener_logger.warning("Result for unknown job: %s", job_id)
            continue

        try:
            loop.call_soon_threadsafe(resolve_future, future, result)
        except RuntimeError:
            listener_logger.warning("Event loop is closed, dropping result for job %s", job_id)


async def post_init(app):
    cleanup_stale_job_dirs()

    ctx = mp.get_context("spawn")
    job_queue = ctx.Queue(maxsize=QUEUE_MAXSIZE)
    result_queue = ctx.Queue()
    stop_event = threading.Event()

    worker = ctx.Process(
        target=worker_main,
        args=(job_queue, result_queue),
    )
    worker.start()

    loop = asyncio.get_running_loop()
    listener = threading.Thread(
        target=result_listener,
        args=(loop, result_queue, stop_event),
        daemon=True,
        name="result-listener",
    )
    listener.start()

    app.bot_data["job_queue"] = job_queue
    app.bot_data["result_queue"] = result_queue
    app.bot_data["worker_process"] = worker
    app.bot_data["stop_event"] = stop_event
    app.bot_data["listener_thread"] = listener

    logger.info("Worker started. queue maxsize=%s", QUEUE_MAXSIZE)


async def post_shutdown(app):
    stop_event = app.bot_data.get("stop_event")
    job_queue = app.bot_data.get("job_queue")
    worker = app.bot_data.get("worker_process")
    listener = app.bot_data.get("listener_thread")

    if stop_event:
        stop_event.set()

    if job_queue:
        try:
            job_queue.put_nowait(None)
        except Exception:
            pass

    if worker:
        worker.join(timeout=5)
        if worker.is_alive():
            logger.warning("Worker did not stop in time, terminating")
            worker.terminate()

    if listener and listener.is_alive():
        listener.join(timeout=2)

    logger.info("Shutdown complete")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("Отправь видео, и я попробую сделать его в 60 FPS.")


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message or not message.from_user:
        return

    user_id = message.from_user.id
    if not acquire_user_slot(user_id):
        await message.reply_text("У тебя уже есть активная задача. Подожди немного.")
        return

    status = None
    job_dir = None

    try:
        job_queue = context.application.bot_data["job_queue"]

        # Best-effort проверка перед скачиванием
        if job_queue.full():
            await message.reply_text("Очередь сейчас заполнена. Попробуй позже.")
            return

        if message.video:
            media = message.video
        elif message.document and message.document.mime_type and message.document.mime_type.startswith("video/"):
            media = message.document
        else:
            await message.reply_text("Пришли именно видео.")
            return

        if media.file_size and media.file_size > MAX_FILE_SIZE:
            await message.reply_text("Видео слишком большое для обработки.")
            return

        job_id = uuid.uuid4().hex
        job_dir = JOB_ROOT / job_id
        job_dir.mkdir(parents=True, exist_ok=False)

        input_path = job_dir / "input.mp4"
        output_path = job_dir / "output_60fps.mp4"

        status = await message.reply_text("Скачиваю видео...")

        tg_file = await context.bot.get_file(media.file_id)
        await tg_file.download_to_drive(custom_path=str(input_path))

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        with pending_lock:
            pending_jobs[job_id] = future

        job = {
            "job_id": job_id,
            "input_path": str(input_path),
            "output_path": str(output_path),
        }

        try:
            job_queue.put_nowait(job)
        except sync_queue.Full:
            with pending_lock:
                pending_jobs.pop(job_id, None)
            future.cancel()
            await status.edit_text("Очередь переполнена. Попробуй позже.")
            return

        try:
            await status.edit_text("Видео добавлено в очередь. Жду обработку...")
        except TelegramError:
            pass

        result = await future

        if not result.get("ok"):
            await message.reply_text(
                f"Не удалось обработать видео.\nFFmpeg: {result.get('error', 'unknown error')}"
            )
            return

        if not output_path.exists():
            await message.reply_text("Файл результата не найден.")
            return

        if output_path.stat().st_size > MAX_UPLOAD_BYTES:
            await message.reply_text("Результат слишком большой для отправки.")
            return

        try:
            await status.edit_text("Отправляю результат...")
        except TelegramError:
            pass

        with output_path.open("rb") as video:
            await message.reply_video(
                video=video,
                caption="Готово: 60 FPS",
                supports_streaming=True,
            )

        try:
            await status.delete()
        except TelegramError:
            pass

    except FileNotFoundError:
        logger.exception("FFmpeg not found")
        await message.reply_text("FFmpeg не установлен на сервере.")
    except TelegramError:
        logger.exception("Telegram error")
        try:
            await message.reply_text("Ошибка Telegram API.")
        except TelegramError:
            pass
    except Exception:
        logger.exception("Unexpected error")
        try:
            await message.reply_text("Произошла ошибка при обработке видео.")
        except TelegramError:
            pass
    finally:
        cleanup_job_dir(job_dir)
        release_user_slot(user_id)
        if status:
            try:
                await status.delete()
            except TelegramError:
                pass


def main() -> None:
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.VIDEO | VIDEO_DOCUMENT, handle_video))

    app.run_polling()


if __name__ == "__main__":
    main()