import asyncio
import os
import time

from bot import config
from bot.func.pyroutils.progress import progress_for_pyrogram
from bot.func.telegram_retry import tg_call
from bot.logger import get_logger

log = get_logger(__name__)

# Telegram only supports streaming video for these formats via send_video
_VIDEO_STREAMING_EXTS = {".mp4", ".mov", ".m4v"}

_up_sem = asyncio.Semaphore(config.MAX_CONCURRENT_UPLOADS)


# Upload timeout: 2 hours max for a single file upload attempt
_UPLOAD_TIMEOUT = 7200

async def upload(
    client,
    chat_id: int,
    path: str,
    thumb: str = None,
    caption: str = "",
    as_video: bool = True,
    reply_to: int = None,
    message=None,
    file_name: str = None,
    ud_type: str = "Uploading",
):
    ext = os.path.splitext(path)[1].lower()
    # Force document upload for formats Telegram can't stream (mkv, avi, etc.)
    if as_video and ext not in _VIDEO_STREAMING_EXTS:
        log.info("Output is %s, switching from send_video to send_document", ext)
        as_video = False

    file_size = os.path.getsize(path) if os.path.isfile(path) else 0
    log.info(
        "Uploading %s (%.1f MB) to chat %s as %s",
        path, file_size / 1024 / 1024, chat_id, "video" if as_video else "document",
    )

    async with _up_sem:
        start = time.time()
        progress_kwargs = {}
        if message is not None:
            progress_kwargs["progress"]      = progress_for_pyrogram
            progress_kwargs["progress_args"] = (ud_type, message, start)

        # NOTE: Do NOT wrap large file uploads in tg_call().
        # tg_call retries by calling the lambda again which restarts the upload
        # from byte 0 each time — on a 500 MB+ file this causes silent multi-hour
        # hangs. Instead we upload once with a hard wall timeout, and only handle
        # FloodWait manually.
        MAX_FLOOD_RETRIES = 3
        for attempt in range(MAX_FLOOD_RETRIES + 1):
            try:
                if as_video:
                    coro = client.send_video(
                        chat_id,
                        path,
                        thumb                = thumb,
                        caption              = caption,
                        supports_streaming   = True,
                        reply_to_message_id  = reply_to,
                        file_name            = file_name,
                        **progress_kwargs,
                    )
                else:
                    coro = client.send_document(
                        chat_id,
                        path,
                        thumb               = thumb,
                        caption             = caption,
                        reply_to_message_id = reply_to,
                        file_name           = file_name,
                        **progress_kwargs,
                    )

                result = await asyncio.wait_for(coro, timeout=_UPLOAD_TIMEOUT)
                log.info("Upload complete for %s (%.1fs)", path, time.time() - start)
                return result

            except asyncio.TimeoutError:
                log.error(
                    "Upload timed out after %ds for %s (attempt %d/%d)",
                    _UPLOAD_TIMEOUT, path, attempt + 1, MAX_FLOOD_RETRIES + 1,
                )
                raise RuntimeError(
                    f"Upload timed out after {_UPLOAD_TIMEOUT // 60} minutes"
                )

            except Exception as exc:
                name = type(exc).__name__
                # FloodWait: Telegram asks us to wait — honour it and retry once
                if "FloodWait" in name and attempt < MAX_FLOOD_RETRIES:
                    wait = getattr(exc, "value", 30)
                    log.warning(
                        "FloodWait %ds during upload attempt %d — waiting",
                        wait, attempt + 1,
                    )
                    await asyncio.sleep(wait)
                    # Reset progress start time so speed/ETA is accurate after wait
                    start = time.time()
                    if message is not None:
                        progress_kwargs["progress_args"] = (ud_type, message, start)
                    continue

                log.exception("Upload failed for %s: %s", path, exc)
                raise
