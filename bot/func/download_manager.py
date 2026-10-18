import asyncio
import os
import time

from bot import config
from bot.func.pyroutils.progress import progress_for_pyrogram
from bot.logger import get_logger

log = get_logger(__name__)

_dl_sem = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)

# How many times to retry a failed or empty download before giving up.
_MAX_DL_RETRIES = 3
# Seconds to wait between retry attempts.
_DL_RETRY_DELAY = 5.0


async def _download_once(client, file_id, destination, message, ud_type, job):
    """Single download attempt. Returns the path on success, raises on failure."""
    start = time.time()
    kwargs = {"file_name": destination}
    if message is not None:
        kwargs["progress"]      = progress_for_pyrogram
        kwargs["progress_args"] = (ud_type, message, start)

    dl_task = asyncio.create_task(client.download_media(file_id, **kwargs))

    while not dl_task.done():
        if job is not None and job.cancel_requested:
            dl_task.cancel()
            try:
                await dl_task
            except (asyncio.CancelledError, Exception):
                pass
            raise asyncio.CancelledError("Download cancelled")
        try:
            await asyncio.wait_for(asyncio.shield(dl_task), timeout=1.0)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            dl_task.cancel()
            raise

    path = dl_task.result()
    elapsed = time.time() - start

    # Treat a zero-byte result as a failure so we retry rather than passing a
    # corrupt/empty file to ffmpeg.
    size = os.path.getsize(path) if path and os.path.isfile(path) else 0
    if size == 0:
        raise RuntimeError(f"Downloaded file is empty (0 bytes): {path}")

    log.info("Downloaded %s -> %s (%.1fs, %d bytes)", file_id, path, elapsed, size)
    return path


async def download(
    client,
    file_id: str,
    destination: str,
    message=None,
    ud_type: str = "Downloading",
    job=None,
):
    os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)

    async with _dl_sem:
        last_exc = None
        for attempt in range(1, _MAX_DL_RETRIES + 1):
            # Remove any partial file left by a previous attempt before retrying
            # so Pyrogram doesn't append to it.
            if attempt > 1 and os.path.exists(destination):
                try:
                    os.remove(destination)
                except Exception:
                    pass

            try:
                return await _download_once(
                    client, file_id, destination, message, ud_type, job
                )
            except asyncio.CancelledError:
                # Propagate cancellation immediately — don't retry.
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_DL_RETRIES:
                    log.warning(
                        "Download attempt %d/%d failed for %s: %s — retrying in %.0fs",
                        attempt, _MAX_DL_RETRIES, file_id, exc, _DL_RETRY_DELAY,
                    )
                    await asyncio.sleep(_DL_RETRY_DELAY)
                else:
                    log.error(
                        "Download failed after %d attempts for %s: %s",
                        _MAX_DL_RETRIES, file_id, exc,
                    )

        raise last_exc
