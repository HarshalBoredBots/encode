import asyncio
import copy
import os
import re
import resource
import shutil
import time

from pyrogram import enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot import config
from bot.logger import get_logger
from bot.utils.format import TimeFormatter, humanbytes
from bot.func import download_manager, upload_manager
from bot.func.ffmpeg_utils import probe_video, generate_ffmpeg_cmd
from bot.func.media import extract_thumbnail
from bot.func.telegram_retry import tg_call
import database


log = get_logger(__name__)

# Matches both -stats format: time=HH:MM:SS.ms
# and -progress format:       out_time=HH:MM:SS.ms
_TIME_RE = re.compile(rb"(?:out_)?time=(\d+):(\d+):([\d.]+)")


def _cancel_markup(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🛑 Cancel", callback_data=f"cancel_job:{job_id}")
    ]])


def _encode_bar(pct: float, cur: float, total: float, speed_fps: float,
                res: str, idx: int, total_res: int,
                codec: str, crf, preset: str, job_id: str) -> str:
    """Build the encoding progress message."""
    filled = int(pct * 10)
    bar    = "█" * filled + "─" * (10 - filled)
    done   = f"{idx + 1}/{total_res}"

    # fps speed line (only when available)
    speed_line = f"⚡ {speed_fps:.1f} fps" if speed_fps > 0 else ""

    # time progress
    cur_fmt   = TimeFormatter(int(cur))
    total_fmt = TimeFormatter(int(total)) if total > 0 else "?"

    return (
        f"🎬 <b>Encoding {res}</b>  ({done})\n"
        f"<code>[{bar}]</code> <b>{int(pct * 100)}%</b>\n"
        f"⏱ {cur_fmt} / {total_fmt}"
        + (f"  ·  {speed_line}" if speed_line else "") + "\n"
        f"🎞 {codec}  ·  CRF {crf}  ·  {preset}"
    )


# fps= appears in both -stats and -progress output
_FPS_RE = re.compile(rb"fps=\s*([\d.]+)")


def _ffmpeg_preexec():
    """
    Called in the child process after fork() but before exec().

    RLIMIT_AS (virtual address space) cap has been intentionally removed.
    FFmpeg and x265 are mmap-heavy: they reserve large virtual address ranges
    that are never fully committed to physical RAM. Capping virtual AS causes
    malloc() failures inside x265 even when actual RSS is only ~200 MB, because
    x265 reserves much larger virtual regions upfront. On a 64-bit dyno virtual
    address space is ~128 TB — there is no reason to cap it. Physical RAM (RSS)
    is the actual constraint and is controlled by the x265-params in ffmpeg_utils.py
    (pools=1, frame-threads=1, wpp=0, pmode=0, pme=0, merange=16, rc-lookahead=5).
    """
    pass


async def _run_ffmpeg(cmd, job, status_msg, duration, settings):
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        preexec_fn=_ffmpeg_preexec,
    )
    # Store proc on the job so queue_manager.stop() can kill it immediately
    # on Heroku SIGTERM without waiting for the encode to finish.
    job.ffmpeg_proc = proc

    last_update = 0.0
    last_log    = 0.0
    LOG_INTERVAL = 60.0
    stderr_lines = []
    v = settings["video"]

    async def read_stderr():
        nonlocal last_update, last_log
        # FFmpeg writes stats with \r (carriage return), NOT \n (newline).
        # readline() blocks forever waiting for \n that never comes.
        # Instead we read raw chunks and split on both \r and \n ourselves.
        buf = b""
        while True:
            if job.cancel_requested:
                try:
                    proc.kill()
                except Exception:
                    pass
                return

            try:
                chunk = await asyncio.wait_for(
                    proc.stderr.read(4096), timeout=5.0
                )
            except asyncio.TimeoutError:
                # No data for 5s — emit heartbeat and check if proc is done
                now = time.time()
                if now - last_log >= LOG_INTERVAL:
                    last_log = now
                    log.info(
                        "FFmpeg running (no output) job=%s pid=%d",
                        job.job_id[:8], proc.pid,
                    )
                if proc.returncode is not None:
                    break
                continue
            except Exception:
                break

            if not chunk:
                break

            buf += chunk
            # Split on both \r and \n so we catch FFmpeg's \r-terminated stats
            parts = re.split(rb"\r|\n", buf)
            buf = parts[-1]  # keep incomplete last part in buffer

            for line in parts[:-1]:
                line = line.strip()
                if not line:
                    continue

                decoded = line.decode(errors="replace")
                if decoded:
                    stderr_lines.append(decoded)

                now = time.time()
                m   = _TIME_RE.search(line)
                if not m or duration <= 0:
                    if now - last_log >= LOG_INTERVAL:
                        last_log = now
                        log.info(
                            "FFmpeg running (no progress yet) job=%s pid=%d",
                            job.job_id[:8], proc.pid,
                        )
                    continue

                h, mm, ss = int(m.group(1)), int(m.group(2)), float(m.group(3))
                cur = h * 3600 + mm * 60 + ss
                pct = min(cur / duration, 1.0)

                fps_m     = _FPS_RE.search(line)
                speed_fps = float(fps_m.group(1)) if fps_m else 0.0

                # Heroku log heartbeat every 60 s — shared timer with the
                # "no progress yet" branch above so they don't interleave.
                if now - last_log >= LOG_INTERVAL:
                    last_log = now
                    log.info(
                        "Encoding [%s] job=%s res=%s %.1f%% (%s / %s) "
                        "crf=%s preset=%s fps=%.1f pid=%d",
                        job.file_name or "unknown",
                        job.job_id[:8],
                        v.get("resolution", ["?"])[0],
                        pct * 100,
                        TimeFormatter(int(cur)),
                        TimeFormatter(int(duration)),
                        v.get("crf", "?"),
                        v.get("preset", "?"),
                        speed_fps,
                        proc.pid,
                    )

                # Telegram progress update (throttled)
                if now - last_update < config.UI_UPDATE_INTERVAL:
                    continue
                last_update = now

                if status_msg is not None:
                    resolutions  = list(v.get("resolution") or ["720p"])
                    current_res  = resolutions[0]
                    res_idx      = getattr(job, "_current_res_idx", 0)

                    text = _encode_bar(
                        pct        = pct,
                        cur        = cur,
                        total      = duration,
                        speed_fps  = speed_fps,
                        res        = current_res,
                        idx        = res_idx,
                        total_res  = len(resolutions),
                        codec      = v.get("codec", "libx264"),
                        crf        = v.get("crf", "?"),
                        preset     = v.get("preset", "?"),
                        job_id     = job.job_id,
                    )
                    try:
                        await status_msg.edit_text(
                            text,
                            parse_mode   = enums.ParseMode.HTML,
                            reply_markup = _cancel_markup(job.job_id),
                        )
                    except Exception:
                        pass

    reader = asyncio.create_task(read_stderr())
    try:
        await asyncio.wait_for(proc.wait(), timeout=config.FFMPEG_WALL_TIMEOUT)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        log.warning("FFmpeg timed out for job %s", job.job_id)
    finally:
        job.ffmpeg_proc = None  # Clear so shutdown logic knows proc is done
        reader.cancel()
        try:
            await reader
        except (asyncio.CancelledError, Exception):
            pass

    if proc.returncode != 0 or stderr_lines:
        log.error(
            "FFmpeg exit code %s for job %s. stderr:\n%s",
            proc.returncode, job.job_id,
            "\n".join(stderr_lines[-20:]),
        )

    # Raise on non-zero exit so callers get a clear failure signal and never
    # accidentally treat partial output (a few frames of garbage) as success.
    if proc.returncode not in (0, None):
        stderr_tail = "\n".join(stderr_lines[-5:])
        raise RuntimeError(
            f"FFmpeg exited with code {proc.returncode}. stderr: {stderr_tail}"
        )


async def _safe_send_message(client, chat_id, text, **kwargs):
    kwargs.setdefault("parse_mode", enums.ParseMode.HTML)
    try:
        return await tg_call(lambda: client.send_message(chat_id, text, **kwargs))
    except Exception as e:
        log.warning("Could not send status message to %s: %s", chat_id, e)
        return None


async def _safe_edit(status, text, parse_mode=enums.ParseMode.HTML, **kwargs):
    if status is None:
        return
    try:
        await status.edit_text(text, parse_mode=parse_mode, **kwargs)
    except Exception:
        pass


async def run_encode_job(client, job, media_ref, src_chat_id: int, src_message_id: int):
    user_id  = job.user_id
    settings = await database.get_user_settings(user_id)
    video    = settings["video"]
    resolutions = list(video.get("resolution") or ["720p"])

    status = None

    # ── Disk space check ────────────────────────────────────────────────────
    try:
        free = shutil.disk_usage(".").free
    except Exception:
        free = config.MIN_FREE_DISK_BYTES

    if free < config.MIN_FREE_DISK_BYTES:
        msg = (
            f"❌ <b>Not enough disk space</b>\n"
            f"Free: {humanbytes(free)}  ·  Required: {humanbytes(config.MIN_FREE_DISK_BYTES)}"
        )
        log.warning("Disk space check failed for job %s", job.job_id)
        await _safe_send_message(client, job.chat_id, msg, parse_mode=enums.ParseMode.HTML)
        return

    download_dir = os.path.abspath(config.DOWNLOAD_DIR)
    os.makedirs(download_dir, exist_ok=True)

    # ── Download ─────────────────────────────────────────────────────────────
    status = await _safe_send_message(
        client,
        job.chat_id,
        "⬇️ <b>Downloading…</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup = _cancel_markup(job.job_id),
    )

    input_path = os.path.join(download_dir, f"{job.job_id}.src")

    try:
        input_path = await download_manager.download(
            client, media_ref.file_id, input_path, message=status,
            ud_type="Downloading",
            job=job,          # enables SIGTERM / cancel-aware abort
        )
    except Exception as e:
        log.exception("Download failed: %s", e)
        await _safe_edit(status, f"❌ <b>Download failed</b>\n<code>{e}</code>")
        return

    if job.cancel_requested:
        _safe_remove(input_path)
        await _safe_edit(status, "🛑 Cancelled.", reply_markup=None)
        return

    # ── Probe ────────────────────────────────────────────────────────────────
    try:
        probe_data = await probe_video(input_path)
    except Exception as e:
        log.exception("Probe failed: %s", e)
        probe_data = {}

    duration = float(probe_data.get("duration") or 0)

    # Integrity check: if ffprobe can't find a video stream or reports zero
    # duration the file is corrupt or was truncated during download.
    if duration <= 0 or not probe_data.get("video_codec"):
        log.error(
            "Integrity check failed for job %s: duration=%.1f video_codec=%s — file corrupt",
            job.job_id, duration, probe_data.get("video_codec"),
        )
        _safe_remove(input_path)
        await _safe_edit(
            status,
            "❌ <b>Download appears corrupt</b> — ffprobe could not read the file.\n"
            "Please re-send the file.",
            reply_markup=None,
        )
        return

    out_paths = []

    # ── Encode loop ──────────────────────────────────────────────────────────
    try:
        for idx, res in enumerate(resolutions):
            if job.cancel_requested:
                break

            job._current_res_idx = idx

            await _safe_edit(
                status,
                (
                    f"🎬 <b>Encoding {res}</b>  ({idx + 1}/{len(resolutions)})\n"
                    f"<code>[──────────]</code> <b>0%</b>\n"
                    f"⏱ 0s / {TimeFormatter(int(duration))}\n"
                    f"🎞 {video.get('codec','libx264')}  ·  "
                    f"CRF {video.get('crf','?')}  ·  {video.get('preset','?')}"
                ),
                parse_mode=enums.ParseMode.HTML,
                reply_markup = _cancel_markup(job.job_id),
            )

            local_settings = copy.deepcopy(settings)
            local_settings["video"]["resolution"] = [res]

            out_ext    = "." + str(video.get("output_format", "mkv"))
            output_path = os.path.join(download_dir, f"{job.job_id}_{res}{out_ext}")

            cmd = generate_ffmpeg_cmd(input_path, output_path, local_settings, probe_data)
            log.info("FFmpeg cmd (%s): %s", res, " ".join(cmd))

            ffmpeg_error = None
            try:
                await _run_ffmpeg(cmd, job, status, duration, local_settings)
            except Exception as _fe:
                ffmpeg_error = _fe

            if job.cancel_requested:
                break

            exists = os.path.isfile(output_path)
            size   = os.path.getsize(output_path) if exists else 0
            log.info("Output check: %s exists=%s size=%d", output_path, exists, size)

            MIN_VALID_SIZE = 512 * 1024  # 512 KB — real videos are always larger
            if ffmpeg_error or not exists or size < MIN_VALID_SIZE:
                reason = str(ffmpeg_error) if ffmpeg_error else f"exists={exists} size={size}"
                log.error("Encoding produced no usable output for %s: %s", res, reason)
                _safe_remove(output_path)  # clean up garbage partial file
                await _safe_edit(
                    status,
                    f"❌ <b>Encoding failed for {res}</b> — {reason}",
                    reply_markup = _cancel_markup(job.job_id),
                )
                continue

            if size > config.MAX_OUTPUT_SIZE:
                log.warning("Output %s exceeds max size", output_path)

            out_paths.append((res, output_path))

    except asyncio.CancelledError:
        # SIGTERM / graceful shutdown killed this coroutine mid-encode.
        job.cancel_requested = True
        raise
    except Exception as e:
        log.exception("Encoding pipeline failed: %s", e)
        await _safe_edit(status, f"❌ <b>Encoding error:</b> <code>{e}</code>")

    if job.cancel_requested:
        _safe_remove(input_path)
        for _, p in out_paths:
            _safe_remove(p)
        # Distinguish a user cancel from a server restart / SIGTERM
        cancel_text = (
            "⚠️ <b>Job interrupted (server restart).</b>\n"
            "The bot was redeployed mid-encode. Please re-send the file."
            if getattr(job, "_sigterm_shutdown", False)
            else "🛑 <b>Cancelled.</b>"
        )
        await _safe_edit(status, cancel_text, parse_mode=enums.ParseMode.HTML, reply_markup=None)
        return

    # ── Upload loop ──────────────────────────────────────────────────────────
    for res, out in out_paths:
        if job.cancel_requested:
            break

        out_size = os.path.getsize(out) if os.path.isfile(out) else 0

        await _safe_edit(
            status,
            (
                f"⬆️ <b>Uploading {res}</b>\n"
                f"<code>[──────────]</code> <b>0%</b>\n"
                f"📦 0 B / {humanbytes(out_size)}\n"
                f"⚡ — · ⏱ ETA: —"
            ),
            parse_mode=enums.ParseMode.HTML,
        )

        # thumbnail
        thumb_path = None
        thumb_dir  = os.path.abspath(config.THUMB_DIR)
        os.makedirs(thumb_dir, exist_ok=True)

        thumb_id = settings.get("thumbnail")
        if thumb_id:
            thumb_path = os.path.join(thumb_dir, f"{job.job_id}_{res}_custom.jpg")
            try:
                await client.download_media(thumb_id, file_name=thumb_path)
                if not os.path.isfile(thumb_path) or os.path.getsize(thumb_path) == 0:
                    _safe_remove(thumb_path)
                    thumb_path = None
            except Exception:
                _safe_remove(thumb_path)
                thumb_path = None
        else:
            thumb_path = os.path.join(thumb_dir, f"{job.job_id}_{res}.jpg")
            try:
                if not await extract_thumbnail(out, thumb_path, duration):
                    _safe_remove(thumb_path)
                    thumb_path = None
            except Exception:
                _safe_remove(thumb_path)
                thumb_path = None

        caption = (
            f"✅ {res}  ·  {video['codec']}  ·  CRF {video['crf']}  ·  "
            f"{settings['audio']['codec']} {settings['audio']['bitrate']}"
        )

        try:
            await upload_manager.upload(
                client,
                job.chat_id,
                out,
                thumb       = thumb_path,
                caption     = caption,
                as_video    = bool(settings.get("output_as_video", True)),
                message     = status,
                ud_type     = f"Uploading {res}",
            )
        except Exception as e:
            log.exception("Upload failed: %s", e)
            await _safe_send_message(
                client, job.chat_id,
                f"❌ <b>Upload failed ({res}):</b> <code>{e}</code>",
            )
        finally:
            # Always clean up the thumb — even if upload failed
            _safe_remove(thumb_path)

    # ── Cleanup — always runs, cancel or not ─────────────────────────────────
    try:
        in_size = os.path.getsize(input_path) if os.path.isfile(input_path) else 0
    except Exception:
        in_size = 0
    out_total = sum(os.path.getsize(p) for _, p in out_paths if os.path.isfile(p))

    _safe_remove(input_path)
    for _, p in out_paths:
        _safe_remove(p)

    if job.cancel_requested:
        await _safe_edit(status, "🛑 <b>Cancelled.</b>", parse_mode=enums.ParseMode.HTML, reply_markup=None)
        return

    await _safe_edit(status, "✅ <b>Encode complete!</b>", reply_markup=None)

    try:
        await database.inc_stats("encodes", 1)
    except Exception:
        pass

    try:
        ratio = ""
        if out_paths and in_size:
            ratio = f" ({100 * out_total / in_size:.1f}% of source)"
        await tg_call(lambda: client.send_message(
            config.LOG_CHANNEL,
            f"✅ Job `{job.job_id}` complete\n"
            f"File: {job.file_name}\n"
            f"Res: {', '.join(r for r, _ in out_paths) or 'none'}{ratio}",
        ))
    except Exception:
        pass


def _safe_remove(path):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass
