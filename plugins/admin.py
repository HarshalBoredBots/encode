import os

from pyrogram import Client, filters

from bot import config
from bot.decorator import is_admin
from bot.logger import get_logger, LOG_FILE
from bot.utils.shell import run_shell
from bot.utils.restart import restart_bot
import database

log = get_logger(__name__)

@Client.on_message(filters.command("broadcast") & filters.private)
@is_admin
async def broadcast_cmd(client, message):
    if len(message.command) < 2 and not message.reply_to_message:
        await message.reply_text("Usage: /broadcast <text> or reply to a message.")
        return

    if message.reply_to_message:
        payload = message.reply_to_message
    else:
        payload = " ".join(message.command[1:])

    users = await database.full_userbase()
    sent = failed = 0
    status = await message.reply_text(f"Broadcasting to {len(users)} users...")
    for uid in users:
        try:
            if isinstance(payload, str):
                await client.send_message(uid, payload)
            else:
                await payload.copy(uid)
            sent += 1
        except Exception:
            failed += 1
    try:
        await status.edit_text(f"✅ Done. Sent: {sent}, Failed: {failed}.")
    except Exception:
        pass

@Client.on_message(filters.command("ban") & filters.private)
@is_admin
async def ban_cmd(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /ban <user_id>")
        return
    try:
        uid = int(message.command[1])
    except ValueError:
        await message.reply_text("Invalid user id.")
        return
    await database.ban_user(uid)
    await message.reply_text(f"🚫 Banned {uid}.")

@Client.on_message(filters.command("unban") & filters.private)
@is_admin
async def unban_cmd(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /unban <user_id>")
        return
    try:
        uid = int(message.command[1])
    except ValueError:
        await message.reply_text("Invalid user id.")
        return
    await database.unban_user(uid)
    await message.reply_text(f"✅ Unbanned {uid}.")

@Client.on_message(filters.command("stats") & filters.private)
@is_admin
async def stats_cmd(client, message):
    stats = await database.get_stats()
    users = await database.full_userbase()
    await message.reply_text(
        f"📈 **Statistics**\n"
        f"• Users: {len(users)}\n"
        f"• Encodes: {stats.get('encodes', 0)}"
    )

@Client.on_message(filters.command("log") & filters.private)
@is_admin
async def log_cmd(client, message):
    if os.path.isfile(LOG_FILE):
        await message.reply_document(LOG_FILE, caption="Latest log file.")
    else:
        await message.reply_text("No log file found.")

@Client.on_message(filters.command("shell") & filters.private)
async def shell_cmd(client, message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.reply_text("Dukhi Atma! 😔")
        return
    if len(message.command) < 2:
        await message.reply_text("Usage: /shell <command>")
        return
    cmd = " ".join(message.command[1:])
    out = await run_shell(cmd)
    if len(out) > 3800:
        out = out[:3800] + "\n...truncated"
    await message.reply_text(f"```\n{out}\n```")

@Client.on_message(filters.command("restart") & filters.private)
async def restart_cmd(client, message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.reply_text("Dukhi Atma! 😔")
        return
    msg = await message.reply_text("♻️ Restarting bot, please wait...")
    try:
        await msg.edit_text("♻️ Restarting bot, please wait...\n`Saving state...`")
    except Exception:
        pass
    await restart_bot()


@Client.on_message(filters.command("forcesettings") & filters.private)
@is_admin
async def forcesettings_cmd(client, message):
    """Owner-only: overwrite ALL users' settings with the owner's current settings."""
    args = message.command[1:]

    if not args or args[0].lower() != "confirm":
        await message.reply_text(
            "⚠️ **Force-Settings Warning**\n\n"
            "This command will **overwrite every user's settings** in the database "
            "with your own current settings — including codec, CRF, preset, resolution, "
            "watermark, audio, rename pattern, output format, subtitle mode, and more.\n\n"
            "**This cannot be undone.**\n\n"
            "To proceed, type:\n"
            "`/forcesettings confirm`"
        )
        return

    owner_id = message.from_user.id
    status_msg = await message.reply_text("⏳ Fetching your settings and applying to all users...")

    owner_settings = await database.get_user_settings(owner_id)
    updated_count = await database.overwrite_all_user_settings(owner_settings)

    # Build a human-readable summary of the key settings being applied.
    wm = owner_settings.get("watermark", {})
    wm_enabled = "✅ Enabled" if wm.get("enabled") else "❌ Disabled"

    summary = (
        f"✅ **Force-Settings Applied**\n\n"
        f"**Users updated:** `{updated_count}`\n\n"
        f"**Settings pushed:**\n"
        f"• Video Codec: `{owner_settings.get('codec', 'N/A')}`\n"
        f"• CRF: `{owner_settings.get('crf', 'N/A')}`\n"
        f"• Preset: `{owner_settings.get('preset', 'N/A')}`\n"
        f"• Resolution: `{owner_settings.get('resolution', 'N/A')}`\n"
        f"• Watermark: {wm_enabled}\n"
        f"• Audio Codec: `{owner_settings.get('audio_codec', 'N/A')}`\n"
        f"• Audio Bitrate: `{owner_settings.get('audio_bitrate', 'N/A')}`\n"
        f"• Output Format: `{owner_settings.get('output_format', 'N/A')}`\n"
        f"• Rename Pattern: `{owner_settings.get('rename_pattern', 'N/A')}`\n"
        f"• Subtitle Mode: `{owner_settings.get('subtitle_mode', 'N/A')}`"
    )

    try:
        await status_msg.edit_text(summary)
    except Exception:
        await message.reply_text(summary)

@Client.on_message(filters.command("allqueue") & filters.private)
async def allqueue_cmd(client, message):
    """Owner-only: show every user's active jobs in a concise table."""
    if message.from_user.id not in config.ADMIN_IDS:
        await message.reply_text("Dukhi Atma! 😔")
        return

    from bot.func.queue_manager import queue_manager
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

    all_jobs = [j for j in queue_manager.jobs.values()
                if j.status in ("pending", "running")]

    if not all_jobs:
        await message.reply_text("📭 Queue is empty.")
        return

    running = [j for j in all_jobs if j.status == "running"]
    pending = [j for j in all_jobs if j.status == "pending"]

    lines = [f"📋 **Global Queue** — {len(all_jobs)} job(s)\n"]

    if running:
        lines.append("▶️ **Running:**")
        for j in running:
            lines.append(f"  • `{j.file_name or 'file'}` (user `{j.user_id}`)")

    if pending:
        lines.append("\n⏳ **Pending:**")
        for i, j in enumerate(pending, 1):
            lines.append(f"  {i}. `{j.file_name or 'file'}` (user `{j.user_id}`)")

    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🛑 Cancel ALL", callback_data="owner_cancel_all")
    ]])
    await message.reply_text("\n".join(lines), reply_markup=markup)
