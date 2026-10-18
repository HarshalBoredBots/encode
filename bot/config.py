import os
import shutil

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
APP_ID = 20140875
API_HASH = "a06fa97d5a853ec2da79015b11335a17"
LOG_CHANNEL = -1004384759946
OWNER_ID = 1828405916
PORT = int(os.environ.get("PORT", "8080"))
BOT_NAME = "Encoder Bot"
BOT_VERSION = "1.0.0"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "EncoderBot")

MAX_CONCURRENT_JOBS = 1
MAX_CONCURRENT_DOWNLOADS = 1
MAX_CONCURRENT_UPLOADS = 1
MAX_CONCURRENT_JOBS_PER_USER = 1      # only 1 encodes at a time
MAX_QUEUED_JOBS_PER_USER = 25         # up to 25 files can wait in queue per user
MAX_QUEUE_LENGTH = 50
TG_BOT_WORKERS = 4
MAX_CONCURRENT_TRANSMISSIONS = 4

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
MAX_OUTPUT_SIZE = 4 * 1024 * 1024 * 1024
MIN_FREE_DISK_BYTES = 300 * 1024 * 1024  # 300 MB (safe for Heroku Standard 2X)

DOWNLOAD_DIR = "downloads"
THUMB_DIR = "thumbs"
WATERMARK_DIR = "watermarks"
FONT_DIR = "fonts"
FONT_PATH = "bot/fonts/Kufam-SemiBold.ttf"

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "/app/ffmpeg-static/ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "/app/ffmpeg-static/ffprobe")

# Cap threads at 2 to prevent OOM crashes (especially with SVT-AV1)
_cpu_count = min(os.cpu_count() or 2, 2)
FFMPEG_THREADS = int(os.environ.get("FFMPEG_THREADS", str(_cpu_count)))

FFMPEG_WALL_TIMEOUT = 28800

# MAX_RESOLUTION: hard cap on the output resolution this app instance will encode.
# Set this env var in each Heroku app's config vars to prevent accidental
# high-res encodes on the wrong worker.
# Valid values: "1080p", "720p", "480p", "360p"
# Default: "480p" — safe for a 1 GB Heroku dyno.
# encode1 → MAX_RESOLUTION=480p
# encode2 → MAX_RESOLUTION=720p
# encode3 → MAX_RESOLUTION=1080p
_RESOLUTION_RANK = {"1080p": 4, "720p": 3, "480p": 2, "360p": 1}
MAX_RESOLUTION = os.environ.get("MAX_RESOLUTION", "480p")
if MAX_RESOLUTION not in _RESOLUTION_RANK:
    MAX_RESOLUTION = "480p"

UI_UPDATE_INTERVAL = 4.0
PROGRESS_CALLBACK_INTERVAL = 4.0

ADMIN_IDS = {OWNER_ID}

def validate_config():
    errors = []
    if not TG_BOT_TOKEN or ":" not in TG_BOT_TOKEN:
        errors.append("TG_BOT_TOKEN is missing or invalid.")
    if not DATABASE_URL:
        errors.append("DATABASE_URL is missing.")
    if not shutil.which(FFMPEG_BIN) and not os.path.isfile(FFMPEG_BIN):
        errors.append(f"ffmpeg binary not found: {FFMPEG_BIN}")
    if not shutil.which(FFPROBE_BIN) and not os.path.isfile(FFPROBE_BIN):
        errors.append(f"ffprobe binary not found: {FFPROBE_BIN}")

    for d in (DOWNLOAD_DIR, THUMB_DIR, WATERMARK_DIR, FONT_DIR, "logs", "bot/fonts"):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            errors.append(f"cannot create directory {d}: {e}")

    if errors:
        raise RuntimeError("Configuration errors:\n - " + "\n - ".join(errors))
    return True
