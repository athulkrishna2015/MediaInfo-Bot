import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", ))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")

ADMIN_ID = int(os.environ.get("ADMIN_ID", ))

_raw = os.environ.get("ALLOWED_CHATS", "")
ALLOWED_CHATS = [int(x.strip()) for x in _raw.split(",") if x.strip()]

LOG_FORMAT = os.environ.get(
    "LOG_FORMAT",
    "[%(asctime)s][%(name)s][%(module)s][%(lineno)d][%(levelname)s] -> %(message)s",
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

GC_THRESHOLD = (
    int(os.environ.get("GC_THRESHOLD_0", 500)),
    int(os.environ.get("GC_THRESHOLD_1", 5)),
    int(os.environ.get("GC_THRESHOLD_2", 5)),
)

CAPTION_TEMPLATE = os.environ.get(
    "CAPTION_TEMPLATE",
    "<b>{title}</b>\n\n"
    "🎬 <b>{video_line}</b> | ⏳ <b>{duration}</b>\n"
    "🔊 <b>{audio}</b>\n"
    "💬 <b>{subtitle}</b>\n\n"
)