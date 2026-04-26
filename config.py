import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

CONFIG_ERRORS: list[str] = []


def _read_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _read_int(name: str, *, required: bool = False) -> Optional[int]:
    raw = _read_env(name)
    if not raw:
        if required:
            CONFIG_ERRORS.append(f"{name} is required.")
        return None

    try:
        return int(raw)
    except ValueError:
        CONFIG_ERRORS.append(f"{name} must be an integer.")
        return None


def _read_int_with_default(name: str, default: int) -> int:
    raw = _read_env(name)
    if not raw:
        return default

    try:
        return int(raw)
    except ValueError:
        CONFIG_ERRORS.append(f"{name} must be an integer.")
        return default


def _read_float_with_default(name: str, default: float) -> float:
    raw = _read_env(name)
    if not raw:
        return default

    try:
        return float(raw)
    except ValueError:
        CONFIG_ERRORS.append(f"{name} must be a number.")
        return default


def _read_int_list(name: str) -> list[int]:
    raw = _read_env(name)
    if not raw:
        return []

    values: list[int] = []
    for item in raw.split(","):
        candidate = item.strip()
        if not candidate:
            continue
        try:
            values.append(int(candidate))
        except ValueError:
            CONFIG_ERRORS.append(f"{name} contains an invalid integer: {candidate}")
    return values


API_ID = _read_int("API_ID", required=True)
API_HASH = _read_env("API_HASH")
BOT_TOKEN = _read_env("BOT_TOKEN")
STRING_SESSION = _read_env("STRING_SESSION")

# Collect all user sessions: STRING_SESSION, STRING_SESSION_2, STRING_SESSION_3, ...
_sessions: list[str] = []
if STRING_SESSION:
    _sessions.append(STRING_SESSION)
for _i in range(2, 20):  # support up to 19 accounts
    _s = _read_env(f"STRING_SESSION_{_i}")
    if _s:
        _sessions.append(_s)
    else:
        break  # stop at first gap
STRING_SESSIONS: list[str] = _sessions
# Support both ADMIN_IDS (comma-separated) and legacy ADMIN_ID (single)
_admin_ids_list = _read_int_list("ADMIN_IDS")
_legacy_admin_id = _read_int("ADMIN_ID")
if not _admin_ids_list and _legacy_admin_id is not None:
    _admin_ids_list = [_legacy_admin_id]
if not _admin_ids_list:
    CONFIG_ERRORS.append("ADMIN_IDS (or ADMIN_ID) is required.")
ADMIN_IDS: list[int] = _admin_ids_list
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else None
ALLOWED_CHATS = _read_int_list("ALLOWED_CHATS")

if not API_HASH:
    CONFIG_ERRORS.append("API_HASH is required.")

if not BOT_TOKEN:
    CONFIG_ERRORS.append("BOT_TOKEN is required.")

LOG_FORMAT = _read_env(
    "LOG_FORMAT",
    "[%(asctime)s][%(name)s][%(module)s][%(lineno)d][%(levelname)s] -> %(message)s",
)
LOG_LEVEL = _read_env("LOG_LEVEL", "INFO") or "INFO"

GC_THRESHOLD = (
    _read_int_with_default("GC_THRESHOLD_0", 500),
    _read_int_with_default("GC_THRESHOLD_1", 5),
    _read_int_with_default("GC_THRESHOLD_2", 5),
)

# Minimum seconds between caption edits (lower = faster, higher = safer vs FloodWait)
EDIT_DELAY: float = _read_float_with_default("EDIT_DELAY", 1.5)

# Number of messages processed concurrently during /scan
# Keep low (1-2) when using a user account for streaming to avoid DC auth FloodWait
SCAN_WORKERS: int = _read_int_with_default("SCAN_WORKERS", 2)

DEFAULT_CAPTION_TEMPLATE = (
    "<b>{title}</b>\n\n"
    "🎬 <b>{video_line}</b> | ⏳ <b>{duration}</b>\n"
    "🔊 <b>{audio}</b>\n"
    "💬 <b>{subtitle}</b>"
)

CAPTION_TEMPLATE = _read_env("CAPTION_TEMPLATE", DEFAULT_CAPTION_TEMPLATE) or DEFAULT_CAPTION_TEMPLATE


def validate_config() -> None:
    if not CONFIG_ERRORS:
        return

    details = "\n".join(f"- {item}" for item in CONFIG_ERRORS)
    raise RuntimeError(f"Invalid configuration:\n{details}")
