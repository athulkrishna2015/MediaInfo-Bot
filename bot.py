import asyncio

# Fix for Python 3.12+ / 3.14 RuntimeError: No current event loop
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import gc
import html
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from functools import lru_cache
from typing import Any, Optional

import psutil
from aiofiles import open as aiopen
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, PeerIdInvalid

from config import (
    ADMIN_ID,
    ADMIN_IDS,
    ALLOWED_CHATS,
    API_HASH,
    API_ID,
    BOT_TOKEN,
    CAPTION_TEMPLATE,
    DEFAULT_CAPTION_TEMPLATE,
    EDIT_DELAY,
    GC_THRESHOLD,
    LOG_FORMAT,
    LOG_LEVEL,
    SCAN_WORKERS,
    STRING_SESSION,
    validate_config,
)

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, force=True)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

app = Client(
    "MediaInfo-Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=6,
    sleep_threshold=300,
)

user_app = (
    Client(
        "MediaInfo-User",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=STRING_SESSION,
        workers=2,
        sleep_threshold=300,
    )
    if STRING_SESSION
    else None
)

stream_semaphore = asyncio.Semaphore(4)
active_users: set[int] = set()

_last_edit: dict[int, float] = {}
channel_queues: dict[int, list[tuple[Any, str]]] = defaultdict(list)
channel_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
media_group_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
last_edit_time: dict[int, float] = {}
processed_media_groups: dict[str, float] = {}
EDIT_DELAY: float = EDIT_DELAY  # from config (default 1.5s, configurable via .env)
CAPTION_LIMIT = 1024
MEDIA_GROUP_CACHE_TTL = 300.0

_flood_wait_until: float = 0.0
_flood_wait_lock = asyncio.Lock()

scheduler = AsyncIOScheduler()
TMP_DIR = "tmp"

_NEVER_FILTER = filters.create(lambda _, __, ___: False)
ALLOWED_CHAT_FILTER = filters.chat(ALLOWED_CHATS) if ALLOWED_CHATS else _NEVER_FILTER
ADMIN_FILTER = filters.user(ADMIN_IDS) if ADMIN_IDS else _NEVER_FILTER

VIDEO_STREAM_STEPS = (16 * 1024, 1 * 1024 * 1024, 3 * 1024 * 1024, 8 * 1024 * 1024)
PHOTO_STREAM_STEPS = (128 * 1024,)
MEDIA_INFO_LINE_RE = re.compile(r"(?m)^(?:<b>)?(?:[📸🎬📄]|\d+\.\s+.*[📸🎬📄]).*$")

def _status_print(text: str) -> None:
    """Overwrite the current terminal line in-place (no scrolling spam)."""
    if _loop_time() < _flood_wait_until:
        return
    sys.stdout.write(f"\r\033[K{text}")
    sys.stdout.flush()


async def _handle_flood_wait(exc_value: int):
    """Synchronized handling of FloodWait to prevent log spam."""
    global _flood_wait_until
    async with _flood_wait_lock:
        now = _loop_time()
        if now < _flood_wait_until:
            return
        _flood_wait_until = now + exc_value + 2
        logger.warning(
            "⚡ FloodWait hit: Pausing for %ss (until %s)",
            exc_value,
            time.strftime("%H:%M:%S", time.localtime(_flood_wait_until)),
        )


def _loop_time() -> float:
    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:
        return asyncio.get_event_loop().time()


def _prune_processed_media_groups() -> None:
    now = _loop_time()
    stale_groups = [group_id for group_id, ts in processed_media_groups.items() if now - ts > MEDIA_GROUP_CACHE_TTL]
    for group_id in stale_groups:
        processed_media_groups.pop(group_id, None)


def _human_size(num: float, suffix: str = "B") -> str:
    value = float(num or 0)
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(value) < 1024.0:
            return f"{value:3.1f} {unit}{suffix}"
        value /= 1024.0
    return f"{value:.1f} Yi{suffix}"


def _human_time(seconds: float) -> str:
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


async def _remove_path(path: Optional[str]) -> None:
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as exc:
        logger.warning("Error removing %s: %s", path, exc)


def _safe_filename(name: Optional[str], fallback: str) -> str:
    base = os.path.basename((name or "").strip()) or fallback
    return re.sub(r"[^A-Za-z0-9._-]+", "_", base)


def _new_tmp_path(prefix: str, message_id: int, suffix: str = ".bin") -> str:
    return os.path.join(TMP_DIR, f"{prefix}_{message_id}_{uuid.uuid4().hex[:8]}{suffix}")


def _download_path(prefix: str, message_id: int, filename: Optional[str]) -> str:
    safe_name = _safe_filename(filename, f"{prefix}_{message_id}.bin")
    return os.path.join(TMP_DIR, f"{uuid.uuid4().hex[:8]}_{safe_name}")


def _init_tmp() -> None:
    os.makedirs(TMP_DIR, exist_ok=True)
    for entry in os.listdir(TMP_DIR):
        path = os.path.join(TMP_DIR, entry)
        if not os.path.isfile(path):
            continue
        try:
            os.remove(path)
        except OSError:
            logger.debug("Skipping temp cleanup for %s", path)
    logger.info("Temporary directory initialized and cleared.")


_init_tmp()


_LANGUAGE_MAP: dict[str, str] = {
    "en": "English",
    "eng": "English",
    "hi": "Hindi",
    "hin": "Hindi",
    "ta": "Tamil",
    "tam": "Tamil",
    "te": "Telugu",
    "tel": "Telugu",
    "ml": "Malayalam",
    "mal": "Malayalam",
    "kn": "Kannada",
    "kan": "Kannada",
    "bn": "Bengali",
    "ben": "Bengali",
    "mr": "Marathi",
    "mar": "Marathi",
    "gu": "Gujarati",
    "guj": "Gujarati",
    "pa": "Punjabi",
    "pun": "Punjabi",
    "bho": "Bhojpuri",
    "zh": "Chinese",
    "chi": "Chinese",
    "cmn": "Chinese",
    "ko": "Korean",
    "kor": "Korean",
    "pt": "Portuguese",
    "por": "Portuguese",
    "th": "Thai",
    "tha": "Thai",
    "tl": "Tagalog",
    "tgl": "Tagalog",
    "fil": "Tagalog",
    "ja": "Japanese",
    "jpn": "Japanese",
    "es": "Spanish",
    "spa": "Spanish",
    "sv": "Swedish",
    "swe": "Swedish",
    "fr": "French",
    "fra": "French",
    "fre": "French",
    "de": "German",
    "deu": "German",
    "ger": "German",
    "it": "Italian",
    "ita": "Italian",
    "ru": "Russian",
    "rus": "Russian",
    "ar": "Arabic",
    "ara": "Arabic",
    "tr": "Turkish",
    "tur": "Turkish",
    "nl": "Dutch",
    "nld": "Dutch",
    "dut": "Dutch",
    "pl": "Polish",
    "pol": "Polish",
    "vi": "Vietnamese",
    "vie": "Vietnamese",
    "id": "Indonesian",
    "ind": "Indonesian",
    "ms": "Malay",
    "msa": "Malay",
    "may": "Malay",
    "fa": "Persian",
    "fas": "Persian",
    "per": "Persian",
    "ur": "Urdu",
    "urd": "Urdu",
    "he": "Hebrew",
    "heb": "Hebrew",
    "el": "Greek",
    "ell": "Greek",
    "gre": "Greek",
    "hu": "Hungarian",
    "hun": "Hungarian",
    "cs": "Czech",
    "ces": "Czech",
    "cze": "Czech",
    "ro": "Romanian",
    "ron": "Romanian",
    "rum": "Romanian",
    "da": "Danish",
    "dan": "Danish",
    "fi": "Finnish",
    "fin": "Finnish",
    "no": "Norwegian",
    "nor": "Norwegian",
    "uk": "Ukrainian",
    "ukr": "Ukrainian",
    "ca": "Catalan",
    "cat": "Catalan",
    "hr": "Croatian",
    "hrv": "Croatian",
    "sk": "Slovak",
    "slk": "Slovak",
    "slo": "Slovak",
    "sr": "Serbian",
    "srp": "Serbian",
    "bg": "Bulgarian",
    "bul": "Bulgarian",
    "und": "Unknown",
    "unknown": "Unknown",
}


@lru_cache(maxsize=256)
def get_full_language_name(code: str) -> str:
    if not code:
        return "Unknown"
    cleaned = code.split("(")[0].strip()
    return _LANGUAGE_MAP.get(cleaned.lower(), "Unknown")


@lru_cache(maxsize=64)
def get_standard_resolution(width: int, height: int) -> Optional[str]:
    min_dim = min(width, height) if width and height else max(width, height)
    if not min_dim:
        return None
    if min_dim <= 240:
        return "240p"
    if min_dim <= 360:
        return "360p"
    if min_dim <= 480:
        return "480p"
    if min_dim <= 720:
        return "720p"
    if min_dim <= 1080:
        return "1080p"
    if min_dim <= 1440:
        return "1440p"
    if min_dim <= 2160:
        return "2160p"
    return "2160p+"


@lru_cache(maxsize=128)
def get_video_format(
    codec: str,
    transfer: str = "",
    hdr: str = "",
    bit_depth: str = "",
) -> Optional[str]:
    if not codec:
        return None

    codec_name = codec.lower()
    parts: list[str] = []

    if any(token in codec_name for token in ("hevc", "h.265", "h265")):
        parts.append("HEVC")
    elif "av1" in codec_name:
        parts.append("AV1")
    elif any(token in codec_name for token in ("avc", "avc1", "h.264", "h264")):
        parts.append("x264")
    elif "vp9" in codec_name:
        parts.append("VP9")
    elif any(token in codec_name for token in ("mpeg4", "xvid")):
        parts.append("MPEG4")
    else:
        return None

    try:
        if bit_depth and int(bit_depth) > 8:
            parts.append(f"{bit_depth}bit")
    except (TypeError, ValueError):
        pass

    transfer_name = transfer.lower()
    hdr_name = hdr.lower()
    if any(token in transfer_name for token in ("pq", "hlg", "smpte", "2084", "st 2084")) or "hdr" in hdr_name or "dolby" in hdr_name:
        parts.append("HDR")

    return " ".join(parts)


def _is_video_track(track: dict[str, Any]) -> bool:
    track_type = (track.get("@type", "") or "").lower()
    format_name = (track.get("Format", "") or "").lower()
    codec_id = (track.get("CodecID", "") or "").lower()
    format_profile = (track.get("Format_Profile", "") or "").lower()
    title = (track.get("Title", "") or "").lower()
    menu = str(track.get("MenuID", "") or "").lower()

    return any(
        [
            track_type == "video",
            any(token in format_name for token in ("avc", "hevc", "h.264", "h264", "h.265", "h265", "av1", "vp9", "mpeg-4", "mpeg4", "xvid")),
            any(token in codec_id for token in ("avc", "h264", "hevc", "h265", "av1", "vp9", "mpeg4", "xvid", "27")),
            "video" in menu,
            "video" in title,
            any(token in format_profile for token in ("main", "high", "baseline")),
        ]
    )


def _is_subtitle_track(track: dict[str, Any]) -> bool:
    track_type = (track.get("@type", "") or "").lower()
    format_name = (track.get("Format", "") or "").lower()
    codec_id = (track.get("CodecID", "") or "").lower()
    encoding = (track.get("Encoding", "") or "").lower()
    format_info = (track.get("Format_Info", "") or "").lower()
    title = (track.get("Title", "") or "").lower()

    return any(
        [
            track_type == "text",
            any(token in format_name for token in ("pgs", "subrip", "ass", "ssa", "srt", "dvb_subtitle", "dvd_subtitle")),
            any(token in codec_id for token in ("s_text", "subp", "pgs", "subtitle", "dvb", "dvd")),
            any(token in encoding for token in ("utf-8", "utf8", "unicode", "text")),
            any(token in format_info for token in ("subtitle", "caption", "text")),
            "subtitle" in title,
        ]
    )


def _parse_int(value: Any) -> int:
    try:
        return int(re.findall(r"\d+", str(value))[0])
    except Exception:
        return 0


def _parse_duration(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0
        raw = str(value).strip()
        if raw.replace(".", "", 1).lstrip("-").isdigit():
            numeric = float(raw)
            if numeric > 86_400_000:
                return numeric / 1_000_000
            if numeric > 86_400:
                return numeric / 1_000
            return numeric
        if ":" in raw:
            parts = [float(part) for part in raw.split(":")]
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
    except Exception:
        pass
    return 0


def _fmt_duration(seconds: float) -> str:
    total = int(seconds or 0)
    return f"{total // 3600:02}:{(total % 3600) // 60:02}:{total % 60:02}"


def _empty_info() -> dict[str, Any]:
    return {"format": {}, "video": [], "audio": [], "subtitle": []}


def _compact_track(track: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in track.items() if value not in (None, "", [], {})}


def _track_key(track: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((key, str(value)) for key, value in _compact_track(track).items()))


def _merge_tracks(primary: list[dict[str, Any]], fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for track in [*(primary or []), *(fallback or [])]:
        cleaned = _compact_track(track)
        if not cleaned:
            continue
        key = _track_key(cleaned)
        if key in seen:
            continue
        seen.add(key)
        merged.append(cleaned)
    return merged


def _merge_info(primary: Optional[dict[str, Any]], fallback: Optional[dict[str, Any]]) -> dict[str, Any]:
    primary = primary or _empty_info()
    fallback = fallback or _empty_info()

    merged = _empty_info()
    duration = primary.get("format", {}).get("duration") or fallback.get("format", {}).get("duration")
    size = primary.get("format", {}).get("size") or fallback.get("format", {}).get("size")
    if duration:
        merged["format"]["duration"] = duration
    if size:
        merged["format"]["size"] = size

    for kind in ("video", "audio", "subtitle"):
        merged[kind] = _merge_tracks(primary.get(kind, []), fallback.get(kind, []))

    return merged


def _is_photo_message(message: Any) -> bool:
    if message.photo:
        return True
    if getattr(message, "sticker", None):
        return True
    mime_type = (getattr(message.document, "mime_type", "") or "").lower()
    return mime_type.startswith("image/")


def _is_video_message(message: Any) -> bool:
    if getattr(message, "video", None) or getattr(message, "animation", None):
        return True
    mime_type = (getattr(message.document, "mime_type", "") or "").lower()
    return mime_type.startswith("video/")


def _base_info_from_message(message: Any, media: Any) -> dict[str, Any]:
    info = _empty_info()
    file_size = getattr(media, "file_size", 0) or 0
    duration = getattr(media, "duration", 0) or 0

    if file_size:
        info["format"]["size"] = file_size
    if duration:
        info["format"]["duration"] = _parse_duration(duration)

    width = _parse_int(getattr(media, "width", 0))
    height = _parse_int(getattr(media, "height", 0))
    if width or height:
        info["video"].append(
            _compact_track(
                {
                    "width": width,
                    "height": height,
                    "codec": getattr(media, "mime_type", ""),
                }
            )
        )

    return info


def _normalize_ffprobe_stream(stream: dict[str, Any]) -> dict[str, Any]:
    tags = stream.get("tags") or {}
    payload = {
        "codec": stream.get("codec_name") or stream.get("codec_long_name") or stream.get("codec_tag_string"),
        "language": tags.get("language") or stream.get("language"),
        "title": tags.get("title") or stream.get("title"),
    }

    if stream.get("codec_type") == "video":
        payload.update(
            {
                "width": _parse_int(stream.get("width")),
                "height": _parse_int(stream.get("height")),
                "bit_depth": _parse_int(stream.get("bits_per_raw_sample") or stream.get("bits_per_sample"))
                or stream.get("bits_per_raw_sample")
                or stream.get("bits_per_sample"),
                "transfer": stream.get("color_transfer") or stream.get("color_space") or stream.get("color_primaries"),
                "hdr": " ".join(
                    json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
                    for item in (stream.get("side_data_list") or [])
                ),
            }
        )

    return _compact_track(payload)


async def _probe_with_ffprobe(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return _empty_info()

    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        if proc.returncode != 0 or not stdout:
            return _empty_info()

        data = json.loads(stdout.decode() or "{}")
        streams = data.get("streams", [])
        format_info = data.get("format", {}) or {}

        return {
            "format": _compact_track(
                {
                    "duration": _parse_duration(format_info.get("duration")),
                    "size": _parse_int(format_info.get("size")),
                }
            ),
            "video": [_normalize_ffprobe_stream(stream) for stream in streams if stream.get("codec_type") == "video"],
            "audio": [_normalize_ffprobe_stream(stream) for stream in streams if stream.get("codec_type") == "audio"],
            "subtitle": [_normalize_ffprobe_stream(stream) for stream in streams if stream.get("codec_type") == "subtitle"],
        }
    except asyncio.TimeoutError:
        logger.warning("ffprobe timed out for %s", path)
    except Exception as exc:
        logger.warning("ffprobe error for %s: %s", path, exc)
    return _empty_info()


def _normalize_mediainfo_track(track: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "codec": track.get("Format_Commercial_IfAny") or track.get("Format") or track.get("CodecID"),
        "language": track.get("Language"),
        "title": track.get("Title"),
    }

    if _is_video_track(track):
        payload.update(
            {
                "width": _parse_int(track.get("Width")),
                "height": _parse_int(track.get("Height")),
                "bit_depth": _parse_int(track.get("BitDepth")) or track.get("BitDepth"),
                "transfer": track.get("transfer_characteristics") or track.get("colour_primaries") or track.get("HDR_Format"),
                "hdr": track.get("HDR_Format_String") or track.get("HDR_Format") or track.get("HDR_Format_Compatibility"),
            }
        )

    return _compact_track(payload)


async def _probe_with_mediainfo(path: str) -> dict[str, Any]:
    if not os.path.exists(path) or not shutil.which("mediainfo"):
        return _empty_info()

    cmd = ["mediainfo", "--ParseSpeed=0", "--Language=raw", "--Output=JSON", path]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        if proc.returncode != 0 or not stdout:
            return _empty_info()

        data = json.loads(stdout.decode() or "{}")
        tracks = data.get("media", {}).get("track", []) or []
        info = _empty_info()

        for track in tracks:
            track_type = (track.get("@type", "") or "").lower()
            if track_type == "general":
                info["format"] = _compact_track(
                    {
                        "duration": _parse_duration(track.get("Duration") or track.get("Duration/String3")),
                        "size": _parse_int(track.get("FileSize")),
                    }
                )
            elif track_type == "audio":
                info["audio"].append(_normalize_mediainfo_track(track))
            elif track_type == "text" or _is_subtitle_track(track):
                info["subtitle"].append(_normalize_mediainfo_track(track))
            elif _is_video_track(track):
                info["video"].append(_normalize_mediainfo_track(track))

        return info
    except asyncio.TimeoutError:
        logger.warning("mediainfo timed out for %s", path)
    except Exception as exc:
        logger.warning("mediainfo error for %s: %s", path, exc)
    return _empty_info()


def _needs_mediainfo_fallback(info: dict[str, Any]) -> bool:
    if not info.get("video"):
        return True
    first_video = info["video"][0]
    return not any((first_video.get("width"), first_video.get("height"), first_video.get("codec")))


async def _probe(path: str) -> dict[str, Any]:
    ffprobe_info = await _probe_with_ffprobe(path)
    if not _needs_mediainfo_fallback(ffprobe_info):
        return ffprobe_info
    mediainfo_info = await _probe_with_mediainfo(path)
    return _merge_info(ffprobe_info, mediainfo_info)


def _build_video_line(info: dict[str, Any], *, is_photo: bool = False) -> str:
    tracks = info.get("video", [])
    if not tracks:
        return "Media Info Unavailable"

    track = tracks[0]
    height = _parse_int(track.get("height"))
    width = _parse_int(track.get("width"))
    parts: list[str] = []

    if is_photo and width and height:
        parts.append(f"{width}x{height}")
    else:
        resolution = get_standard_resolution(width, height)
        if resolution:
            parts.append(resolution)
        elif width and height:
            parts.append(f"{width}x{height}")

    if not is_photo:
        video_format = get_video_format(
            str(track.get("codec", "")),
            str(track.get("transfer", "")),
            str(track.get("hdr", "")),
            str(track.get("bit_depth", "")),
        )
        if video_format:
            parts.append(video_format)
        elif track.get("codec"):
            parts.append(str(track["codec"]).upper())

    return " ".join(part for part in parts if part).strip() or "Media Info Unavailable"


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _extract_text_and_files(message: Any, media: Any, *, is_photo: bool, is_video: bool, group: Optional[list[Any]] = None) -> tuple[str, str]:
    caption_text = (message.caption or "").strip()
    
    file_names = []
    if group:
        for i, item in enumerate(group, start=1):
            item_media = item.video or item.document or item.photo
            if item_media:
                name = (getattr(item_media, "file_name", None) or "").strip()
                is_item_photo = bool(item.photo or (item.document and getattr(item.document, "mime_type", "").startswith("image/")))
                is_item_video = bool(item.video or (item.document and getattr(item.document, "mime_type", "").startswith("video/")))
                
                if not name:
                    if is_item_photo:
                        name = "Photo"
                    elif is_item_video:
                        name = "Video"
                    else:
                        name = "File"
                
                info_parts = []
                size = getattr(item_media, "file_size", 0)
                if size:
                    info_parts.append(_human_size(size))
                    
                width = _parse_int(getattr(item_media, "width", 0))
                height = _parse_int(getattr(item_media, "height", 0))
                if width or height:
                    if is_item_photo:
                        info_parts.append(f"📸 {width}x{height}")
                    else:
                        res = get_standard_resolution(width, height) or f"{width}x{height}"
                        info_parts.append(f"🎬 {res}")
                
                duration = _parse_duration(getattr(item_media, "duration", 0))
                if duration and not is_item_photo:
                    info_parts.append(f"⏳ {_fmt_duration(duration)}")
                    
                details = " | ".join(info_parts)
                if details:
                    file_names.append(f"{i}. {name} ({details})")
                else:
                    file_names.append(f"{i}. {name}")
    
    file_name = "\n".join(file_names) if group else (getattr(media, "file_name", None) or "").strip()

    if caption_text and file_name and not group and caption_text == file_name:
        file_name = ""

    if not caption_text and not file_name:
        if is_photo:
            file_name = ""
        else:
            file_name = "Video" if is_video else "File"

    return caption_text, file_name


def _build_audio_text(info: dict[str, Any]) -> str:
    labels = []
    for track in info.get("audio", []):
        language = get_full_language_name(str(track.get("language", "")))
        if language != "Unknown":
            labels.append(language)

    labels = _unique(labels)
    return ", ".join(labels)


def _build_subtitle_text(info: dict[str, Any]) -> str:
    labels = []
    for track in info.get("subtitle", []):
        language = get_full_language_name(str(track.get("language", "")))
        labels.append("SUB" if language == "Unknown" else language)

    labels = _unique(labels)
    return ", ".join(labels)


def _truncate(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    if limit == 1:
        return "…"
    return text[: limit - 1].rstrip() + "…"


def _render_caption_template(values: dict[str, str]) -> str:
    try:
        return CAPTION_TEMPLATE.format(**values).strip()
    except (IndexError, KeyError, ValueError) as exc:
        logger.warning("Invalid CAPTION_TEMPLATE (%s). Falling back to default template.", exc)
        return DEFAULT_CAPTION_TEMPLATE.format(**values).strip()


def _build_caption(message: Any, media: Any, info: dict[str, Any], group: Optional[list[Any]] = None) -> str:
    is_photo = _is_photo_message(message)
    is_video = _is_video_message(message)
    raw_caption, raw_files = _extract_text_and_files(message, media, is_photo=is_photo, is_video=is_video, group=group)
    
    caption_text = html.escape(raw_caption)
    file_list = html.escape(raw_files)

    merged_info = _merge_info(info, _base_info_from_message(message, media))
    size_value = merged_info.get("format", {}).get("size") or getattr(media, "file_size", 0) or 0

    if not (is_photo or is_video):
        info_block = f"📄 <b>{html.escape(_human_size(size_value))}</b>"
        lines = []
        if caption_text: lines.append(f"<b>{caption_text}</b>")
        lines.append(info_block)
        if file_list: lines.append(f"<b>{file_list}</b>")
        return _truncate("\n".join(lines), CAPTION_LIMIT)

    duration = merged_info.get("format", {}).get("duration") or getattr(media, "duration", 0) or 0
    media_emoji = "📸" if is_photo else "🎬"
    video_line = html.escape(_build_video_line(merged_info, is_photo=is_photo))
    audio_text = html.escape(_build_audio_text(merged_info))
    subtitle_text = html.escape(_build_subtitle_text(merged_info))

    lines: list[str] = []
    if caption_text:
        lines.append(f"<b>{caption_text}</b>")

    if not group:
        primary_line = f"{media_emoji} <b>{video_line}</b>"
        if duration and not is_photo:
            primary_line += f" | ⏳ <b>{html.escape(_fmt_duration(duration))}</b>"
        lines.append(primary_line)

        if audio_text:
            lines.append(f"🔊 <b>{audio_text}</b>")
        if subtitle_text:
            lines.append(f"💬 <b>{subtitle_text}</b>")

    if file_list:
        lines.append(f"<b>{file_list}</b>")

    caption = "\n".join(lines).strip()
    if len(caption) <= CAPTION_LIMIT:
        return caption

    if len(caption) > CAPTION_LIMIT and caption_text:
        lines_without_caption = lines[1:]
        static_text = "\n".join(lines_without_caption).strip()
        available_len = max(CAPTION_LIMIT - len(static_text) - 6, 0)
        truncated_caption = _truncate(caption_text, available_len)
        lines = [f"<b>{truncated_caption}</b>", *lines_without_caption]
        caption = "\n".join(lines).strip()

    if len(caption) > CAPTION_LIMIT:
        logger.warning("Caption too long for msg %s, truncating final output.", getattr(message, "id", "?"))
        return _truncate(caption, CAPTION_LIMIT)
    return caption


def caption_has_media_info(caption: str) -> bool:
    if not caption:
        return False
    if not MEDIA_INFO_LINE_RE.search(caption):
        return False
    if any(token in caption for token in ("⏳", "🔊", "💬", "KiB", "MiB", "GiB", "TiB", "B</b>", "Media Info Unavailable")):
        return True
    return bool(re.search(r"[📸🎬]\s*(?:<b>)?\s*\d+(?:x\d+|p)", caption))


async def _stream_chunk(client: Client, media: Any, size: int, path: str) -> bool:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            written = 0
            async with stream_semaphore:
                async with aiopen(path, "wb") as file_obj:
                    async for chunk in client.stream_media(media):
                        if not chunk:
                            break
                        remaining = size - written
                        if remaining <= 0:
                            break
                        piece = chunk[:remaining]
                        await file_obj.write(piece)
                        written += len(piece)
                        if written >= size:
                            break
            return os.path.exists(path) and os.path.getsize(path) > 0
        except FloodWait:
            raise
        except OSError as exc:
            # Non-retryable filesystem error
            logger.warning("stream_chunk I/O error (%s): %s", size, exc)
            return False
        except Exception as exc:
            exc_name = type(exc).__name__
            exc_msg = str(exc).strip() or "(no message)"
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s
                logger.debug(
                    "stream_chunk %s/%s failed (%s) [%s: %s] — retrying in %ss",
                    attempt + 1, max_retries, size, exc_name, exc_msg, wait,
                )
                await asyncio.sleep(wait)
                continue
            logger.warning(
                "stream_chunk failed after %s attempts (%s) [%s: %s]",
                max_retries, size, exc_name, exc_msg,
            )
            return False
    return False


def _has_enough_info(info: dict[str, Any], *, is_photo: bool) -> bool:
    if not info.get("video"):
        return False

    track = info["video"][0]
    if is_photo:
        return bool(track.get("width") or track.get("height"))

    if track.get("width") or track.get("height"):
        return True
    if get_video_format(
        str(track.get("codec", "")),
        str(track.get("transfer", "")),
        str(track.get("hdr", "")),
        str(track.get("bit_depth", "")),
    ):
        return True
    return bool(info.get("audio") or info.get("subtitle"))


async def process_message(client: Client, message: Any, progress_msg: Any = None, group: Optional[list[Any]] = None, stream_client: Optional[Client] = None) -> tuple[str, Optional[str]]:
    media = message.video or message.document or message.photo or getattr(message, "animation", None) or getattr(message, "sticker", None)
    if not media:
        return "", None

    # Use dedicated stream_client for media downloads if provided
    _streamer = stream_client or client

    link = getattr(message, "link", None) or f"ID: {message.id}"
    _status_print(f"⚡ Processing: {link}")

    is_photo = _is_photo_message(message)
    is_video = _is_video_message(message)
    base_info = _base_info_from_message(message, media)

    if message.photo:
        return _build_caption(message, media, base_info, group=group), None

    if not (is_photo or is_video):
        logger.info("Zero-download processing for msg %s (generic document)", message.id)
        return _build_caption(message, media, base_info, group=group), None

    async def _update(text: str) -> None:
        if progress_msg:
            await _safe_edit(progress_msg, text)

    file_size = getattr(media, "file_size", 0) or 0
    steps = PHOTO_STREAM_STEPS if is_photo else VIDEO_STREAM_STEPS
    info = base_info
    last_tmp = None

    for step in steps:
        limit = min(step, file_size) if file_size else step
        if limit <= 0:
            continue

        tmp = _new_tmp_path("probe", message.id)
        try:
            await _update(f"🔍 Sampling {_human_size(limit)}…")
            if not await _stream_chunk(_streamer, media, limit, tmp):
                await _remove_path(tmp)
                continue

            probed_info = await _probe(tmp)
            info = _merge_info(probed_info, info)

            if _has_enough_info(info, is_photo=is_photo):
                last_tmp = tmp
                break

            await _remove_path(tmp)
        except FloodWait:
            await _remove_path(tmp)
            raise
        except Exception as exc:
            logger.warning("Sampling step failed for msg %s: %s", message.id, exc)
            await _remove_path(tmp)

    return _build_caption(message, media, info, group=group), last_tmp


async def _safe_edit(msg: Any, text: str, parse_mode: Optional[ParseMode] = None, *, force: bool = False) -> None:
    if not msg:
        return

    key = msg.id
    now = _loop_time()
    if not force and key in _last_edit and now - _last_edit[key] < 1.7:
        return

    try:
        await msg.edit_text(text, parse_mode=parse_mode)
        _last_edit[key] = now
    except MessageNotModified:
        pass
    except Exception:
        logger.debug("Failed to edit message %s", key, exc_info=True)


async def _get_media_group_target(client: Client, message: Any) -> tuple[Any, Optional[list[Any]]]:
    media_group_id = getattr(message, "media_group_id", None)
    if not media_group_id:
        return message, None

    try:
        group = await client.get_media_group(message.chat.id, message.id)
    except Exception as exc:
        logger.debug("Failed to fetch media group %s: %s", media_group_id, exc)
        return message, None

    if not group:
        return message, None

    sorted_group = sorted(group, key=lambda item: item.id)
    caption_message = next((item for item in sorted_group if (item.caption or "").strip()), None)
    return caption_message or sorted_group[0], sorted_group


async def _process_channel_queue(channel_id: int) -> None:
    global EDIT_DELAY

    async with channel_locks[channel_id]:
        while channel_queues[channel_id]:
            message, caption = channel_queues[channel_id].pop(0)
            now = _loop_time()
            last = last_edit_time.get(channel_id, 0)

            if now - last < EDIT_DELAY:
                await asyncio.sleep(EDIT_DELAY - (now - last))

            try:
                await message.edit_caption(caption, parse_mode=ParseMode.HTML)
                last_edit_time[channel_id] = _loop_time()
            except MessageNotModified:
                last_edit_time[channel_id] = _loop_time()
            except FloodWait as exc:
                EDIT_DELAY = max(EDIT_DELAY, exc.value / 10 + 1)
                await asyncio.sleep(exc.value)
                try:
                    await message.edit_caption(caption, parse_mode=ParseMode.HTML)
                    last_edit_time[channel_id] = _loop_time()
                except MessageNotModified:
                    last_edit_time[channel_id] = _loop_time()
                except Exception as retry_exc:
                    logger.error("Retry edit failed for %s: %s", message.id, retry_exc)
            except Exception as exc:
                logger.error("Edit failed for %s: %s", message.id, exc)


@app.on_message((filters.video | filters.document | filters.photo | filters.animation | filters.sticker) & (filters.channel | filters.group) & ALLOWED_CHAT_FILTER & ~filters.service)
async def channel_handler(_, message: Any) -> None:
    media_group_id = getattr(message, "media_group_id", None)
    if not media_group_id or message.document:
        if caption_has_media_info(message.caption or ""):
            return

        caption, file_path = await process_message(app, message)
        logger.info("Generated live caption for %s", message.id)

        channel_id = message.chat.id
        channel_queues[channel_id].append((message, caption))
        asyncio.create_task(_process_channel_queue(channel_id))

        await _remove_path(file_path)
        return

    await asyncio.sleep(1.0)
    async with media_group_locks[str(media_group_id)]:
        _prune_processed_media_groups()
        if str(media_group_id) in processed_media_groups:
            return

        target_message, group = await _get_media_group_target(app, message)
        if target_message.id != message.id:
            return

        if caption_has_media_info(target_message.caption or ""):
            processed_media_groups[str(media_group_id)] = _loop_time()
            return

        caption, file_path = await process_message(app, target_message, group=group)
        logger.info("Generated gallery caption for %s (group %s)", target_message.id, media_group_id)

        channel_id = target_message.chat.id
        channel_queues[channel_id].append((target_message, caption))
        processed_media_groups[str(media_group_id)] = _loop_time()
        asyncio.create_task(_process_channel_queue(channel_id))

        await _remove_path(file_path)


@app.on_message(filters.private & (filters.video | filters.document | filters.photo | filters.animation | filters.sticker))
async def private_handler(_, message: Any) -> None:
    user_id = getattr(message.from_user, "id", None)
    if user_id is None:
        return
    if user_id in active_users:
        await message.reply_text("⚠️ Please wait until your current file is processed.")
        return
    active_users.add(user_id)
    asyncio.create_task(_handle_private(message))


async def _handle_private(message: Any) -> None:
    file_path = None
    progress_msg = None
    user_id = getattr(message.from_user, "id", None)

    try:
        await asyncio.sleep(0.5)
        progress_msg = await message.reply_text("⏳ Processing…")
        caption, file_path = await process_message(app, message, progress_msg)
        await _safe_edit(progress_msg, caption, parse_mode=ParseMode.HTML, force=True)
    except Exception as exc:
        logger.error("Private handler error: %s", exc)
        if progress_msg:
            await _safe_edit(
                progress_msg,
                f"❌ Failed to analyze this file.\n\n<code>{html.escape(str(exc))}</code>",
                parse_mode=ParseMode.HTML,
                force=True,
            )
    finally:
        if user_id is not None:
            active_users.discard(user_id)
        await _remove_path(file_path)


@app.on_message(filters.command("info") & filters.reply)
async def info_command(_, message: Any) -> None:
    reply = message.reply_to_message
    if not reply or not (reply.video or reply.document or reply.photo):
        await message.reply_text("⚠️ Reply to a video, photo or document.")
        return

    media = reply.video or reply.document or reply.photo
    tmp = _new_tmp_path("info", reply.id)
    download_path = None

    try:
        result = _base_info_from_message(reply, media)

        if reply.photo:
            caption = _build_caption(reply, media, result)
            await message.reply_text(caption, parse_mode=ParseMode.HTML)
            return

        ok = await _stream_chunk(app, media, 8 * 1024 * 1024, tmp)
        if ok:
            result = _merge_info(await _probe(tmp), result)

        if not _has_enough_info(result, is_photo=_is_photo_message(reply)) and not reply.photo:
            download_path = _download_path("info", reply.id, getattr(media, "file_name", None))
            downloaded = await reply.download(file_name=download_path)
            result = _merge_info(await _probe(downloaded), result)

        caption = _build_caption(reply, media, result)
        await message.reply_text(caption, parse_mode=ParseMode.HTML)
    except Exception as exc:
        await message.reply_text(
            f"❌ Failed\n\n<code>{html.escape(str(exc))}</code>",
            parse_mode=ParseMode.HTML,
        )
    finally:
        await _remove_path(tmp)
        await _remove_path(download_path)


@app.on_message(filters.command("start") & filters.private)
async def start(_, message: Any) -> None:
    await message.reply_text(
        "<b>🎬 Media Info Bot</b>\n\n"
        "Send me a video, photo, or document and I'll add clean media details.\n\n"
        "I can show:\n"
        "• 🎞 Resolution, codec, bit depth, and HDR flags\n"
        "• ⏳ Duration\n"
        "• 🔊 Audio languages\n"
        "• 💬 Subtitle info\n\n"
        "<b>⚡ Fast • Clean • Accurate</b>\n\n"
        "📌 <i>Note:</i> Send one file at a time.\n\n"
        "🤖 Bot by @piroxbots",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("server") & ADMIN_FILTER)
async def server_cmd(_, message: Any) -> None:
    await message.reply_text(
        f"CPU: {psutil.cpu_percent()}%\n"
        f"RAM: {psutil.virtual_memory().percent}%\n"
        f"Disk: {psutil.disk_usage('/').percent}%"
    )


@app.on_message(filters.command("restart") & ADMIN_FILTER)
async def restart_cmd(_, message: Any) -> None:
    await message.reply_text("Restarting…")
    os.execv(sys.executable, [sys.executable, *sys.argv])


@app.on_message(filters.command("shutdown") & ADMIN_FILTER)
async def shutdown_cmd(_, message: Any) -> None:
    await message.reply_text("Shutting down…")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await app.stop()
    finally:
        if user_app:
            try:
                await user_app.stop()
            except Exception:
                pass
        os._exit(0)


async def _run_command(cmd: list[str]) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    output = "\n".join(part for part in (stdout.decode().strip(), stderr.decode().strip()) if part)
    return proc.returncode, output


@app.on_message(filters.command("update") & ADMIN_FILTER)
async def update_cmd(_, message: Any) -> None:
    await message.reply_text("Updating…")
    try:
        pull_code, pull_output = await _run_command(["git", "pull", "--ff-only"])
        if pull_code != 0:
            await message.reply_text(
                f"❌ git pull failed.\n\n<code>{html.escape(pull_output or 'Unknown error')}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        pip_code, pip_output = await _run_command(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--no-cache-dir", "-q"]
        )
        if pip_code != 0:
            await message.reply_text(
                f"❌ Dependency update failed.\n\n<code>{html.escape(pip_output or 'Unknown error')}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        await message.reply_text("✅ Updated. Restarting…")
        os.execv(sys.executable, [sys.executable, *sys.argv])
    except Exception as exc:
        await message.reply_text(
            f"❌ Update failed.\n\n<code>{html.escape(str(exc))}</code>",
            parse_mode=ParseMode.HTML,
        )


_scan_active: dict[str, bool] = {}


@app.on_message(filters.command("scan") & ADMIN_FILTER)
async def scan_cmd(_, message: Any) -> None:
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply_text(
            "⚠️ Usage: <code>/scan &lt;chat_id_or_link&gt; [limit] [offset_id]</code>\n\n"
            "Examples:\n"
            "<code>/scan https://t.me/c/12345/678</code> — scan starting from msg 678\n"
            "<code>/scan -1001234567890 100</code> — scan last 100 posts\n"
            "<code>/scan username 0 1234</code> — scan starting from msg 1234",
            parse_mode=ParseMode.HTML,
        )
        return

    target_str = parts[1]
    offset_id = 0
    chat_id: Union[int, str] = 0

    if target_str.startswith("http") or target_str.startswith("t.me"):
        clean_link = target_str.replace("https://", "").replace("http://", "").replace("t.me/", "")
        link_parts = [p for p in clean_link.split("/") if p]
        if len(link_parts) >= 2:
            if link_parts[0] == "c":
                try:
                    chat_id = int("-100" + link_parts[1])
                    offset_id = int(link_parts[-1]) + 1  # offset_id is exclusive in Telegram API
                except ValueError:
                    pass
            else:
                chat_id = link_parts[0]
                try:
                    offset_id = int(link_parts[-1]) + 1  # offset_id is exclusive in Telegram API
                except ValueError:
                    pass
                    
        if not chat_id:
            await message.reply_text("❌ Invalid link format.")
            return
    else:
        try:
            chat_id = int(target_str)
        except ValueError:
            chat_id = target_str

    limit = 0
    if len(parts) >= 3:
        try:
            limit = int(parts[2])
        except ValueError:
            await message.reply_text("❌ Invalid limit number.")
            return
        if limit < 0:
            await message.reply_text("❌ Limit must be 0 or greater.")
            return

    if len(parts) >= 4:
        try:
            offset_id = int(parts[3])
        except ValueError:
            await message.reply_text("❌ Invalid offset_id number.")
            return
        if offset_id < 0:
            await message.reply_text("❌ offset_id must be 0 or greater.")
            return

    chat_id_str = str(chat_id)
    if _scan_active.get(chat_id_str):
        await message.reply_text("⚠️ A scan is already running for this chat.")
        return

    _scan_active[chat_id_str] = True
    asyncio.create_task(_run_scan(message, chat_id, limit, offset_id))


@app.on_message(filters.command("stopscan") & ADMIN_FILTER)
async def stopscan_cmd(_, message: Any) -> None:
    parts = message.text.split()
    if len(parts) < 2:
        running = [cid for cid, active in _scan_active.items() if active]
        if running:
            await message.reply_text(
                f"⚠️ Usage: <code>/stopscan &lt;channel_id&gt;</code>\n\nActive scans: {', '.join(running)}",
                parse_mode=ParseMode.HTML,
            )
            return
        await message.reply_text("ℹ️ No active scans.")
        return

    chat_id_str = parts[1]

    if _scan_active.get(chat_id_str):
        _scan_active[chat_id_str] = False
        await message.reply_text("🛑 Scan will stop after the current file finishes.")
    else:
        await message.reply_text("ℹ️ No active scan for this channel.")


async def _resolve_scan_client(chat_id: Union[int, str]) -> Client:
    try:
        await app.get_chat(chat_id)
        async for _ in app.get_chat_history(chat_id, limit=1):
            break
        return app
    except Exception:
        if user_app:
            logger.info("Bot access failed for %s, falling back to User Helper", chat_id)
            return user_app
        raise


async def _run_scan(admin_msg: Any, chat_id: Union[int, str], limit: int, offset_id: int = 0) -> None:
    chat_id_str = str(chat_id)
    try:
        history_client = await _resolve_scan_client(chat_id)
    except Exception:
        _scan_active[chat_id_str] = False
        await admin_msg.reply_text(
            "❌ <b>Access Denied</b>\n\n"
            "The bot cannot access this chat history. "
            "Configure <code>STRING_SESSION</code> in your .env to enable the User Helper fallback.",
            parse_mode=ParseMode.HTML,
        )
        return

    status = await admin_msg.reply_text(
        f"🔍 Starting scan of <code>{chat_id}</code> using "
        f"{'<b>User Helper</b>' if history_client == user_app else '<b>Bot Account</b>'}… "
        f"({SCAN_WORKERS} workers)",
        parse_mode=ParseMode.HTML,
    )

    counters = {"scanned": 0, "edited": 0, "skipped": 0, "errors": 0}
    scanned_media_groups: set[str] = set()

    # Semaphore limits concurrent downloads/probes
    proc_sem = asyncio.Semaphore(SCAN_WORKERS)
    # Lock + timestamp ensure edits are serialized with minimum EDIT_DELAY between them
    edit_lock = asyncio.Lock()
    last_edit_ts: list[float] = [0.0]

    async def _edit_with_delay(msg_id: int, caption: str) -> None:
        async with edit_lock:
            wait = EDIT_DELAY - (_loop_time() - last_edit_ts[0])
            if wait > 0:
                await asyncio.sleep(wait)
            # Always try bot account first for edits, fallback to history_client
            for edit_client in ([app, history_client] if history_client != app else [app]):
                try:
                    await edit_client.edit_message_caption(chat_id, msg_id, caption, parse_mode=ParseMode.HTML)
                    counters["edited"] += 1
                    break
                except MessageNotModified:
                    counters["skipped"] += 1
                    break
                except FloodWait as exc:
                    await _handle_flood_wait(exc.value)
                    await asyncio.sleep(exc.value)
                    continue
                except Exception as exc:
                    if edit_client != history_client:
                        logger.debug("Bot edit failed for %s, trying user helper: %s", msg_id, exc)
                        continue
                    logger.error("Edit failed for %s: %s", msg_id, exc)
                    counters["errors"] += 1
            last_edit_ts[0] = _loop_time()


    async def _process_one(message: Any, group: Any) -> None:
        file_path = None
        async with proc_sem:
            try:
                # Check global flood wait before starting
                now = _loop_time()
                if now < _flood_wait_until:
                    await asyncio.sleep(_flood_wait_until - now)

                caption, file_path = await process_message(history_client, message, group=group)
            except FloodWait as exc:
                await _handle_flood_wait(exc.value)
                counters["errors"] += 1
                return
            except Exception as exc:
                logger.error("Scan process error for %s: %s", message.id, exc)
                counters["errors"] += 1
                return
            finally:
                await _remove_path(file_path)
        await _edit_with_delay(message.id, caption)

    pending: list[asyncio.Task] = []

    try:
        async for message in history_client.get_chat_history(chat_id, limit=limit or None, offset_id=offset_id or 0):
            media_group_id = getattr(message, "media_group_id", None)
            group = None
            if media_group_id and not message.document:
                media_group_key = str(media_group_id)
                if media_group_key in scanned_media_groups:
                    continue
                scanned_media_groups.add(media_group_key)
                message, group = await _get_media_group_target(history_client, message)

            if not _scan_active.get(chat_id_str, False):
                break

            if not (message.video or message.document or message.photo or getattr(message, "animation", None) or getattr(message, "sticker", None)):
                continue

            # Check global flood wait in main loop
            now = _loop_time()
            if now < _flood_wait_until:
                wait_time = _flood_wait_until - now
                logger.info("Main scan loop waiting for FloodWait: %ss", int(wait_time))
                await asyncio.sleep(wait_time)

            counters["scanned"] += 1

            if caption_has_media_info(message.caption or ""):
                counters["skipped"] += 1
                continue

            pending.append(asyncio.create_task(_process_one(message, group)))

            if counters["scanned"] % 25 == 0:
                await _safe_edit(
                    status,
                    f"🔍 Scanning… {counters['scanned']} checked | ✅ {counters['edited']} edited | ⏭ {counters['skipped']} skipped | ❌ {counters['errors']} errors",
                )

        # Wait for all in-flight tasks to finish
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        if not _scan_active.get(chat_id_str, False):
            await _safe_edit(
                status,
                f"🛑 Scan stopped.\n\n📊 Scanned: {counters['scanned']} | ✅ Edited: {counters['edited']} | ⏭ Skipped: {counters['skipped']} | ❌ Errors: {counters['errors']}",
                force=True,
            )
            return

    except FloodWait as exc:
        logger.warning("Scan history FloodWait: sleeping %ss", exc.value)
        await asyncio.sleep(exc.value)
    except Exception as exc:
        logger.error("Scan failed: %s", exc)
        await _safe_edit(
            status,
            f"❌ Scan error: <code>{html.escape(str(exc))}</code>\n\n"
            f"📊 Scanned: {counters['scanned']} | ✅ Edited: {counters['edited']} | ⏭ Skipped: {counters['skipped']} | ❌ Errors: {counters['errors']}",
            parse_mode=ParseMode.HTML,
            force=True,
        )
        return
    finally:
        await _safe_edit(
            status,
            f"✅ <b>Scan Complete!</b>\n\n📊 Scanned: {counters['scanned']} | ✅ Edited: {counters['edited']} | ⏭ Skipped: {counters['skipped']} | ❌ Errors: {counters['errors']}",
            parse_mode=ParseMode.HTML,
            force=True,
        )
        _scan_active.pop(chat_id_str, None)


def _install_deps() -> None:
    missing = []
    for binary, package in (("ffprobe", "ffmpeg"), ("mediainfo", "mediainfo")):
        if shutil.which(binary):
            continue
        missing.append(package)
        logger.warning(
            "❌ Missing dependency: %s. Install it manually (for example: sudo apt install %s).",
            package,
            package,
        )

    if not missing:
        logger.info("System dependencies are available.")


async def main() -> None:
    validate_config()
    gc.set_threshold(*GC_THRESHOLD)
    _install_deps()

    if not ALLOWED_CHATS:
        logger.warning("ALLOWED_CHATS is empty. Channel/group auto-editing is currently disabled.")

    await app.start()
    if user_app:
        await user_app.start()

    try:
        me = await app.get_me()
        logger.info("@%s started", me.username or me.id)
        if user_app:
            helper = await user_app.get_me()
            logger.info("User Helper started: @%s", helper.username or helper.id)

        try:
            await app.send_message(ADMIN_ID, "🚀 Bot Started")
        except PeerIdInvalid:
            logger.warning("Could not send startup message to ADMIN_ID %s. Start the bot in Telegram first.", ADMIN_ID)
        except Exception as exc:
            logger.warning("Startup message failed: %s", exc)

        scheduler.add_job(gc.collect, "interval", minutes=20)
        scheduler.start()

        await asyncio.Event().wait()
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        if user_app:
            try:
                await user_app.stop()
            except Exception:
                pass
        try:
            await app.stop()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        app.run(main())
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
