import asyncio

# Fix for Python 3.12+ / 3.14 RuntimeError: No current event loop
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import json
import subprocess
import os
import logging
import sys
import psutil
import gc
import re
import uuid
import time
from aiofiles import open as aiopen
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from functools import lru_cache
from typing import Optional
from aiofiles.os import remove as aioremove
from pyrogram.errors import MessageNotModified, FloodWait, PeerIdInvalid
from collections import defaultdict
from config import (
    API_ID, API_HASH, BOT_TOKEN, STRING_SESSION,
    ADMIN_ID, ALLOWED_CHATS,
    LOG_FORMAT, LOG_LEVEL,
    GC_THRESHOLD,
    CAPTION_TEMPLATE,
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
    sleep_threshold=60,
)

user_app = Client(
    "MediaInfo-User",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=STRING_SESSION,
    workers=2,
    sleep_threshold=60,
) if STRING_SESSION else None

stream_semaphore  = asyncio.Semaphore(4)
channel_semaphore = asyncio.Semaphore(3)
active_users: set = set()

_last_edit:      dict[int, float] = {}
channel_queues:  dict[int, list]  = defaultdict(list)
channel_locks:   dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
last_edit_time:  dict[int, float] = {}
EDIT_DELAY = 3.5

scheduler = AsyncIOScheduler()
TMP_DIR   = "tmp"

def _human_size(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"

def _human_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

async def aioremove(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning(f"Error removing {path}: {e}")

def _init_tmp():
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
    else:
        # Clear existing temp files on startup
        for f in os.listdir(TMP_DIR):
            try:
                os.remove(os.path.join(TMP_DIR, f))
            except:
                pass
    logger.info("Temporary directory initialized and cleared.")

_init_tmp()


_LANGUAGE_MAP: dict[str, str] = {
    'en': 'English',  'eng': 'English',
    'hi': 'Hindi',    'hin': 'Hindi',
    'ta': 'Tamil',    'tam': 'Tamil',
    'te': 'Telugu',   'tel': 'Telugu',
    'ml': 'Malayalam','mal': 'Malayalam',
    'kn': 'Kannada',  'kan': 'Kannada',
    'bn': 'Bengali',  'ben': 'Bengali',
    'mr': 'Marathi',  'mar': 'Marathi',
    'gu': 'Gujarati', 'guj': 'Gujarati',
    'pa': 'Punjabi',  'pun': 'Punjabi',
    'bho':'Bhojpuri',
    'zh': 'Chinese',  'chi': 'Chinese',  'cmn': 'Chinese',
    'ko': 'Korean',   'kor': 'Korean',
    'pt': 'Portuguese','por': 'Portuguese',
    'th': 'Thai',     'tha': 'Thai',
    'tl': 'Tagalog',  'tgl': 'Tagalog',  'fil': 'Tagalog',
    'ja': 'Japanese', 'jpn': 'Japanese',
    'es': 'Spanish',  'spa': 'Spanish',
    'sv': 'Swedish',  'swe': 'Swedish',
    'fr': 'French',   'fra': 'French',   'fre': 'French',
    'de': 'German',   'deu': 'German',   'ger': 'German',
    'it': 'Italian',  'ita': 'Italian',
    'ru': 'Russian',  'rus': 'Russian',
    'ar': 'Arabic',   'ara': 'Arabic',
    'tr': 'Turkish',  'tur': 'Turkish',
    'nl': 'Dutch',    'nld': 'Dutch',    'dut': 'Dutch',
    'pl': 'Polish',   'pol': 'Polish',
    'vi': 'Vietnamese','vie': 'Vietnamese',
    'id': 'Indonesian','ind': 'Indonesian',
    'ms': 'Malay',    'msa': 'Malay',    'may': 'Malay',
    'fa': 'Persian',  'fas': 'Persian',  'per': 'Persian',
    'ur': 'Urdu',     'urd': 'Urdu',
    'he': 'Hebrew',   'heb': 'Hebrew',
    'el': 'Greek',    'ell': 'Greek',    'gre': 'Greek',
    'hu': 'Hungarian','hun': 'Hungarian',
    'cs': 'Czech',    'ces': 'Czech',    'cze': 'Czech',
    'ro': 'Romanian', 'ron': 'Romanian', 'rum': 'Romanian',
    'da': 'Danish',   'dan': 'Danish',
    'fi': 'Finnish',  'fin': 'Finnish',
    'no': 'Norwegian','nor': 'Norwegian',
    'uk': 'Ukrainian','ukr': 'Ukrainian',
    'ca': 'Catalan',  'cat': 'Catalan',
    'hr': 'Croatian', 'hrv': 'Croatian',
    'sk': 'Slovak',   'slk': 'Slovak',   'slo': 'Slovak',
    'sr': 'Serbian',  'srp': 'Serbian',
    'bg': 'Bulgarian','bul': 'Bulgarian',
    'unknown': 'Original Audio',
}


@lru_cache(maxsize=256)
def get_full_language_name(code: str) -> str:
    if not code:
        return 'Unknown'
    cleaned = code.split('(')[0].strip()
    return _LANGUAGE_MAP.get(cleaned.lower(), 'Original Audio')


@lru_cache(maxsize=64)
def get_standard_resolution(height: int) -> Optional[str]:
    if not height:
        return None
    if height <= 240:  return "240p"
    if height <= 360:  return "360p"
    if height <= 480:  return "480p"
    if height <= 720:  return "720p"
    if height <= 1080: return "1080p"
    if height <= 1440: return "1440p"
    if height <= 2160: return "2160p"
    return "2160p+"


@lru_cache(maxsize=128)
def get_video_format(codec: str, transfer: str = '', hdr: str = '', bit_depth: str = '') -> Optional[str]:
    if not codec:
        return None
    codec = codec.lower()
    parts: list[str] = []

    if   any(x in codec for x in ('hevc', 'h.265', 'h265')):  parts.append('HEVC')
    elif 'av1' in codec:                                        parts.append('AV1')
    elif any(x in codec for x in ('avc', 'avc1', 'h.264', 'h264')): parts.append('x264')
    elif 'vp9' in codec:                                        parts.append('VP9')
    elif any(x in codec for x in ('mpeg4', 'xvid')):            parts.append('MPEG4')
    else:
        return None

    try:
        if bit_depth and int(bit_depth) > 8:
            parts.append(f"{bit_depth}bit")
    except (ValueError, TypeError):
        pass

    t = transfer.lower();  h = hdr.lower()
    if any(x in t for x in ('pq', 'hlg', 'smpte', '2084', 'st 2084')) or 'hdr' in h or 'dolby' in h:
        parts.append('HDR')

    return ' '.join(parts)


def _is_video_track(track: dict) -> bool:
    t      = (track.get('@type',        '') or '').lower()
    fmt    = (track.get('Format',       '') or '').lower()
    cid    = (track.get('CodecID',      '') or '').lower()
    fp     = (track.get('Format_Profile','') or '').lower()
    title  = (track.get('Title',        '') or '').lower()
    menu   = str(track.get('MenuID',    '') or '').lower()

    return any([
        t == 'video',
        any(x in fmt for x in ('avc','hevc','h.264','h264','h.265','h265','av1','vp9','mpeg-4','mpeg4','xvid')),
        any(x in cid for x in ('avc','h264','hevc','h265','av1','vp9','mpeg4','xvid','27')),
        'video' in menu,
        'video' in title,
        any(x in fp  for x in ('main','high','baseline')),
    ])


def _has_subtitles(tracks: list) -> bool:
    for track in tracks:
        if not isinstance(track, dict):
            continue
        t   = (track.get('@type',       '') or '').lower()
        fmt = (track.get('Format',      '') or '').lower()
        cid = (track.get('CodecID',     '') or '').lower()
        enc = (track.get('Encoding',    '') or '').lower()
        fi  = (track.get('Format_Info', '') or '').lower()
        ttl = (track.get('Title',       '') or '').lower()
        if any([
            t == 'text',
            any(x in fmt for x in ('pgs','subrip','ass','ssa','srt','dvb_subtitle','dvd_subtitle')),
            any(x in cid for x in ('s_text','subp','pgs','subtitle','dvb','dvd')),
            any(x in enc for x in ('utf-8','utf8','unicode','text')),
            any(x in fi  for x in ('subtitle','caption','text')),
            'subtitle' in ttl,
        ]):
            return True
    return False


def _parse_int(value) -> int:
    try:
        return int(re.findall(r"\d+", str(value))[0])
    except Exception:
        return 0


def _parse_duration(value) -> float:
    try:
        if not value:
            return 0
        v = str(value).strip()
        if v.replace('.', '', 1).lstrip('-').isdigit():
            f = float(v)
            if f > 86_400_000:
                return f / 1_000_000
            if f > 86_400:
                return f / 1_000
            return f
        if ':' in v:
            parts = [float(p) for p in v.split(':')]
            if len(parts) == 3:
                return parts[0]*3600 + parts[1]*60 + parts[2]
            if len(parts) == 2:
                return parts[0]*60 + parts[1]
    except Exception:
        pass
    return 0


def _fmt_duration(s: float) -> str:
    s = int(s)
    return f"{s//3600:02}:{(s%3600)//60:02}:{s%60:02}"


async def _run_mediainfo(path: str) -> dict:
    try:
        proc = await asyncio.create_subprocess_shell(
            f'mediainfo --ParseSpeed=0 --Language=raw --Output=JSON "{path}"',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        except asyncio.TimeoutError:
            proc.kill();  await proc.wait()
            return {}
        return json.loads(stdout.decode() or '{}')
    except Exception as e:
        logger.warning(f"mediainfo error: {e}")
        return {}


# Obsolete legacy functions removed
async def _probe(path: str) -> dict:
    """Analyze media file and return metadata dictionary."""
    try:
        if not os.path.exists(path):
            return {}
            
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format", "-show_streams",
            path
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if not stdout:
            return {}
            
        data = json.loads(stdout)
        streams = data.get("streams", [])
        
        info = {
            "format": data.get("format", {}),
            "video": [s for s in streams if s.get("codec_type") == "video"],
            "audio": [s for s in streams if s.get("codec_type") == "audio"],
            "subtitle": [s for s in streams if s.get("codec_type") == "subtitle"]
        }
        return info
    except Exception as e:
        logger.error(f"Probe error: {e}")
        return {}


def _build_caption(message, media, info: dict) -> str:
    """Compose the final HTML caption, preserving the original."""
    # Robust type detection
    is_photo = False
    is_video = False
    default_name = "File"

    if message.photo:
        is_photo = True
        default_name = "Photo"
    elif message.video:
        is_video = True
        default_name = "Video"
    elif message.document:
        mime = message.document.mime_type or ""
        if mime.startswith("image/"):
            is_photo = True
            default_name = "Photo"
        elif mime.startswith("video/"):
            is_video = True
            default_name = "Video"
        else:
            default_name = getattr(media, 'file_name', 'File') or 'File'
    
    orig_caption = message.caption or default_name
    size_str = _human_size(getattr(media, 'file_size', 0))

    v_tracks = info.get("video", [])
    a_tracks = info.get("audio", [])
    s_tracks = info.get("subtitle", [])

    # Resolution & Duration line
    res_dur = []
    if v_tracks:
        v = v_tracks[0]
        emoji = "📸" if is_photo else "🎬"
        res_dur.append(f"{emoji} {v.get('width','?')}x{v.get('height','?')}")
    
    duration = info.get("format", {}).get("duration")
    if duration:
        res_dur.append(f"⏳ {_human_time(float(duration))}")
    
    # If no res/dur, use the File emoji with size
    header = " | ".join(res_dur) if res_dur else f"📄 {size_str}"

    info_lines = [
        "", # Spacer
        f"<b>{header}</b>"
    ]

    # Only add audio/subs if they exist
    if a_tracks:
        audio_info = ", ".join(list(dict.fromkeys(
            [f"{t.get('language', 'Unknown').upper()} ({t.get('codec', 'Audio')})" for t in a_tracks]
        )))
        info_lines.append(f"🔊 {audio_info}")

    if s_tracks:
        sub_info = ", ".join(list(dict.fromkeys(
            [t.get('language', 'Unknown').upper() for t in s_tracks]
        )))
        info_lines.append(f"💬 {sub_info}")

    media_info = "\n".join(info_lines)
    
    # Ensure total length doesn't exceed Telegram's 1024 char limit
    # If it's too long, we prioritize the original caption and skip the info.
    if len(orig_caption) + len(media_info) + 1 > 1024:
        logger.warning(f"Caption too long for msg {message.id}, skipping media info.")
        return orig_caption

    return f"{orig_caption}\n{media_info}"
def caption_has_media_info(caption: str) -> bool:
    if not caption:
        return False
    # Check for any of our "fingerprint" emojis
    return any(e in caption for e in ["🎬", "⏳", "📄", "📸", "📦"])


_STREAM_STEPS = [
    ("16KB",  16  * 1024),
    ("1MB",   1   * 1024 * 1024),
    ("3MB",   3   * 1024 * 1024),
    ("8MB",   8   * 1024 * 1024),
]


async def _stream_chunk(client, media, size: int, path: str) -> bool:
    try:
        written = 0
        async with stream_semaphore:
            async with aiopen(path, 'wb') as f:
                async for chunk in client.stream_media(media):
                    if not chunk:
                        break
                    remaining = size - written
                    if remaining <= 0:
                        break
                    piece = chunk[:remaining]
                    await f.write(piece)
                    written += len(piece)
                    if written >= size:
                        break
        return os.path.exists(path) and os.path.getsize(path) > 0
    except Exception as e:
        logger.warning(f"stream_chunk failed ({size}): {e}")
        return False


async def process_message(client, message, progress_msg=None) -> tuple[str, Optional[str]]:
    """Analyze media with minimal data usage. NEVER does full download."""
    media = message.video or message.document or message.photo
    if not media:
        return "", None

    # 1. Type Detection
    is_photo = message.photo or (message.document and message.document.mime_type and message.document.mime_type.startswith("image/"))
    is_video = message.video or (message.document and message.document.mime_type and message.document.mime_type.startswith("video/"))
    
    # 2. No Download for Generic Documents
    if not (is_photo or is_video):
        logger.info(f"Zero-download processing for msg {message.id} (Document)")
        return _build_caption(message, media, {}), None

    async def _update(text: str):
        if progress_msg:
            await _safe_edit(progress_msg, text)

    # 3. Step-wise probing (Stops early if info found)
    steps = [128 * 1024] if is_photo else [16 * 1024, 1 * 1024 * 1024, 3 * 1024 * 1024, 8 * 1024 * 1024]
    info = {}
    last_tmp = None

    for i, limit in enumerate(steps):
        if limit > media.file_size:
            limit = media.file_size
            
        tmp = os.path.join(TMP_DIR, f"probe_{message.id}_{uuid.uuid4().hex[:8]}.bin")
        try:
            await _update(f"🔍 Sampling {_human_size(limit)}…")
            ok = await _stream_chunk(client, media, limit, tmp)
            if ok:
                info = await _probe(tmp)
                # If we have video info (resolution), we can stop
                if info.get("video"):
                    last_tmp = tmp
                    break
            
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception as e:
            logger.warning(f"Step {i+1} failed: {e}")
            if os.path.exists(tmp):
                await aioremove(tmp)

    return _build_caption(message, media, info), last_tmp


async def _safe_edit(msg, text: str, parse_mode=None):
    if not msg:
        return
    key  = msg.id
    now  = asyncio.get_event_loop().time()
    if key in _last_edit and now - _last_edit[key] < 1.7:
        return
    try:
        await msg.edit_text(text, parse_mode=parse_mode)
        _last_edit[key] = now
    except (MessageNotModified, Exception):
        pass


async def _process_channel_queue(channel_id: int):
    global EDIT_DELAY
    async with channel_locks[channel_id]:
        while channel_queues[channel_id]:
            message, caption = channel_queues[channel_id].pop(0)
            now  = asyncio.get_event_loop().time()
            last = last_edit_time.get(channel_id, 0)
            if now - last < EDIT_DELAY:
                await asyncio.sleep(EDIT_DELAY - (now - last))
            try:
                await message.edit_caption(caption, parse_mode=ParseMode.HTML)
                last_edit_time[channel_id] = asyncio.get_event_loop().time()
            except FloodWait as e:
                EDIT_DELAY = max(EDIT_DELAY, e.value / 10 + 1)
                await asyncio.sleep(e.value)
                try:
                    await message.edit_caption(caption, parse_mode=ParseMode.HTML)
                    last_edit_time[channel_id] = asyncio.get_event_loop().time()
                except Exception as err:
                    logger.error(f"Retry edit failed: {err}")
            except Exception as e:
                logger.error(f"Edit failed: {e}")


@app.on_message((filters.video | filters.document | filters.photo) & (filters.channel | filters.group) & filters.chat(ALLOWED_CHATS) & ~filters.service)
async def channel_handler(_, message):
    if caption_has_media_info(message.caption or ''):
        return
        
    caption, file_path = await process_message(app, message)
    logger.info(f"Generated live caption: {caption}")

    channel_id = message.chat.id
    channel_queues[channel_id].append((message, caption))
    asyncio.create_task(_process_channel_queue(channel_id))

    if file_path and os.path.exists(file_path):
        await aioremove(file_path)


@app.on_message(filters.private & (filters.video | filters.document))
async def private_handler(_, message):
    user_id = message.from_user.id
    if user_id in active_users:
        await message.reply_text("⚠️ Please wait until your current file is processed.")
        return
    active_users.add(user_id)
    asyncio.create_task(_handle_private(message))


async def _handle_private(message):
    file_path = None
    progress_msg = None
    user_id = message.from_user.id
    try:
        await asyncio.sleep(0.5)
        progress_msg = await message.reply_text("⏳ Processing…")
        caption, file_path = await process_message(app, message, progress_msg)
        try:
            await _safe_edit(progress_msg, caption, parse_mode=ParseMode.HTML)
        except MessageNotModified:
            pass
    except Exception as e:
        logger.error(f"Private handler error: {e}")
    finally:
        active_users.discard(user_id)
        if file_path and os.path.exists(file_path):
            await aioremove(file_path)


@app.on_message(filters.command("info") & filters.reply)
async def info_command(_, message):
    reply = message.reply_to_message
    if not reply or not (reply.video or reply.document or reply.photo):
        return await message.reply_text("⚠️ Reply to a video, photo or document.")

    media = reply.video or reply.document or reply.photo
    tmp   = os.path.join(TMP_DIR, f"info_{reply.id}_{uuid.uuid4().hex[:6]}.bin")
    try:
        ok = await _stream_chunk(app, media, 8 * 1024 * 1024, tmp)
        if not ok:
            download_path = os.path.join(TMP_DIR, getattr(media, 'file_name', f"info_{reply.id}.bin") or f"info_{reply.id}.bin")
            tmp2 = await reply.download(file_name=download_path)
            result = await _probe(tmp2)
            os.remove(tmp2)
        else:
            result = await _probe(tmp)

        caption = _build_caption(reply, media, result)
        await message.reply_text(caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Failed\n\n<code>{e}</code>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(tmp):
            await aioremove(tmp)


@app.on_message(filters.command("start") & filters.private)
async def start(_, m):
    await m.reply_text(
        "<b>🎬 Media Info Bot</b>\n\n"
        "Send me any video or file and I'll extract detailed media information.\n\n"
        "I provide:\n"
        "• 🎞 Video quality, codec &amp; bit depth\n"
        "• ⏳ Duration\n"
        "• 🔊 Audio languages\n"
        "• 💬 Subtitle info\n\n"
        "<b>⚡ Fast • Clean • Accurate</b>\n\n"
        "📌 <i>Note:</i> Send one file at a time.\n\n"
        "🤖 Bot by @piroxbots",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("server") & filters.user(ADMIN_ID))
async def server_cmd(_, m):
    await m.reply_text(
        f"CPU: {psutil.cpu_percent()}%\n"
        f"RAM: {psutil.virtual_memory().percent}%\n"
        f"Disk: {psutil.disk_usage('/').percent}%"
    )


@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, m):
    await m.reply_text("Restarting…")
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("shutdown") & filters.user(ADMIN_ID))
async def shutdown_cmd(_, m):
    await m.reply_text("Shutting down…")
    scheduler.shutdown(wait=False)
    await app.stop()
    if user_app:
        await user_app.stop()
    os._exit(0)


@app.on_message(filters.command("update") & filters.user(ADMIN_ID))
async def update_cmd(_, m):
    await m.reply_text("Updating…")
    try:
        os.system("git pull")
        os.system("pip install -r requirements.txt --no-cache-dir -q")
        await m.reply_text("✅ Updated. Restarting…")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await m.reply_text(f"Update failed: {e}")


# ── Scan past messages ───────────────────────────────────

_scan_active: dict[int, bool] = {}   # channel_id → running flag


@app.on_message(filters.command("scan") & filters.user(ADMIN_ID))
async def scan_cmd(_, m):
    """
    /scan <chat_id> [limit] [offset_id]
    Scans past messages in a channel/group and adds media info captions.
    limit defaults to 0 (all messages).
    offset_id defaults to 0 (start from newest).
    """
    parts = m.text.split()
    if len(parts) < 2:
        return await m.reply_text(
            "⚠️ Usage: <code>/scan &lt;chat_id&gt; [limit] [offset_id]</code>\n\n"
            "Example:\n"
            "<code>/scan -1001234567890</code>  — scan all\n"
            "<code>/scan -1001234567890 100</code>  — scan last 100 posts\n"
            "<code>/scan -1001234567890 0 1234</code> — scan starting from msg 1234",
            parse_mode=ParseMode.HTML,
        )

    try:
        chat_id = int(parts[1])
    except ValueError:
        return await m.reply_text("❌ Invalid chat ID.")

    limit = 0
    if len(parts) >= 3:
        try:
            limit = int(parts[2])
        except ValueError:
            return await m.reply_text("❌ Invalid limit number.")

    offset_id = 0
    if len(parts) >= 4:
        try:
            offset_id = int(parts[3])
        except ValueError:
            return await m.reply_text("❌ Invalid offset_id number.")

    if chat_id in _scan_active and _scan_active[chat_id]:
        return await m.reply_text("⚠️ A scan is already running for this chat.")

    _scan_active[chat_id] = True
    asyncio.create_task(_run_scan(m, chat_id, limit, offset_id))


@app.on_message(filters.command("stopscan") & filters.user(ADMIN_ID))
async def stopscan_cmd(_, m):
    """Stop a running scan. Usage: /stopscan <channel_id>"""
    parts = m.text.split()
    if len(parts) < 2:
        running = [str(cid) for cid, v in _scan_active.items() if v]
        if running:
            return await m.reply_text(
                f"⚠️ Usage: <code>/stopscan &lt;channel_id&gt;</code>\n\n"
                f"Active scans: {', '.join(running)}",
                parse_mode=ParseMode.HTML,
            )
        return await m.reply_text("ℹ️ No active scans.")

    try:
        chat_id = int(parts[1])
    except ValueError:
        return await m.reply_text("❌ Invalid channel ID.")

    if chat_id in _scan_active and _scan_active[chat_id]:
        _scan_active[chat_id] = False
        await m.reply_text("🛑 Scan will stop after the current file finishes.")
    else:
        await m.reply_text("ℹ️ No active scan for this channel.")


async def _run_scan(admin_msg, chat_id: int, limit: int, offset_id: int = 0):
    """Iterate chat history and add media info to uncaptioned videos."""
    # Determine which client to use: try Bot first, fallback to User
    history_client = app
    try:
        await app.get_chat(chat_id)
        # Try a tiny history fetch to verify access
        async for _ in app.get_chat_history(chat_id, limit=1):
            break
    except Exception:
        if user_app:
            history_client = user_app
            logger.info(f"Bot access failed for {chat_id}, falling back to User Helper")
        else:
            return await admin_msg.reply_text(
                "❌ <b>Access Denied</b>\n\n"
                "The Bot cannot access this chat history. "
                "Please configure a <code>STRING_SESSION</code> in your .env to use the User Helper fallback.",
                parse_mode=ParseMode.HTML
            )

    status = await admin_msg.reply_text(
        f"🔍 Starting scan of <code>{chat_id}</code> using "
        f"{'<b>User Helper</b>' if history_client == user_app else '<b>Bot Account</b>'}…",
        parse_mode=ParseMode.HTML
    )

    scanned = 0
    edited  = 0
    skipped = 0
    errors  = 0

    # Determine which client to use for history
    history_client = user_app if user_app else app
    
    try:
        async for message in history_client.get_chat_history(chat_id, limit=limit or None, offset_id=offset_id or 0):
            # Check for cancellation
            if not _scan_active.get(chat_id, False):
                await _safe_edit(status, f"🛑 Scan stopped.\n\n📊 Scanned: {scanned} | ✅ Edited: {edited} | ⏭ Skipped: {skipped} | ❌ Errors: {errors}")
                return

            # Skip non-media messages
            if not (message.video or message.document or message.photo):
                continue

            scanned += 1

            # Skip if already has media info
            if caption_has_media_info(message.caption or ''):
                skipped += 1
                logger.info(f"[{scanned}] Skipping msg {message.id} (already has info)")
                if scanned % 25 == 0:
                    await _safe_edit(
                        status,
                        f"🔍 Scanning… {scanned} checked | ✅ {edited} edited | ⏭ {skipped} skipped | ❌ {errors} errors",
                    )
                continue

            # Process the message
            try:
                logger.info(f"[{scanned}] Processing msg {message.id}...")
                # Use the client that fetched the message for processing and editing
                caption, file_path = await process_message(history_client, message)
                logger.info(f"[{scanned}] Generated scan caption:\n{caption}")

                try:
                    await history_client.edit_message_caption(chat_id, message.id, caption, parse_mode=ParseMode.HTML)
                    edited += 1
                    logger.info(f"[{scanned}] ✅ Edited msg {message.id}")
                except FloodWait as e:
                    logger.warning(f"Scan FloodWait: sleeping {e.value}s")
                    await asyncio.sleep(e.value)
                    await history_client.edit_message_caption(chat_id, message.id, caption, parse_mode=ParseMode.HTML)
                    edited += 1
                    logger.info(f"[{scanned}] ✅ Edited msg {message.id} (after FloodWait)")
                except MessageNotModified:
                    skipped += 1
                    logger.info(f"[{scanned}] ⏭ Msg {message.id} not modified")
                except Exception as e:
                    logger.error(f"[{scanned}] ❌ Edit failed msg {message.id}: {e}")
                    errors += 1

                if file_path and os.path.exists(file_path):
                    await aioremove(file_path)

                # Rate limit between edits
                await asyncio.sleep(EDIT_DELAY)

            except Exception as e:
                logger.error(f"Scan process error msg {message.id}: {e}")
                errors += 1

            # Progress update every 25 files
            if scanned % 25 == 0:
                await _safe_edit(
                    status,
                    f"🔍 Scanning… {scanned} checked | ✅ {edited} edited | ⏭ {skipped} skipped | ❌ {errors} errors",
                )

    except FloodWait as e:
        logger.warning(f"Scan history FloodWait: sleeping {e.value}s")
        await asyncio.sleep(e.value)
        # Don't restart — just report what we have so far
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        await _safe_edit(status, f"❌ Scan error: <code>{e}</code>\n\n📊 Scanned: {scanned} | ✅ Edited: {edited} | ⏭ Skipped: {skipped} | ❌ Errors: {errors}", parse_mode=ParseMode.HTML)
        return
    finally:
        _scan_active[chat_id] = False

    await _safe_edit(
        status,
        f"✅ Scan complete!\n\n📊 Scanned: {scanned} | ✅ Edited: {edited} | ⏭ Skipped: {skipped} | ❌ Errors: {errors}",
    )


def _install_deps():
    for binary, pkg in (("ffprobe", "ffmpeg"), ("mediainfo", "mediainfo")):
        r = subprocess.run(["which", binary], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if r.returncode != 0:
            logger.warning(f"❌ Missing dependency: {pkg}. Please install it manually (e.g., 'sudo pacman -S {pkg}' or 'sudo apt install {pkg}').")


async def main():
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    gc.set_threshold(*GC_THRESHOLD)
    _install_deps()

    await app.start()
    if user_app:
        await user_app.start()
        
    me = await app.get_me()
    logger.info(f"@{me.username} started")
    if user_app:
        u_me = await user_app.get_me()
        logger.info(f"User Helper started: @{u_me.username or u_me.id}")
    try:
        await app.send_message(ADMIN_ID, "🚀 Bot Started")
    except PeerIdInvalid:
        logger.warning(f"⚠️ Could not send startup message to ADMIN_ID {ADMIN_ID}. Have you started the bot yet?")
    except Exception as e:
        logger.warning(f"⚠️ Startup message failed: {e}")

    scheduler.add_job(gc.collect, "interval", minutes=20)
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    app.run(main())