# 🎬 MediaInfo Bot

> A Telegram bot that enriches captions with media details like resolution, codec, duration, audio languages, and subtitles. It supports channels, groups, topics, and private chats.

**Made by [@piroxbots](https://t.me/piroxbots) · Bug reports: [@notyourpiro](https://t.me/notyourpiro)**

## Features

- Progressive probing for videos: `16 KB` → `1 MB` → `3 MB` → `8 MB`
- Rich caption output with resolution, codec, bit depth, HDR, duration, audio, and subtitle labels
- Accurate resolution detection for vertical and ultrawide videos (uses shortest dimension)
- `/info` fallback to full download when partial probing is not enough
- Photo, video, document, animated WebP/GIF, and sticker support in private chats and channels
- Photo captions use `📸` with dimensions like `480x270` instead of video-style `240p`
- Empty audio/subtitle lines are omitted when no tracks are present
- Existing captions are preserved; filenames are appended for videos/documents when available
- Channel/group auto-editing for allowed chats only
- Smart re-processing prevention: emoji-based detection skips already-captioned media
- Gallery/album support with numbered file list including per-item size, resolution, and duration
- Admin commands for server status, restart, update, shutdown, and history scans
- Multi-admin support via `ADMIN_IDS` (comma-separated list)
- Optional user-helper account via `STRING_SESSION` for history scans in restricted channels
- Smart FloodWait handling: single warning log, global pause, no terminal spam
- Bot account used for all media streaming and edits; user account only for history access
- Config validation on startup with clear error messages

## Caption Output

Typical video caption:

```html
<b>Sample Movie</b>
movie.mkv
🎬 <b>1080p HEVC 10bit HDR</b> | ⏳ <b>01:45:32</b>
🔊 <b>English, Hindi</b>
💬 <b>English</b>
```

Album/gallery caption:

```
User caption text

1. clip1.mp4 (10.5 MiB | 🎬 1080p | ⏳ 00:00:18)
2. clip2.mp4 (5.0 MiB | 🎬 720p | ⏳ 00:00:10)
3. photo.jpg (690 KiB | 📸 1237x227)
```

Photo with no text caption:

```html
📸 <b>480x270</b>
```

Generic document:

```html
archive.zip
📄 <b>12.1 KiB</b>
```

## Requirements

- Python 3.10+
- `ffprobe` from [FFmpeg](https://ffmpeg.org/)
- `mediainfo`
- Telegram API credentials from [my.telegram.org](https://my.telegram.org)
- A bot token from [@BotFather](https://t.me/BotFather)
- The bot must be an admin in every target channel you want it to edit

The bot checks for missing system dependencies and logs a clear warning on startup. It does not try to install OS packages at runtime.

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/PIROXTG/MediaInfo-Bot.git
cd MediaInfo-Bot
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

Install system packages:

```bash
sudo apt install ffmpeg mediainfo
```

### 3. Configure `.env`

An example file is included as `.env.example`.

```env
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token

# Single admin (legacy)
ADMIN_ID=your_telegram_user_id

# Multiple admins (preferred)
ADMIN_IDS=111111111,222222222

ALLOWED_CHATS=-1001234567890,-1009876543210

# Primary user account session for scanning restricted channels
# STRING_SESSION=your_session_string

# Additional user accounts — bot tries each until one can edit the message
# Use this if messages in a channel were posted by different accounts
# (fixes MESSAGE_AUTHOR_REQUIRED errors)
# STRING_SESSION_2=second_session_string
# STRING_SESSION_3=third_session_string
```

Configuration reference:

| Variable | Description | Required |
|---|---|---|
| `API_ID` | Telegram API ID | Yes |
| `API_HASH` | Telegram API hash | Yes |
| `BOT_TOKEN` | Bot token | Yes |
| `ADMIN_IDS` | Comma-separated Telegram user IDs for admin commands | Yes* |
| `ADMIN_ID` | Legacy single admin ID (used if `ADMIN_IDS` is not set) | Yes* |
| `ALLOWED_CHATS` | Comma-separated chat IDs for auto-editing | No |
| `STRING_SESSION` | Primary user account session for history-scan fallback | No |
| `STRING_SESSION_2` … `STRING_SESSION_N` | Additional user accounts; tried in order when editing fails | No |
| `EDIT_DELAY` | Minimum seconds between caption edits (default: `1.5`) | No |
| `SCAN_WORKERS` | Parallel workers during `/scan` (default: `2`) | No |
| `LOG_LEVEL` | Logging level (default: `INFO`) | No |
| `LOG_FORMAT` | Python logging format string | No |
| `GC_THRESHOLD_0` | GC threshold gen 0 (default: `500`) | No |
| `GC_THRESHOLD_1` | GC threshold gen 1 (default: `5`) | No |
| `GC_THRESHOLD_2` | GC threshold gen 2 (default: `5`) | No |

*At least one of `ADMIN_IDS` or `ADMIN_ID` is required.

### 4. Run

```bash
python bot.py
```

On startup the bot validates config, checks for `ffprobe`/`mediainfo`, connects to Telegram, and starts scheduled garbage collection.

## Docker

```bash
docker build -t mediainfo-bot .
docker run -d --env-file .env mediainfo-bot
```

The Docker image installs both `ffmpeg` and `mediainfo`.

## Commands

### User Commands

| Command | Description |
|---|---|
| `/start` | Introduction and usage guide |
| `/info` | Reply to a video, photo, or document to analyse it inline |

### Admin Commands

| Command | Description |
|---|---|
| `/server` | Show CPU, RAM, and disk usage |
| `/restart` | Restart the bot process |
| `/update` | `git pull --ff-only`, install Python deps, then restart |
| `/shutdown` | Stop the bot |
| `/scan <chat_id_or_link> [limit] [offset_id]` | Scan older posts in a channel/group |
| `/stopscan <chat_id>` | Stop a running scan |

Scan examples:

```
/scan -1001234567890
/scan -1001234567890 100
/scan -1001234567890 0 54321
/scan https://t.me/c/1234567890/100
/scan https://t.me/c/1234567890/50/100
/stopscan -1001234567890
```

The `/scan` command accepts direct Telegram message links (including topic links). The scan starts from the linked message onwards. Scans run with `SCAN_WORKERS` parallel workers and use the bot account for all edits and media streaming.

## Notes

- If `ALLOWED_CHATS` is empty, auto-editing for channels/groups is disabled, but private chat features still work.
- History scans use the bot account first. If the bot lacks history access and `STRING_SESSION` is configured, it falls back to the user helper for `get_chat_history` only. All media streaming and caption edits still attempt the bot account first.
- **Multi-account editing**: If a message was posted by a specific user account (not the channel/bot), editing requires that account's session. Configure `STRING_SESSION_2`, `STRING_SESSION_3`, etc. The bot tries each account in order and caches the winning account per channel — so subsequent messages in the same channel go straight to the right account without retrying all of them.
- Generic documents get a lightweight size caption instead of media-track details.
- Photos do not invent a filename line if Telegram does not provide one.
- FloodWait events are handled gracefully: a single warning is logged, all workers pause until the cooldown expires, and the terminal status line is suppressed during the wait.
- Animated WebP files and stickers sent in channels are fully processed.
- Keep `SCAN_WORKERS` at `1`–`2` when scanning channels where the user account streams cross-DC files to avoid `auth.ExportAuthorization` FloodWait.

## Project Structure

```text
MediaInfo-Bot/
├── bot.py
├── config.py
├── gen_session.py
├── requirements.txt
├── Dockerfile
├── Procfile
└── .env.example
```

## Support

Found a bug or need help? Open an issue or reach out at **[@notyourpiro](https://t.me/notyourpiro)**.

**Bot channel: [@piroxbots](https://t.me/piroxbots)**
