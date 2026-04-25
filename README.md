# 🎬 MediaInfo Bot

> A Telegram bot that enriches captions with media details like resolution, codec, duration, audio languages, and subtitles. It supports channels, groups, and private chats.

**Made by [@piroxbots](https://t.me/piroxbots) · Bug reports: [@notyourpiro](https://t.me/notyourpiro)**

## Features

- Progressive probing for videos: `16 KB` → `1 MB` → `3 MB` → `8 MB`
- Rich caption output with resolution, codec, bit depth, HDR, duration, audio, and subtitle labels
- `/info` fallback to full download when partial probing is not enough
- Photo, video, and document support in private chats
- Channel/group auto-editing for allowed chats only
- Admin commands for server status, restart, update, shutdown, and history scans
- Optional user-helper account via `STRING_SESSION` for history scans
- Config validation on startup with clear error messages

## Caption Template

The bot uses `CAPTION_TEMPLATE` with Python `str.format()` placeholders:

| Placeholder | Description |
|---|---|
| `{title}` | Original caption or filename |
| `{video_line}` | Resolution + codec details, for example `1080p HEVC 10bit HDR` |
| `{duration}` | Duration as `HH:MM:SS` |
| `{audio}` | Audio language list |
| `{subtitle}` | Subtitle list or `No Sub` |

Default output:

```html
<b>{title}</b>

🎬 <b>{video_line}</b> | ⏳ <b>{duration}</b>
🔊 <b>{audio}</b>
💬 <b>{subtitle}</b>
```

## Requirements

- Python 3.10+
- `ffprobe` from [FFmpeg](https://ffmpeg.org/)
- `mediainfo`
- Telegram API credentials from [my.telegram.org](https://my.telegram.org)
- A bot token from [@BotFather](https://t.me/BotFather)
- The bot must be an admin in every target channel you want it to edit

The bot now checks for missing system dependencies and logs a clear warning. It does not try to install OS packages at runtime.

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

Install system packages too:

```bash
sudo apt install ffmpeg mediainfo
```

### 3. Configure `.env`

An example file is included as `.env.example`.

```env
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
ADMIN_ID=your_telegram_user_id
ALLOWED_CHATS=-1001234567890,-1009876543210
# STRING_SESSION=optional_user_session
```

Configuration reference:

| Variable | Description | Required |
|---|---|---|
| `API_ID` | Telegram API ID | Yes |
| `API_HASH` | Telegram API hash | Yes |
| `BOT_TOKEN` | Bot token | Yes |
| `ADMIN_ID` | Telegram user ID allowed to run admin commands | Yes |
| `ALLOWED_CHATS` | Comma-separated chat IDs for auto-editing | No |
| `STRING_SESSION` | Optional user account session for history-scan fallback | No |
| `LOG_LEVEL` | Logging level | No |
| `LOG_FORMAT` | Python logging format | No |
| `GC_THRESHOLD_0` | GC threshold gen 0 | No |
| `GC_THRESHOLD_1` | GC threshold gen 1 | No |
| `GC_THRESHOLD_2` | GC threshold gen 2 | No |
| `CAPTION_TEMPLATE` | Custom HTML caption template | No |

### 4. Run

```bash
python bot.py
```

On startup the bot validates config, checks for `ffprobe`/`mediainfo`, connects to Telegram, sends a startup message to `ADMIN_ID`, and starts scheduled garbage collection.

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
| `/scan <chat_id> [limit] [offset_id]` | Scan older posts in a channel/group |
| `/stopscan <chat_id>` | Stop a running scan |

Scan examples:

- `/scan -1001234567890`
- `/scan -1001234567890 100`
- `/scan -1001234567890 0 54321`
- `/stopscan -1001234567890`

## Notes

- If `ALLOWED_CHATS` is empty, auto-editing for channels/groups is disabled, but private chat features still work.
- History scans use the bot account first. If that fails and `STRING_SESSION` is configured, the bot falls back to the user helper account.
- Generic documents get a lightweight size caption instead of media-track details.

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
