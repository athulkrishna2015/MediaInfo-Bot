import asyncio, os, tempfile
asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client

# Use a truly temporary file so Pyrogram never reuses a cached session
tmp = tempfile.mktemp(suffix=".session")
try:
    with Client(tmp, api_id=int(input("API_ID: ")), api_hash=input("API_HASH: ")) as app:
        print(app.export_session_string())
finally:
    for f in (tmp, tmp + "-journal", tmp + "-shm", tmp + "-wal"):
        try:
            os.remove(f)
        except FileNotFoundError:
            pass
