import asyncio

# Fix for Python 3.12+ / 3.14 RuntimeError: No current event loop
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import os
from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()

async def main():
    api_id = os.environ.get("API_ID")
    api_hash = os.environ.get("API_HASH")
    
    if not api_id or not api_hash:
        api_id = input("Enter API_ID: ")
        api_hash = input("Enter API_HASH: ")
    else:
        print(f"✅ Using API_ID: {api_id} from .env")
    
    async with Client(":memory:", api_id=int(api_id), api_hash=api_hash) as app:
        session_string = await app.export_session_string()
        
        # Save to .env
        env_path = ".env"
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()
            
            with open(env_path, "w") as f:
                found = False
                for line in lines:
                    if line.startswith("STRING_SESSION="):
                        f.write(f"STRING_SESSION={session_string}\n")
                        found = True
                    else:
                        f.write(line)
                if not found:
                    f.write(f"\nSTRING_SESSION={session_string}\n")
            print(f"✅ Saved STRING_SESSION to {env_path}")
        else:
            print(f"⚠️ {env_path} not found. Printing session instead:")
            
        print("\n" + "="*50)
        print("YOUR STRING_SESSION (Keep it secret!):")
        print("="*50)
        print(f"\n{session_string}\n")
        print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
