import os
from dotenv import load_dotenv

load_dotenv()

def _need(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

TELEGRAM_BOT_TOKEN = _need("TELEGRAM_BOT_TOKEN")
TELEGRAM_FORUM_CHAT_ID = _need("TELEGRAM_FORUM_CHAT_ID")
TELEGRAM_MEETS_THREAD_ID = os.getenv("TELEGRAM_MEETS_THREAD_ID", "").strip()

GOOGLE_SHEET_URL = _need("GOOGLE_SHEET_URL")
GOOGLE_MANAGERS_SHEET = os.getenv("GOOGLE_MANAGERS_SHEET", "Managers")

GOOGLE_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID", "primary")
TZ = os.getenv("TZ", "Asia/Almaty")
