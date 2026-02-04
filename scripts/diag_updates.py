import json
import time
import requests
from src.config import TELEGRAM_BOT_TOKEN

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

def tg(method, payload=None):
    r = requests.post(f"{TG_API}/{method}", data=payload or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    me = tg("getMe")
    print("BOT:", me)

    offset = 0
    print("Listening updates... send ANY message or press ANY button.")
    while True:
        resp = tg("getUpdates", {"timeout": 25, "offset": offset})
        updates = resp.get("result", [])
        for upd in updates:
            offset = upd["update_id"] + 1
            print("\n=== UPDATE ===")
            print(json.dumps(upd, ensure_ascii=False, indent=2))
        time.sleep(0.2)

if __name__ == "__main__":
    main()
