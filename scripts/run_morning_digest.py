from datetime import datetime
import pytz
import requests
import re
import json


from src.calendar.calendar_service import list_events_for_date
from src.config import (
    TZ,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_FORUM_CHAT_ID,
    TELEGRAM_MEETS_THREAD_ID,
)

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# --- patterns ---
USERNAME_RE = re.compile(r"@([a-zA-Z0-9_]{5,32})")
MANAGER_LINE_RE = re.compile(r"(?im)^\s*менеджер\s*:\s*(.+?)\s*$")


def tg_send_message(text: str, reply_markup: dict | None = None):
    payload = {
        "chat_id": TELEGRAM_FORUM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if TELEGRAM_MEETS_THREAD_ID and TELEGRAM_MEETS_THREAD_ID.isdigit() and int(TELEGRAM_MEETS_THREAD_ID) > 0:
        payload["message_thread_id"] = int(TELEGRAM_MEETS_THREAD_ID)

    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    r = requests.post(f"{TG_API}/sendMessage", data=payload, timeout=20)
    if r.status_code != 200:
        print("Telegram error:", r.status_code, r.text)
        r.raise_for_status()



def fmt_time(dt_str: str) -> str:
    # dt_str: "2026-01-30T10:00:00+05:00" or "2026-01-30"
    try:
        if "T" in dt_str:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return dt.strftime("%H:%M")
        return "Весь день"
    except:
        return "??:??"


def extract_manager(summary: str, description: str | None):
    """
    Returns tuple: (manager_key, pretty_title)
    manager_key used for grouping.
    pretty_title displayed in message.
    Priority:
      1) @username in summary
      2) 'Менеджер: Name' in description
      3) None
      4) Тут идет оьъяснение функциии как она работает
    """
    # 1) @username in summary
    m = USERNAME_RE.search(summary or "")
    if m:
        username = m.group(1)
        key = f"@{username}"
        title = f"@{username}"
        return key, title

    # 2) Менеджер: Name in description
    desc = description or ""
    m2 = MANAGER_LINE_RE.search(desc)
    if m2:
        name = m2.group(1).strip()
        if name:
            key = name
            title = name
            return key, title

    return None, None


def main():
    tz = pytz.timezone(TZ)
    today = datetime.now(tz).date()

    events = list_events_for_date(today)

    if not events:
        tg_send_message("📅 <b>Встречи на сегодня:</b>\n\n✅ На сегодня встреч нет.")
        return

    grouped = {}  # manager_key -> {"title": str, "items": [line]}
    no_manager = []  # list of lines

    for e in events:
        summary = e.get("summary", "Без названия")
        description = e.get("description", "")

        start = e.get("start", {})
        start_dt = start.get("dateTime") or start.get("date")
        t = fmt_time(start_dt) if start_dt else "??:??"

        line = f"• <b>{t}</b> — {summary}"

        key, title = extract_manager(summary, description)

        if key:
            if key not in grouped:
                grouped[key] = {"title": title, "items": []}
            grouped[key]["items"].append(line)
        else:
            no_manager.append(line)

    lines = ["📅 <b>Встречи на сегодня:</b>\n"]

    # менеджеры: сначала те, у кого @username, потом по имени
    def sort_key(k: str):
        return (0, k.lower()) if k.startswith("@") else (1, k.lower())

    for key in sorted(grouped.keys(), key=sort_key):
        block = grouped[key]
        lines.append(f"👤 <b>{block['title']}</b>")
        lines.extend(block["items"])
        lines.append("")  # пустая строка

    if no_manager:
        lines.append("⚠️ <b>Без назначенного менеджера</b>")
        lines.extend(no_manager)

    keyboard = {
        "inline_keyboard": [
            [{"text": "➕ Создать встречу", "callback_data": "meet:create"}]
        ]
    }
    tg_send_message("\n".join(lines).strip(), reply_markup=keyboard)


if __name__ == "__main__":
    main()
