# scripts/run_daily.py
from __future__ import annotations

import sys
import os
from datetime import datetime, date
import requests
import pytz

# --- fix imports when running directly ---
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.calendar.calendar_service import list_events_for_date
from src.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_FORUM_CHAT_ID,
    TELEGRAM_MEETS_THREAD_ID,
    TZ,
)

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


# -------------------- Telegram helpers --------------------
def _today_local_date() -> date:
    try:
        tz = pytz.timezone(TZ)
    except Exception:
        tz = pytz.timezone("Asia/Almaty")
    return datetime.now(tz).date()

def tg_request(method: str, payload: dict):
    r = requests.post(f"{TG_API}/{method}", data=payload, timeout=30)
    if r.status_code != 200:
        print("Telegram error:", r.status_code, r.text)
        r.raise_for_status()
    return r.json()


def tg_send_message(text: str, thread_id: int | str | None = None, reply_markup: dict | None = None):
    payload = {
        "chat_id": TELEGRAM_FORUM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id is not None and str(thread_id).isdigit() and int(thread_id) > 0:
        payload["message_thread_id"] = int(thread_id)
    if reply_markup:
        payload["reply_markup"] = __import__("json").dumps(reply_markup, ensure_ascii=False)
    return tg_request("sendMessage", payload)


def escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# -------------------- Calendar parsing helpers --------------------
def _extract_from_description(description: str, key: str) -> str:
    if not description:
        return ""
    for line in description.splitlines():
        if line.lower().startswith(key.lower() + ":"):
            return line.split(":", 1)[1].strip()
    return ""


def _event_time_local(event: dict) -> str:
    tz = pytz.timezone(TZ)
    start = (event.get("start") or {})
    dt_s = start.get("dateTime")
    if not dt_s:
        return "—"

    dt = datetime.fromisoformat(dt_s)
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    else:
        dt = dt.astimezone(tz)

    return dt.strftime("%H:%M")


def _today_local_date() -> date:
    tz = pytz.timezone(TZ)
    return datetime.now(tz).date()


def _meeting_keyboard(event_id: str) -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "✏️ Изменить", "callback_data": f"meet:edit:{event_id}"},
                {"text": "🗑 Удалить", "callback_data": f"meet:delete:{event_id}"},
            ]
        ]
    }


# -------------------- Report builder --------------------
def build_cards_for_today() -> list[dict]:
    """
    Возвращает список карточек:
    [{ "text": "...", "event_id": "..." }, ...]
    """
    day = _today_local_date()
    date_str = day.strftime("%d.%m.%Y")

    events = list_events_for_date(day)

    ours = []
    for e in events:
        if e.get("status") == "cancelled":
            continue
        desc = e.get("description") or ""
        if "source: qeepe_meets" not in desc:
            continue
        ours.append(e)

    if not ours:
        # одна карточка без кнопок
        return [{"text": f"☀️ <b>Встречи на сегодня</b> — <code>{date_str}</code>\n\nНет встреч ✅", "event_id": ""}]

    ours.sort(key=lambda ev: (ev.get("start") or {}).get("dateTime") or "")

    cards = []
    header = f"☀️ <b>Встречи на сегодня</b> — <code>{date_str}</code>\n\n"

    for i, e in enumerate(ours, 1):
        event_id = (e.get("id") or "").strip()

        summary = (e.get("summary") or "Встреча").strip()
        if summary.lower().startswith("встреча:"):
            summary = summary.split(":", 1)[1].strip()

        desc = e.get("description") or ""
        time_s = _event_time_local(e)

        manager_name = _extract_from_description(desc, "manager_name")
        manager_username = _extract_from_description(desc, "manager_username")
        comment = _extract_from_description(desc, "comment")

        # Заголовок встречи
        text = f"📌 <b>{escape_html(time_s)}</b> — {escape_html(summary)}"
        if manager_username:
            text += f" — <b>{escape_html(manager_username)}</b>"

        # Дальше строки
        if manager_name:
            text += f"\n👤 {escape_html(manager_name)}"
        if comment:
            text += f"\n📝 {escape_html(comment)}"
        if event_id:
            text += f"\n🆔 <code>{escape_html(event_id)}</code>"

        # только первая карточка получает общий хедер
        if i == 1:
            text = header + text

        cards.append({"text": text, "event_id": event_id})

    return cards


# -------------------- Entrypoint --------------------
def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    if TELEGRAM_FORUM_CHAT_ID is None:
        raise RuntimeError("Missing TELEGRAM_FORUM_CHAT_ID")
    if not TELEGRAM_MEETS_THREAD_ID:
        raise RuntimeError("Missing TELEGRAM_MEETS_THREAD_ID")

    cards = build_cards_for_today()

    for c in cards:
        event_id = c.get("event_id") or ""
        kb = _meeting_keyboard(event_id) if event_id else None
        tg_send_message(c["text"], thread_id=TELEGRAM_MEETS_THREAD_ID, reply_markup=kb)

    print("OK: daily report sent (cards only)")


if __name__ == "__main__":
    main()
