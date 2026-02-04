import re
import json
import time
import requests
from datetime import datetime, timedelta
import pytz

from src.sheets.managers_repo import append_meeting
from src.sheets.managers_repo import get_managers

from src.sheets.managers_repo import (
    get_meeting_by_event_id,
    update_meeting_by_event_id,
)

from src.calendar.calendar_service import (
    create_meeting_event,
    update_meeting_event,
    delete_event,
)

from src.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_FORUM_CHAT_ID,
    TELEGRAM_MEETS_THREAD_ID,
    TZ,
)

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# --- simple in-memory state: user_id -> dict ---
STATE: dict[int, dict] = {}

USERNAME_RE = re.compile(r"@([a-zA-Z0-9_]{5,32})")

# Включай для диагностики (потом выключи)
DEBUG_UPDATES = True


# -------------------- Telegram helpers --------------------
def tg_request(method: str, payload: dict):
    r = requests.post(f"{TG_API}/{method}", data=payload, timeout=30)
    if r.status_code != 200:
        print("Telegram error:", r.status_code, r.text)
        r.raise_for_status()
    return r.json()


def tg_send_message(text: str, reply_markup: dict | None = None, thread_id: int | str | None = None):
    payload = {
        "chat_id": TELEGRAM_FORUM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id is not None and str(thread_id).isdigit() and int(thread_id) > 0:
        payload["message_thread_id"] = int(thread_id)
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    return tg_request("sendMessage", payload)


def tg_send_message_to(chat_id: int, text: str, thread_id: int | None = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if thread_id is not None and int(thread_id) > 0:
        payload["message_thread_id"] = int(thread_id)
    return tg_request("sendMessage", payload)


def tg_answer_callback(callback_query_id: str, text: str = ""):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    return tg_request("answerCallbackQuery", payload)


# -------------------- Time helpers --------------------
def tz_now():
    return datetime.now(pytz.timezone(TZ))


def today_date_str():
    return tz_now().strftime("%d.%m.%Y")


def tomorrow_date_str():
    return (tz_now() + timedelta(days=1)).strftime("%d.%m.%Y")


# -------------------- Forum / identity helpers --------------------
def normalize_thread_id() -> str:
    return str(TELEGRAM_MEETS_THREAD_ID or "").strip()


def in_meets_thread(message: dict) -> bool:
    """
    True, если сообщение относится к нужной теме форума.
    Учитываем и message_thread_id, и reply_to_message.message_thread_id.
    """
    target = normalize_thread_id()
    if not target:
        return False

    tid = message.get("message_thread_id")
    if str(tid) == target:
        return True

    rt = message.get("reply_to_message") or {}
    rtid = rt.get("message_thread_id")
    if str(rtid) == target:
        return True

    return False


def resolve_user_id_from_message(message: dict) -> int | None:
    """
    В форумах при анонимном админстве сообщения приходят от GroupAnonymousBot.
    Тогда реальный автор часто лежит в reply_to_message.from (см. твой лог).
    """
    frm = message.get("from") or {}
    if frm.get("id") and not frm.get("is_bot"):
        return int(frm["id"])

    rt = message.get("reply_to_message") or {}
    rfrm = rt.get("from") or {}
    if rfrm.get("id") and not rfrm.get("is_bot"):
        return int(rfrm["id"])

    return None


def escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_dt_from_inputs(date_str: str, time_str: str) -> datetime:
    dd, mm, yyyy = date_str.split(".")
    hh, mi = time_str.split(":")
    return datetime(int(yyyy), int(mm), int(dd), int(hh), int(mi), 0)


# -------------------- Keyboards --------------------
def managers_keyboard():
    managers = get_managers()
    rows = []
    row = []
    for m in managers:
        name = (m.get("name") or "").strip() or "Manager"
        username = (m.get("username") or "").strip()

        if username and not username.startswith("@"):
            username = "@" + username
        if not username:
            username = f"NAME:{name}"

        telegram_id = (m.get("telegram_id") or "").strip() or "0"

        row.append({"text": name, "callback_data": f"meet:manager:{username}|{telegram_id}|{name}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([{"text": "⬅️ Назад", "callback_data": "meet:back:time"}])
    rows.append([{"text": "❌ Отмена", "callback_data": "meet:cancel"}])
    return {"inline_keyboard": rows}


def post_meeting_keyboard(event_id: str):
    """
    3 кнопки после создания встречи:
    1) создать новую
    2) изменить ЭТУ
    3) удалить ЭТУ
    """
    return {
        "inline_keyboard": [
            [
                {"text": "➕ Создать новую", "callback_data": "meet:new"},
                {"text": "✏️ Изменить", "callback_data": f"meet:edit:{event_id}"},
                {"text": "🗑 Удалить", "callback_data": f"meet:delete:{event_id}"},
            ]
        ]
    }


def post_deleted_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "➕ Создать новую", "callback_data": "meet:new"},
                {"text": "✏️ Изменить", "callback_data": "meet:deleted:edit"},
                {"text": "🗑 Удалить", "callback_data": "meet:deleted:delete"},
            ]
        ]
    }


def edit_fields_keyboard():
    # ДОБАВИЛИ: дату
    return {
        "inline_keyboard": [
            [{"text": "📅 Дата", "callback_data": "meet:editfield:date"}],
            [{"text": "⏰ Время", "callback_data": "meet:editfield:time"}],
            [{"text": "🧑 Клиент", "callback_data": "meet:editfield:client"}],
            [{"text": "📝 Комментарий", "callback_data": "meet:editfield:comment"}],
            [{"text": "❌ Отмена", "callback_data": "meet:cancel"}],
        ]
    }


# -------------------- FSM steps (messages) --------------------
def ask_client(user_id: int):
    STATE[user_id] = {"step": "client"}  # reset
    kb = {"inline_keyboard": [[{"text": "❌ Отмена", "callback_data": "meet:cancel"}]]}
    tg_send_message("🧑 <b>Клиент</b>\n\nНапиши название клиента одним сообщением.", reply_markup=kb, thread_id=TELEGRAM_MEETS_THREAD_ID)


def ask_date(user_id: int):
    STATE[user_id]["step"] = "date"
    kb = {
        "inline_keyboard": [
            [
                {"text": f"Сегодня ({today_date_str()})", "callback_data": "meet:date:today"},
                {"text": f"Завтра ({tomorrow_date_str()})", "callback_data": "meet:date:tomorrow"},
            ],
            [{"text": "📆 Ввести дату вручную", "callback_data": "meet:date:custom"}],
            [{"text": "⬅️ Назад", "callback_data": "meet:back:client"}, {"text": "❌ Отмена", "callback_data": "meet:cancel"}],
        ]
    }
    tg_send_message("📅 <b>Дата встречи</b>\n\nВыбери вариант:", reply_markup=kb, thread_id=TELEGRAM_MEETS_THREAD_ID)


def ask_custom_date(user_id: int):
    STATE[user_id]["step"] = "custom_date"
    kb = {"inline_keyboard": [[{"text": "⬅️ Назад", "callback_data": "meet:back:date"}, {"text": "❌ Отмена", "callback_data": "meet:cancel"}]]}
    tg_send_message(
        "📅 Введи дату в формате <code>ДД.ММ</code> или <code>ДД.ММ.ГГГГ</code>\n\nПример: <code>05.02</code>",
        reply_markup=kb,
        thread_id=TELEGRAM_MEETS_THREAD_ID,
    )


def ask_time(user_id: int):
    STATE[user_id]["step"] = "time"
    kb = {
        "inline_keyboard": [
            [{"text": "10:00", "callback_data": "meet:time:10:00"}, {"text": "11:00", "callback_data": "meet:time:11:00"}],
            [{"text": "12:00", "callback_data": "meet:time:12:00"}, {"text": "15:00", "callback_data": "meet:time:15:00"}],
            [{"text": "16:00", "callback_data": "meet:time:16:00"}, {"text": "Другое…", "callback_data": "meet:time:custom"}],
            [{"text": "⬅️ Назад", "callback_data": "meet:back:date"}, {"text": "❌ Отмена", "callback_data": "meet:cancel"}],
        ]
    }
    tg_send_message("⏰ <b>Время встречи</b>\n\nВыбери время:", reply_markup=kb, thread_id=TELEGRAM_MEETS_THREAD_ID)


def ask_custom_time(user_id: int):
    STATE[user_id]["step"] = "custom_time"
    kb = {"inline_keyboard": [[{"text": "⬅️ Назад", "callback_data": "meet:back:time"}, {"text": "❌ Отмена", "callback_data": "meet:cancel"}]]}
    tg_send_message("⏰ Введи время в формате <code>ЧЧ:ММ</code>\n\nПример: <code>15:30</code>", reply_markup=kb, thread_id=TELEGRAM_MEETS_THREAD_ID)


def ask_manager(user_id: int):
    STATE[user_id]["step"] = "manager"
    tg_send_message("👤 <b>Менеджер</b>\n\nВыбери менеджера:", reply_markup=managers_keyboard(), thread_id=TELEGRAM_MEETS_THREAD_ID)


def ask_comment(user_id: int):
    STATE[user_id]["step"] = "comment"
    kb = {
        "inline_keyboard": [
            [{"text": "Пропустить", "callback_data": "meet:comment:skip"}],
            [{"text": "⬅️ Назад", "callback_data": "meet:back:manager"}, {"text": "❌ Отмена", "callback_data": "meet:cancel"}],
        ]
    }
    tg_send_message("📝 <b>Комментарий</b>\n\nНапиши комментарий или нажми «Пропустить».", reply_markup=kb, thread_id=TELEGRAM_MEETS_THREAD_ID)


def show_confirm(user_id: int):
    data = STATE.get(user_id, {})
    client = data.get("client", "—")
    date_s = data.get("date", "—")
    time_s = data.get("time", "—")
    manager = data.get("manager_pretty", data.get("manager", "—"))
    comment = data.get("comment") or "—"

    text = (
        "📅 <b>Новая встреча</b>\n\n"
        f"🧑 Клиент: <b>{escape_html(client)}</b>\n"
        f"📅 Дата: <b>{escape_html(date_s)}</b>\n"
        f"⏰ Время: <b>{escape_html(time_s)}</b>\n"
        f"👤 Менеджер: <b>{escape_html(manager)}</b>\n"
        f"📝 Комментарий: <i>{escape_html(comment)}</i>\n\n"
        "Нажми ✅ чтобы создать."
    )
    kb = {
        "inline_keyboard": [
            [{"text": "✅ Создать", "callback_data": "meet:confirm:create"}],
            [{"text": "✏️ Изменить", "callback_data": "meet:confirm:edit"}],
            [{"text": "❌ Отменить", "callback_data": "meet:cancel"}],
        ]
    }
    STATE[user_id]["step"] = "confirm"
    tg_send_message(text, reply_markup=kb, thread_id=TELEGRAM_MEETS_THREAD_ID)


# -------------------- Parsers --------------------
def parse_date_input(s: str) -> str | None:
    s = (s or "").strip()
    m = re.match(r"^(\d{2})\.(\d{2})(?:\.(\d{4}))?$", s)
    if not m:
        return None
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    if yyyy is None:
        yyyy = str(tz_now().year)

    try:
        datetime(int(yyyy), int(mm), int(dd))
    except Exception:
        return None
    return f"{dd}.{mm}.{yyyy}"


def parse_time_input(s: str) -> str | None:
    s = (s or "").strip()
    m = re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", s)
    if not m:
        return None
    return s


# -------------------- Callback handler --------------------
def handle_callback(callback: dict):
    cq_id = callback.get("id")
    data = callback.get("data", "")
    user_id = callback.get("from", {}).get("id")

    if cq_id:
        tg_answer_callback(cq_id)

    if not user_id:
        return

    user_id = int(user_id)

    # 1) создать новую встречу (кнопка после создания)
    if data == "meet:new":
        ask_client(user_id)
        return

    if data == "meet:deleted:edit" or data == "meet:deleted:delete":
        tg_send_message("ℹ️ Эта встреча уже удалена. Нажми «➕ Создать новую».", thread_id=TELEGRAM_MEETS_THREAD_ID)
        return

    if data == "meet:create":
        ask_client(user_id)
        return

    if data == "meet:cancel":
        STATE.pop(user_id, None)
        tg_send_message("❌ Действие отменено.", thread_id=TELEGRAM_MEETS_THREAD_ID)
        return

    # 2) редактирование конкретной встречи по event_id
    if data.startswith("meet:edit:"):
        event_id = data.split(":", 2)[2].strip()
        if not event_id:
            tg_send_message("⚠️ Не вижу event_id для редактирования.", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        meeting = None
        try:
            meeting = get_meeting_by_event_id(event_id)
        except Exception as e:
            tg_send_message(f"⚠️ Ошибка чтения встречи из таблицы:\n<code>{escape_html(str(e))}</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        if not meeting:
            tg_send_message("⚠️ Не нашёл эту встречу в таблице (Meetings).", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        STATE[user_id] = {
            "step": "edit_menu",
            "edit_event_id": event_id,
        }

        client = (meeting.get("client") or "").strip()
        date_s = (meeting.get("date") or "").strip()
        time_s = (meeting.get("time") or "").strip()

        tg_send_message(
            "✏️ <b>Редактирование встречи</b>\n\n"
            f"🧑 Клиент: <b>{escape_html(client)}</b>\n"
            f"📅 Дата: <b>{escape_html(date_s)}</b>\n"
            f"⏰ Время: <b>{escape_html(time_s)}</b>\n"
            f"🆔 <code>{escape_html(event_id)}</code>\n\n"
            "Что меняем?",
            reply_markup=edit_fields_keyboard(),
            thread_id=TELEGRAM_MEETS_THREAD_ID,
        )
        return

    # 3) удаление конкретной встречи по event_id
    if data.startswith("meet:delete:"):
        event_id = data.split(":", 2)[2].strip()
        if not event_id:
            tg_send_message("⚠️ Не вижу event_id для удаления.", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        # удаляем в календаре
        try:
            delete_event(event_id)
        except Exception as e:
            tg_send_message(
                f"❌ Ошибка удаления встречи из календаря:\n<code>{escape_html(str(e))}</code>",
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return

        # помечаем в таблице
        try:
            update_meeting_by_event_id(event_id, {"status": "canceled"})
        except Exception as e:
            tg_send_message(
                "⚠️ Встреча удалена из календаря, но не смог обновить статус в таблице.\n"
                f"<code>{escape_html(str(e))}</code>",
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )

        # сбрасываем edit-сессию если вдруг редактировали её же
        st = STATE.get(user_id) or {}
        if st.get("edit_event_id") == event_id:
            STATE.pop(user_id, None)

        kb = {
            "inline_keyboard": [
                [{"text": "➕ Создать новую встречу", "callback_data": "meet:new"}]
            ]
        }

        tg_send_message(
            "🗑 <b>Встреча удалена</b>\n\nМожешь сразу создать новую встречу 👇",
            reply_markup=kb,
            thread_id=TELEGRAM_MEETS_THREAD_ID,
        )
        return

    # выбор поля для редактирования
    if data.startswith("meet:editfield:"):
        field = data.split(":", 2)[2].strip()
        st = STATE.get(user_id) or {}
        event_id = (st.get("edit_event_id") or "").strip()
        if not event_id:
            tg_send_message("⚠️ Сессия редактирования устарела. Нажми «Изменить» на нужной встрече ещё раз.", thread_id=TELEGRAM_MEETS_THREAD_ID)
            STATE.pop(user_id, None)
            return

        # ДОБАВИЛИ: редактирование даты
        if field == "date":
            STATE[user_id]["step"] = "edit_date"
            tg_send_message(
                "📅 Введи новую дату в формате <code>ДД.ММ</code> или <code>ДД.ММ.ГГГГ</code>\nПример: <code>05.02</code>",
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return

        if field == "time":
            STATE[user_id]["step"] = "edit_time"
            tg_send_message("⏰ Введи новое время в формате <code>ЧЧ:ММ</code>\nПример: <code>15:30</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        if field == "client":
            STATE[user_id]["step"] = "edit_client"
            tg_send_message("🧑 Введи новое название клиента одним сообщением.", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        if field == "comment":
            STATE[user_id]["step"] = "edit_comment"
            tg_send_message("📝 Введи новый комментарий (если удалить — отправь <code>-</code>)", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        return

    # --- исходный create-flow ---
    if data.startswith("meet:back:"):
        step = data.split(":", 2)[2]
        if step == "client":
            ask_client(user_id)
        elif step == "date":
            ask_date(user_id)
        elif step == "time":
            ask_time(user_id)
        elif step == "manager":
            ask_manager(user_id)
        return

    if data.startswith("meet:date:"):
        choice = data.split(":", 2)[2]
        if user_id not in STATE:
            ask_client(user_id)
            return
        if choice == "today":
            STATE[user_id]["date"] = today_date_str()
            ask_time(user_id)
        elif choice == "tomorrow":
            STATE[user_id]["date"] = tomorrow_date_str()
            ask_time(user_id)
        elif choice == "custom":
            ask_custom_date(user_id)
        return

    if data.startswith("meet:time:"):
        choice = data.split(":", 2)[2]
        if user_id not in STATE:
            ask_client(user_id)
            return
        if choice == "custom":
            ask_custom_time(user_id)
        else:
            STATE[user_id]["time"] = choice
            ask_manager(user_id)
        return

    if data.startswith("meet:manager:"):
        raw = data.split(":", 2)[2]
        if user_id not in STATE:
            ask_client(user_id)
            return

        parts = raw.split("|")
        manager_token = parts[0] if len(parts) >= 1 else ""
        telegram_id = parts[1] if len(parts) >= 2 else "0"
        manager_name = parts[2] if len(parts) >= 3 else manager_token.replace("NAME:", "")

        manager_token = (manager_token or "").strip()
        manager_name = (manager_name or "").strip()
        telegram_id = (telegram_id or "0").strip()

        manager_pretty = manager_name
        if manager_token.startswith("@"):
            manager_pretty = manager_token
        elif manager_token.startswith("NAME:"):
            manager_pretty = manager_name

        STATE[user_id]["manager"] = manager_token
        STATE[user_id]["manager_pretty"] = manager_pretty
        STATE[user_id]["manager_name"] = manager_name
        STATE[user_id]["manager_id"] = telegram_id

        ask_comment(user_id)
        return

    if data.startswith("meet:comment:"):
        choice = data.split(":", 2)[2]
        if user_id not in STATE:
            ask_client(user_id)
            return
        if choice == "skip":
            STATE[user_id]["comment"] = ""
            show_confirm(user_id)
        return

    if data.startswith("meet:confirm:"):
        action = data.split(":", 2)[2]
        if user_id not in STATE:
            ask_client(user_id)
            return

        if action == "edit":
            # редактирование ДО создания (предпросмотр) — оставляем как возврат к клиенту
            ask_client(user_id)
            return

        if action == "create":
            d = STATE.get(user_id, {})

            client = (d.get("client") or "").strip()
            date_s = (d.get("date") or "").strip()
            time_s = (d.get("time") or "").strip()
            comment = (d.get("comment") or "").strip()

            manager_id = (d.get("manager_id") or "").strip()
            manager_name = (d.get("manager_name") or "").strip()
            manager_pretty = (d.get("manager_pretty") or manager_name or "").strip()

            if not (client and date_s and time_s and manager_id and manager_name):
                tg_send_message("⚠️ Не хватает данных для создания встречи. Заполни заново.", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            start_dt = build_dt_from_inputs(date_s, time_s)
            end_dt = start_dt + timedelta(minutes=60)

            # title for calendar
            if manager_pretty.startswith("@"):
                client_for_title = f"{client} — {manager_pretty}"
            else:
                client_for_title = f"{client} — {manager_name}"

            pretty_prefix = f"Менеджер: {manager_name}"
            if manager_pretty.startswith("@"):
                pretty_prefix += f" ({manager_pretty})"
            pretty_comment = f"{pretty_prefix}\n{comment}" if comment else pretty_prefix

            try:
                event_id = create_meeting_event(
                    client=client_for_title,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    manager_id=int(manager_id) if str(manager_id).isdigit() else 0,
                    manager_name=manager_name,
                    comment=pretty_comment,
                )
            except Exception as e:
                tg_send_message(
                    f"❌ Ошибка создания события в календаре:\n<code>{escape_html(str(e))}</code>",
                    thread_id=TELEGRAM_MEETS_THREAD_ID,
                )
                return

            # ---- Save to Google Sheet (Meetings) ----
            try:
                append_meeting(
                    created_by_id=user_id,
                    created_by_username="",  # позже улучшим (можно вытянуть из update)
                    chat_id=int(TELEGRAM_FORUM_CHAT_ID),
                    thread_id=int(TELEGRAM_MEETS_THREAD_ID) if str(TELEGRAM_MEETS_THREAD_ID).isdigit() else None,
                    client=client,  # оригинальное имя клиента (без приписки менеджера)
                    date=date_s,
                    time=time_s,
                    start_iso=start_dt.isoformat(),
                    end_iso=end_dt.isoformat(),
                    manager_name=manager_name,
                    manager_username=manager_pretty if manager_pretty.startswith("@") else "",
                    manager_telegram_id=int(manager_id) if str(manager_id).isdigit() else 0,
                    comment=pretty_comment,
                    event_id=event_id,
                    status="created",
                )
            except Exception as e:
                tg_send_message(
                    "⚠️ Встреча создана в календаре, но не смог записать в таблицу.\n"
                    f"<code>{escape_html(str(e))}</code>",
                    thread_id=TELEGRAM_MEETS_THREAD_ID,
                )

            STATE.pop(user_id, None)

            tg_send_message(
                "✅ <b>Встреча создана</b>\n\n"
                f"🧑 Клиент: <b>{escape_html(client)}</b>\n"
                f"📅 {escape_html(date_s)} ⏰ {escape_html(time_s)}\n"
                f"👤 Менеджер: <b>{escape_html(manager_name)}</b> {escape_html(manager_pretty) if manager_pretty.startswith('@') else ''}\n"
                f"🆔 Event ID: <code>{escape_html(event_id)}</code>",
                reply_markup=post_meeting_keyboard(event_id),
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return


# -------------------- Message handler --------------------
def handle_message(message: dict):
    text = (message.get("text") or "").strip()

    # 1) личка — разрешаем /meet для теста
    chat = message.get("chat") or {}
    chat_type = chat.get("type")
    if chat_type == "private":
        user_id = resolve_user_id_from_message(message) or (message.get("from") or {}).get("id")
        if not user_id:
            return
        user_id = int(user_id)

        if text.lower() in ("/meet", "создать встречу", "+ встреча"):
            ask_client(user_id)
            return

        st = STATE.get(user_id)
        if not st:
            return

        return _handle_fsm_text(user_id, text)

    # 2) группа/форум: только нужная тема
    if not in_meets_thread(message):
        return

    user_id = resolve_user_id_from_message(message)
    if not user_id:
        if text:
            tg_send_message(
                "⚠️ Не вижу автора сообщения (анонимность/от имени группы). "
                "Отключи анонимность у админа или отвечай reply на сообщение бота.",
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
        return

    if not text:
        return

    # allow manual start
    if text.lower() in ("/meet", "создать встречу", "+ встреча"):
        ask_client(user_id)
        return

    return _handle_fsm_text(user_id, text)


def _handle_fsm_text(user_id: int, text: str):
    st = STATE.get(user_id)
    if not st:
        return

    step = st.get("step")

    # --------- CREATE FLOW ---------
    if step == "client":
        STATE[user_id]["client"] = text
        ask_date(user_id)
        return

    if step == "custom_date":
        parsed = parse_date_input(text)
        if not parsed:
            tg_send_message("⚠️ Неверный формат даты. Пример: <code>05.02</code> или <code>05.02.2026</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return
        STATE[user_id]["date"] = parsed
        ask_time(user_id)
        return

    if step == "custom_time":
        parsed = parse_time_input(text)
        if not parsed:
            tg_send_message("⚠️ Неверный формат времени. Пример: <code>15:30</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return
        STATE[user_id]["time"] = parsed
        ask_manager(user_id)
        return

    if step == "comment":
        STATE[user_id]["comment"] = text
        show_confirm(user_id)
        return

    # --------- EDIT FLOW (by event_id) ---------
    if step in ("edit_date", "edit_time", "edit_client", "edit_comment"):
        event_id = (st.get("edit_event_id") or "").strip()
        if not event_id:
            tg_send_message("⚠️ Сессия редактирования устарела. Нажми «Изменить» на встрече ещё раз.", thread_id=TELEGRAM_MEETS_THREAD_ID)
            STATE.pop(user_id, None)
            return

        meeting = None
        try:
            meeting = get_meeting_by_event_id(event_id)
        except Exception as e:
            tg_send_message(f"⚠️ Ошибка чтения встречи из таблицы:\n<code>{escape_html(str(e))}</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
            return

        if not meeting:
            tg_send_message("⚠️ Не нашёл эту встречу в таблице (Meetings).", thread_id=TELEGRAM_MEETS_THREAD_ID)
            STATE.pop(user_id, None)
            return

        date_s = (meeting.get("date") or "").strip()
        old_time = (meeting.get("time") or "").strip()
        old_client = (meeting.get("client") or "").strip()
        old_comment = (meeting.get("comment") or "").strip()

        manager_name = (meeting.get("manager_name") or "").strip()
        manager_username = (meeting.get("manager_username") or "").strip()  # может быть "@xxx"
        manager_pretty = manager_username if manager_username.startswith("@") else manager_name

        # --- НОВОЕ: edit_date ---
        if step == "edit_date":
            parsed_date = parse_date_input(text)
            if not parsed_date:
                tg_send_message("⚠️ Неверный формат даты. Пример: <code>05.02</code> или <code>05.02.2026</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            # сохраняем время как было
            if not old_time:
                tg_send_message("⚠️ Не вижу текущее время встречи в таблице, не могу пересобрать дату/время.", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            start_dt = build_dt_from_inputs(parsed_date, old_time)
            end_dt = start_dt + timedelta(minutes=60)

            try:
                update_meeting_event(event_id=event_id, start_dt=start_dt, end_dt=end_dt)
            except Exception as e:
                tg_send_message(f"❌ Ошибка обновления даты в календаре:\n<code>{escape_html(str(e))}</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            try:
                update_meeting_by_event_id(event_id, {
                    "date": parsed_date,
                    "start_iso": start_dt.isoformat(),
                    "end_iso": end_dt.isoformat(),
                    "status": "created",
                })
            except Exception as e:
                tg_send_message(
                    "⚠️ В календаре обновил, но не смог обновить строку в таблице.\n"
                    f"<code>{escape_html(str(e))}</code>",
                    thread_id=TELEGRAM_MEETS_THREAD_ID,
                )

            STATE.pop(user_id, None)

            tg_send_message(
                "✅ <b>Встреча обновлена</b>\n\n"
                f"🧑 Клиент: <b>{escape_html(old_client)}</b>\n"
                f"📅 <b>{escape_html(parsed_date)}</b> ⏰ {escape_html(old_time)}\n"
                f"👤 Менеджер: <b>{escape_html(manager_name)}</b> {escape_html(manager_pretty) if manager_pretty.startswith('@') else ''}\n"
                f"🆔 Event ID: <code>{escape_html(event_id)}</code>",
                reply_markup=post_meeting_keyboard(event_id),
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return

        if step == "edit_time":
            parsed = parse_time_input(text)
            if not parsed:
                tg_send_message("⚠️ Неверный формат времени. Пример: <code>15:30</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            start_dt = build_dt_from_inputs(date_s, parsed)
            end_dt = start_dt + timedelta(minutes=60)

            try:
                update_meeting_event(event_id=event_id, start_dt=start_dt, end_dt=end_dt)
            except Exception as e:
                tg_send_message(f"❌ Ошибка обновления в календаре:\n<code>{escape_html(str(e))}</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            try:
                update_meeting_by_event_id(event_id, {
                    "time": parsed,
                    "start_iso": start_dt.isoformat(),
                    "end_iso": end_dt.isoformat(),
                    "status": "created",
                })
            except Exception as e:
                tg_send_message(
                    "⚠️ В календаре обновил, но не смог обновить строку в таблице.\n"
                    f"<code>{escape_html(str(e))}</code>",
                    thread_id=TELEGRAM_MEETS_THREAD_ID,
                )

            STATE.pop(user_id, None)

            tg_send_message(
                "✅ <b>Встреча обновлена</b>\n\n"
                f"🧑 Клиент: <b>{escape_html(old_client)}</b>\n"
                f"📅 {escape_html(date_s)} ⏰ <b>{escape_html(parsed)}</b>\n"
                f"👤 Менеджер: <b>{escape_html(manager_name)}</b> {escape_html(manager_pretty) if manager_pretty.startswith('@') else ''}\n"
                f"🆔 Event ID: <code>{escape_html(event_id)}</code>",
                reply_markup=post_meeting_keyboard(event_id),
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return

        if step == "edit_client":
            new_client = text.strip()
            if not new_client:
                tg_send_message("⚠️ Клиент не может быть пустым.", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            # обновляем calendar summary + client внутри description
            try:
                update_meeting_event(event_id=event_id, client=new_client)
            except Exception as e:
                tg_send_message(f"❌ Ошибка обновления клиента в календаре:\n<code>{escape_html(str(e))}</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            try:
                update_meeting_by_event_id(event_id, {"client": new_client})
            except Exception as e:
                tg_send_message(
                    "⚠️ В календаре обновил, но не смог обновить строку в таблице.\n"
                    f"<code>{escape_html(str(e))}</code>",
                    thread_id=TELEGRAM_MEETS_THREAD_ID,
                )

            STATE.pop(user_id, None)

            tg_send_message(
                "✅ <b>Встреча обновлена</b>\n\n"
                f"🧑 Клиент: <b>{escape_html(new_client)}</b>\n"
                f"📅 {escape_html(date_s)} ⏰ <b>{escape_html(old_time)}</b>\n"
                f"👤 Менеджер: <b>{escape_html(manager_name)}</b> {escape_html(manager_pretty) if manager_pretty.startswith('@') else ''}\n"
                f"🆔 Event ID: <code>{escape_html(event_id)}</code>",
                reply_markup=post_meeting_keyboard(event_id),
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return

        if step == "edit_comment":
            new_comment = text.strip()
            if new_comment == "-":
                new_comment = ""

            try:
                update_meeting_event(event_id=event_id, comment=new_comment)
            except Exception as e:
                tg_send_message(f"❌ Ошибка обновления комментария в календаре:\n<code>{escape_html(str(e))}</code>", thread_id=TELEGRAM_MEETS_THREAD_ID)
                return

            try:
                update_meeting_by_event_id(event_id, {"comment": new_comment})
            except Exception as e:
                tg_send_message(
                    "⚠️ В календаре обновил, но не смог обновить строку в таблице.\n"
                    f"<code>{escape_html(str(e))}</code>",
                    thread_id=TELEGRAM_MEETS_THREAD_ID,
                )

            STATE.pop(user_id, None)

            tg_send_message(
                "✅ <b>Встреча обновлена</b>\n\n"
                f"🧑 Клиент: <b>{escape_html(old_client)}</b>\n"
                f"📅 {escape_html(date_s)} ⏰ <b>{escape_html(old_time)}</b>\n"
                f"👤 Менеджер: <b>{escape_html(manager_name)}</b> {escape_html(manager_pretty) if manager_pretty.startswith('@') else ''}\n"
                f"📝 Комментарий: <i>{escape_html(new_comment) if new_comment else '—'}</i>\n"
                f"🆔 Event ID: <code>{escape_html(event_id)}</code>",
                reply_markup=post_meeting_keyboard(event_id),
                thread_id=TELEGRAM_MEETS_THREAD_ID,
            )
            return


# -------------------- Polling loop --------------------
def poll_updates():
    offset = 0
    print("Qeepe Meets bot polling started...")

    now = tz_now().strftime("%Y-%m-%d %H:%M:%S")
    managers = get_managers()
    tg_send_message(
        f"✅ <b>Qeepe Meets</b> запущен\n"
        f"🕒 Время: <code>{now}</code>\n"
        f"📌 Thread: <code>{TELEGRAM_MEETS_THREAD_ID or 'NO_THREAD'}</code>\n",
        # f"👥 Managers loaded: <code>{len(managers)}</code>",
        thread_id=TELEGRAM_MEETS_THREAD_ID,
    )

    while True:
        try:
            resp = tg_request("getUpdates", {"timeout": 30, "offset": offset})
            updates = resp.get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1

                if DEBUG_UPDATES:
                    print(json.dumps(upd, ensure_ascii=False, indent=2))

                if "callback_query" in upd:
                    handle_callback(upd["callback_query"])
                elif "message" in upd:
                    handle_message(upd["message"])
                elif "edited_message" in upd:
                    handle_message(upd["edited_message"])

        except Exception as e:
            print("Poll error:", repr(e))
            time.sleep(2)


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    if TELEGRAM_FORUM_CHAT_ID is None:
        raise RuntimeError("Missing TELEGRAM_FORUM_CHAT_ID")
    if not TELEGRAM_MEETS_THREAD_ID:
        raise RuntimeError("Missing TELEGRAM_MEETS_THREAD_ID")

    poll_updates()


if __name__ == "__main__":
    main()
