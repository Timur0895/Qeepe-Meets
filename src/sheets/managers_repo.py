import re
from datetime import datetime
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from src.config import GOOGLE_SHEET_URL, TZ

# ----------------------------
# Internal helpers / caching
# ----------------------------

_SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_GC = None
_SHEET = None


def _norm(s: str) -> str:
    # normalize header keys: remove spaces, lowercase
    return re.sub(r"\s+", "", (s or "").strip().lower())


def _get_sheet():
    """
    Cached access to spreadsheet via service account.
    Avoids re-auth on every call.
    """
    global _GC, _SHEET
    if _GC is None or _SHEET is None:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", _SCOPE)
        _GC = gspread.authorize(creds)
        _SHEET = _GC.open_by_url(GOOGLE_SHEET_URL)
    return _SHEET


def _now_str() -> str:
    return datetime.now(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M:%S")


# ----------------------------
# Managers (existing logic)
# ----------------------------

def get_managers():
    sheet = _get_sheet()
    ws = sheet.worksheet("Managers")

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers_raw = values[0]
    headers = [_norm(h) for h in headers_raw]

    idx = {"telegram_id": None, "name": None, "username": None}

    for key in ["telegram_id", "telegramid", "tgid", "id"]:
        if key in headers:
            idx["telegram_id"] = headers.index(key)
            break

    for key in ["name", "manager", "fullname"]:
        if key in headers:
            idx["name"] = headers.index(key)
            break

    for key in ["username", "tgusername", "telegramusername", "@username", "user"]:
        if key in headers:
            idx["username"] = headers.index(key)
            break

    if idx["telegram_id"] is None or idx["name"] is None:
        raise RuntimeError("Лист Managers должен содержать колонки telegram_id и name")

    managers = []
    for row in values[1:]:
        telegram_id = row[idx["telegram_id"]] if idx["telegram_id"] < len(row) else ""
        name = row[idx["name"]] if idx["name"] < len(row) else ""

        username = ""
        if idx["username"] is not None and idx["username"] < len(row):
            username = row[idx["username"]]

        telegram_id = telegram_id.strip()
        name = name.strip()
        username = username.strip()

        if not telegram_id and not name:
            continue

        managers.append(
            {
                "telegram_id": telegram_id,
                "name": name,
                "username": username,
            }
        )

    return managers


# ----------------------------
# Meetings storage (NEW)
# ----------------------------

MEETINGS_SHEET_NAME = "Meetings"

MEETINGS_HEADERS = [
    "created_at",
    "created_by_id",
    "created_by_username",
    "chat_id",
    "thread_id",
    "client",
    "date",
    "time",
    "start_iso",
    "end_iso",
    "manager_name",
    "manager_username",
    "manager_telegram_id",
    "comment",
    "event_id",
    "status",  # created / canceled / updated
]


def ensure_meetings_sheet():
    """
    Ensures Meetings sheet exists and has header row.
    If sheet doesn't exist -> create it.
    If exists but empty -> add headers.
    """
    sheet = _get_sheet()

    try:
        ws = sheet.worksheet(MEETINGS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=MEETINGS_SHEET_NAME, rows=2000, cols=len(MEETINGS_HEADERS))

    values = ws.get_all_values()
    if not values:
        ws.append_row(MEETINGS_HEADERS, value_input_option="USER_ENTERED")
        return ws

    # If first row doesn't look like headers — we won't overwrite; but we can check minimal
    first_row_norm = [_norm(x) for x in values[0]]
    expected_norm = [_norm(x) for x in MEETINGS_HEADERS]
    if first_row_norm[: len(expected_norm)] != expected_norm:
        # Do not destroy existing data; just raise clear error
        raise RuntimeError(
            f"Лист '{MEETINGS_SHEET_NAME}' существует, но заголовки не совпадают. "
            f"Ожидаю: {MEETINGS_HEADERS}"
        )

    return ws


def append_meeting(
    *,
    created_by_id: int,
    created_by_username: str,
    chat_id: int,
    thread_id: int | None,
    client: str,
    date: str,         # DD.MM.YYYY
    time: str,         # HH:MM
    start_iso: str,    # ISO string
    end_iso: str,      # ISO string
    manager_name: str,
    manager_username: str,
    manager_telegram_id: int,
    comment: str,
    event_id: str,
    status: str = "created",
):
    """
    Appends a meeting row into Meetings sheet.
    """
    ws = ensure_meetings_sheet()

    row = [
        _now_str(),
        str(created_by_id),
        created_by_username or "",
        str(chat_id),
        str(thread_id or ""),
        client or "",
        date or "",
        time or "",
        start_iso or "",
        end_iso or "",
        manager_name or "",
        manager_username or "",
        str(manager_telegram_id or 0),
        comment or "",
        event_id or "",
        status or "created",
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")


def list_meetings_for_date(date_ddmmYYYY: str) -> list[dict]:
    """
    Reads meetings for specific date (DD.MM.YYYY) from Meetings sheet.
    Returns list of dicts with keys from headers.
    """
    ws = ensure_meetings_sheet()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = values[0]
    rows = values[1:]

    # find date col index
    headers_norm = [_norm(h) for h in headers]
    try:
        idx_date = headers_norm.index("date")
    except ValueError:
        return []

    res = []
    for r in rows:
        if len(r) <= idx_date:
            continue
        if (r[idx_date] or "").strip() == date_ddmmYYYY:
            item = {}
            for i, h in enumerate(headers):
                item[h] = r[i] if i < len(r) else ""
            res.append(item)

    # sort by time if possible
    def _sort_key(x: dict):
        return (x.get("time") or "").strip()

    res.sort(key=_sort_key)
    return res

def get_meeting_by_event_id(event_id: str) -> dict | None:
    """
    Ищет встречу в листе Meetings по event_id.
    Возвращает dict (headers -> values) или None.
    """
    ws = ensure_meetings_sheet()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    headers = values[0]
    headers_norm = [_norm(h) for h in headers]

    try:
        idx_event = headers_norm.index("event_id")
    except ValueError:
        return None

    for row in values[1:]:
        if len(row) <= idx_event:
            continue
        if (row[idx_event] or "").strip() == (event_id or "").strip():
            item = {}
            for i, h in enumerate(headers):
                item[h] = row[i] if i < len(row) else ""
            return item

    return None


def update_meeting_by_event_id(event_id: str, updates: dict) -> bool:
    """
    Обновляет строку в Meetings по event_id (на месте).
    updates: {"time": "...", "client": "...", "comment": "...", ...}
    Возвращает True если обновили.
    """
    ws = ensure_meetings_sheet()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return False

    headers = values[0]
    headers_norm = [_norm(h) for h in headers]

    try:
        idx_event = headers_norm.index("event_id")
    except ValueError:
        return False

    target_row = None
    for i, row in enumerate(values[1:], start=2):  # 1-based для gspread, header = 1
        if len(row) > idx_event and (row[idx_event] or "").strip() == (event_id or "").strip():
            target_row = i
            break

    if not target_row:
        return False

    for k, v in updates.items():
        kn = _norm(k)
        if kn not in headers_norm:
            continue
        col_idx = headers_norm.index(kn) + 1
        ws.update_cell(target_row, col_idx, str(v))

    return True
