# src/calendar/calendar_service.py
from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional

import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import GOOGLE_CALENDAR_ID, TZ

SCOPES = ["https://www.googleapis.com/auth/calendar"]


# -------------------- Google Calendar service --------------------
def _get_calendar_service():
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=SCOPES,
    )
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


# -------------------- TZ helpers --------------------
def _ensure_tz(dt: datetime) -> datetime:
    """
    Гарантируем, что datetime в TZ и timezone-aware.
    - naive -> localize(TZ)
    - aware -> convert to TZ
    """
    tz = pytz.timezone(TZ)
    if dt.tzinfo is None:
        return tz.localize(dt)
    return dt.astimezone(tz)


# -------------------- Description helpers --------------------
def _build_description(
    *,
    manager_id: int,
    manager_name: str,
    client: str,
    comment: str = "",
) -> str:
    """
    Структурированное описание (удобно обновлять и парсить).

    Поддержка многострочного comment:
    comment<<<
    ...
    >>>comment
    """
    lines = [
        "source: qeepe_meets",
        f"manager_id: {manager_id}",
        f"manager_name: {manager_name}",
        f"client: {client}",
    ]

    c = (comment or "").strip()
    if c:
        # блок для многострочного комментария
        lines.append("comment<<<")
        lines.extend(c.splitlines())
        lines.append(">>>comment")

    return "\n".join(lines)


def parse_qeepe_description(description: str) -> Dict[str, Any]:
    """
    Парсит description, который был создан _build_description().

    Поддерживает 2 формата:
    1) Новый:
       comment<<<
       line1
       line2
       >>>comment

    2) Старый:
       comment: some text
       (берём только одну строку после comment:)
    """
    desc = (description or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = desc.split("\n")

    data: Dict[str, Any] = {}
    comment_lines: List[str] = []
    in_comment_block = False

    def _is_kv_line(s: str) -> bool:
        # "key: value" (простая эвристика)
        if ":" not in s:
            return False
        k = s.split(":", 1)[0].strip()
        return bool(k) and all(ch.isalnum() or ch in "_-" for ch in k)

    for line in lines:
        raw = line.strip()

        if raw == "comment<<<":
            in_comment_block = True
            comment_lines = []
            continue

        if raw == ">>>comment":
            in_comment_block = False
            data["comment"] = "\n".join(comment_lines).strip()
            continue

        if in_comment_block:
            comment_lines.append(line.rstrip("\n"))
            continue

        # старый формат comment:
        if raw.startswith("comment:"):
            data["comment"] = raw.replace("comment:", "", 1).strip()
            continue

        # обычные key: value
        if _is_kv_line(raw):
            k, v = raw.split(":", 1)
            data[k.strip()] = v.strip()

    # приведение типов
    if "manager_id" in data:
        try:
            data["manager_id"] = int(str(data["manager_id"]).strip() or "0")
        except Exception:
            data["manager_id"] = 0

    return data


def extract_qeepe_fields_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализатор: вытаскивает основные поля из event, включая распарсенный description.
    Удобно для отчётов и кнопок.
    """
    desc = event.get("description") or ""
    parsed = parse_qeepe_description(desc)

    # start/end (RFC3339)
    start_raw = (event.get("start") or {}).get("dateTime") or ""
    end_raw = (event.get("end") or {}).get("dateTime") or ""

    tz = pytz.timezone(TZ)

    def _parse_rfc3339(s: str) -> Optional[datetime]:
        if not s:
            return None
        # python 3.11+ умеет fromisoformat с offset, но иногда приходит 'Z'
        s2 = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            return dt.astimezone(tz)
        except Exception:
            return None

    start_dt = _parse_rfc3339(start_raw)
    end_dt = _parse_rfc3339(end_raw)

    return {
        "event_id": event.get("id"),
        "summary": event.get("summary") or "",
        "source": parsed.get("source") or "",
        "manager_id": parsed.get("manager_id", 0),
        "manager_name": parsed.get("manager_name", "") or "",
        "client": parsed.get("client", "") or "",
        "comment": parsed.get("comment", "") or "",
        "start_dt": start_dt,
        "end_dt": end_dt,
        "raw": event,
    }


# -------------------- CRUD --------------------
def create_meeting_event(
    client: str,
    start_dt: datetime,
    end_dt: datetime,
    manager_id: int,
    manager_name: str,
    comment: str = "",
) -> str:
    """
    Создаёт событие в общем календаре.
    Возвращает event_id.
    """
    service = _get_calendar_service()

    start_dt = _ensure_tz(start_dt)
    end_dt = _ensure_tz(end_dt)

    event = {
        "summary": f"Встреча: {client}",
        "description": _build_description(
            manager_id=manager_id,
            manager_name=manager_name,
            client=client,
            comment=comment,
        ),
        "start": {"dateTime": start_dt.isoformat(), "timeZone": TZ},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": TZ},
    }

    created = service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=event).execute()
    return created["id"]


def get_event(event_id: str) -> Dict[str, Any]:
    """
    Получить событие по event_id.
    """
    service = _get_calendar_service()
    return service.events().get(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id).execute()


def update_meeting_event(
    *,
    event_id: str,
    client: Optional[str] = None,            # обновит summary + client в description
    start_dt: Optional[datetime] = None,     # обновит start
    end_dt: Optional[datetime] = None,       # обновит end
    manager_id: Optional[int] = None,        # для description
    manager_name: Optional[str] = None,      # для description
    comment: Optional[str] = None,           # для description
    summary: Optional[str] = None,           # ручной summary
    description: Optional[str] = None,       # ручной description
) -> Dict[str, Any]:
    """
    PATCH-update события.

    Если переданы client/manager/comment — description собираем автоматически,
    но можно переопределить вручную через description=...
    summary можно авто (Встреча: client) или вручную через summary=...
    """
    service = _get_calendar_service()
    patch: Dict[str, Any] = {}

    # ---- time ----
    if start_dt is not None:
        start_dt = _ensure_tz(start_dt)
        patch["start"] = {"dateTime": start_dt.isoformat(), "timeZone": TZ}

    if end_dt is not None:
        end_dt = _ensure_tz(end_dt)
        patch["end"] = {"dateTime": end_dt.isoformat(), "timeZone": TZ}

    # ---- summary ----
    if summary is not None:
        patch["summary"] = summary
    elif client is not None:
        patch["summary"] = f"Встреча: {client}"

    # ---- description ----
    if description is not None:
        patch["description"] = description
    else:
        need_rebuild = any(x is not None for x in [client, manager_id, manager_name, comment])
        if need_rebuild:
            current = get_event(event_id)
            parsed = parse_qeepe_description(current.get("description") or "")

            curr_manager_id = int(parsed.get("manager_id", 0) or 0)
            curr_manager_name = str(parsed.get("manager_name", "") or "")
            curr_client = str(parsed.get("client", "") or "")
            curr_comment = str(parsed.get("comment", "") or "")

            new_client = client if client is not None else curr_client
            new_manager_id = manager_id if manager_id is not None else curr_manager_id
            new_manager_name = manager_name if manager_name is not None else curr_manager_name
            new_comment = comment if comment is not None else curr_comment

            patch["description"] = _build_description(
                manager_id=new_manager_id,
                manager_name=new_manager_name,
                client=new_client,
                comment=new_comment,
            )

    if not patch:
        return get_event(event_id)

    updated = service.events().patch(
        calendarId=GOOGLE_CALENDAR_ID,
        eventId=event_id,
        body=patch,
    ).execute()

    return updated


def delete_event(event_id: str) -> None:
    """
    Удалить событие по event_id.
    """
    service = _get_calendar_service()
    service.events().delete(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id).execute()


# -------------------- Listing --------------------
def list_events_for_date(day: date) -> List[Dict[str, Any]]:
    """
    Список событий на конкретный день (по TZ).
    """
    service = _get_calendar_service()
    tz = pytz.timezone(TZ)

    start = tz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
    end = start + timedelta(days=1)

    events_result = (
        service.events()
        .list(
            calendarId=GOOGLE_CALENDAR_ID,
            timeMin=start.isoformat(),
            timeMax=end.isoformat(),
            singleEvents=True,
            orderBy="startTime",
            maxResults=250,
        )
        .execute()
    )

    return events_result.get("items", [])


def list_qeepe_meetings_for_date(day: date, *, only_source: bool = True) -> List[Dict[str, Any]]:
    """
    Удобная версия для отчётов:
    - возвращает список НОРМАЛИЗОВАННЫХ встреч (dict), где есть client/manager/comment/start_dt/end_dt/event_id
    - если only_source=True, берём только те, у которых source == 'qeepe_meets'
    """
    items = list_events_for_date(day)
    out: List[Dict[str, Any]] = []

    for ev in items:
        fields = extract_qeepe_fields_from_event(ev)
        if only_source:
            if (fields.get("source") or "").strip() != "qeepe_meets":
                continue
        out.append(fields)

    return out
