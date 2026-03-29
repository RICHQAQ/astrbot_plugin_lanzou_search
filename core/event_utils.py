from __future__ import annotations

from typing import Any

import astrbot.api.message_components as Comp

from .utils import normalize_session_body


def event_sender_id(event: Any) -> str:
    try:
        return str(event.get_sender_id() or "").strip()
    except Exception:
        return ""


def event_group_id(event: Any) -> str:
    try:
        return str(event.get_group_id() or "").strip()
    except Exception:
        return ""


def event_sender_scope(event: Any) -> str:
    sender_id = event_sender_id(event)
    group_id = event_group_id(event)
    if group_id:
        return f"group:{group_id}:user:{sender_id or 'unknown'}"
    return f"private:{sender_id or 'unknown'}"


def event_self_id(event: Any) -> str:
    for obj in (getattr(event, "message_obj", None), event):
        if obj is None:
            continue
        try:
            value = str(getattr(obj, "self_id", "") or "").strip()
        except Exception:
            value = ""
        if value:
            return value
    return ""


def event_message_token(event: Any) -> str:
    getter = getattr(event, "get_message_id", None)
    if callable(getter):
        try:
            token = str(getter() or "").strip()
            if token:
                return token
        except Exception:
            pass

    for attr_name in ("message_id", "msg_id", "id"):
        try:
            token = str(getattr(event, attr_name, "") or "").strip()
        except Exception:
            token = ""
        if token:
            return token

    for obj_name in ("message_obj", "raw_message", "message"):
        try:
            obj = getattr(event, obj_name, None)
        except Exception:
            obj = None
        if obj is None:
            continue
        for attr_name in ("message_id", "msg_id", "id"):
            try:
                token = str(getattr(obj, attr_name, "") or "").strip()
            except Exception:
                token = ""
            if token:
                return token
    return ""


def is_group_chat(event: Any) -> bool:
    return bool(event_group_id(event))


def event_session_key(event: Any) -> str:
    return event_sender_scope(event)


def event_message_components(event: Any) -> list[Any]:
    try:
        message_obj = getattr(event, "message_obj", None)
    except Exception:
        message_obj = None
    if message_obj is None:
        return []
    try:
        components = getattr(message_obj, "message", None)
    except Exception:
        components = None
    return components if isinstance(components, list) else []


def event_reply_component(event: Any) -> Any | None:
    for component in event_message_components(event):
        if isinstance(component, Comp.Reply):
            return component
    return None


def event_unified_msg_origin(event: Any) -> str:
    try:
        return str(getattr(event, "unified_msg_origin", "") or "").strip()
    except Exception:
        return ""


def event_role(event: Any) -> str:
    try:
        return str(getattr(event, "role", "") or "").strip().lower()
    except Exception:
        return ""


def is_admin_event(event: Any) -> bool:
    return event_role(event) == "admin"


def reply_body_text(reply_component: Any) -> str:
    chain = getattr(reply_component, "chain", None) or []
    parts: list[str] = []
    if isinstance(chain, list):
        for component in chain:
            if isinstance(component, Comp.At):
                continue
            if isinstance(component, Comp.Plain):
                parts.append(str(getattr(component, "text", "") or ""))
                continue
            text = str(getattr(component, "text", "") or "").strip()
            if text:
                parts.append(text)
    body = normalize_session_body("".join(parts))
    if body:
        return body
    return normalize_session_body(
        str(getattr(reply_component, "message_str", "") or "")
    )
