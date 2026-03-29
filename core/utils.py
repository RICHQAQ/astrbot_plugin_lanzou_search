from __future__ import annotations

import hashlib
import math
import posixpath
import re
import secrets
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from .constants import (
    ACTIVATION_CODE_ALPHABET,
    ACTIVATION_COMMAND_ALIASES,
    COMMAND_NAME,
    PAGE_SIZE,
)
from .models import SearchItem


def safe_int(
    value: Any, default: int, minimum: int | None = None, maximum: int | None = None
) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def safe_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def extract_command_keyword(raw_text: str, command: str) -> str:
    text = (raw_text or "").strip()
    if not text:
        return ""
    match = re.match(
        rf"^(?:[/!#.])?{re.escape(command)}(?:\s+(.*))?$", text, flags=re.IGNORECASE
    )
    if match:
        return (match.group(1) or "").strip()
    return text


def parse_search_command(raw_text: str) -> str | None:
    text = (raw_text or "").strip()
    if not text:
        return None
    match = re.match(
        rf"^(?:[/!#.])?{re.escape(COMMAND_NAME)}(?:\s+(.*))?$",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return (match.group(1) or "").strip()
    return None


def parse_activation_command(raw_text: str) -> str | None:
    text = (raw_text or "").strip()
    if not text:
        return None
    for alias in ACTIVATION_COMMAND_ALIASES:
        match = re.match(
            rf"^(?:[/!#.])?{re.escape(alias)}(?:\s+(.*))?$", text, flags=re.IGNORECASE
        )
        if match:
            code = str(match.group(1) or "").strip()
            return normalize_activation_code(code)
    return None


def normalize_session_body(text: str) -> str:
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized


def hash_session_body(text: str) -> str:
    normalized = normalize_session_body(text)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def normalize_activation_code(code: str) -> str:
    return re.sub(r"\s+", "", str(code or "").upper())


def normalize_path(path: str) -> str:
    text = "" if path is None else str(path)
    if not text.strip():
        return "/"
    normalized = posixpath.normpath(text if text.startswith("/") else f"/{text}")
    return "/" if normalized in {"", "."} else normalized


def resolve_search_root(mount_path: str, search_path: str) -> str:
    normalized_mount = normalize_path(mount_path)
    normalized_search = normalize_path(search_path)
    if normalized_search == "/":
        return normalized_mount
    if normalized_search == normalized_mount or normalized_search.startswith(
        normalized_mount + "/"
    ):
        return normalized_search
    return normalize_path(
        posixpath.join(normalized_mount, normalized_search.lstrip("/"))
    )


def encode_openlist_path(path: str) -> str:
    normalized = normalize_path(path)
    segments = normalized.split("/")
    return "/".join(quote(segment, safe="") for segment in segments)


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\\\|?*\\x00-\\x1f]', "_", str(name or "").strip())
    return cleaned or f"download_{uuid4().hex}"


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        for item in value:
            text = as_text(item)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        for key in ("title", "name", "text", "value", "message"):
            text = as_text(value.get(key))
            if text:
                return text
        return ""
    return str(value).strip()


def safe_name(text: str, fallback: str = "未命名") -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", as_text(text) or fallback)
    cleaned = cleaned.strip(" .")
    return cleaned[:80] or fallback


def get_gmt_date_string() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def quark_response_ok(payload: dict[str, Any]) -> bool:
    if "status" in payload:
        return safe_int(payload.get("status"), 500) == 200
    if "code" in payload:
        return safe_int(payload.get("code"), -1) == 0
    return False


def quark_response_message(payload: dict[str, Any], default: str) -> str:
    for key in ("message", "msg", "error"):
        text = as_text(payload.get(key))
        if text:
            return text
    return default


def generate_activation_code(length: int) -> str:
    normalized_length = max(4, min(16, int(length)))
    return "".join(
        secrets.choice(ACTIVATION_CODE_ALPHABET) for _ in range(normalized_length)
    )


def total_pages(total: int) -> int:
    return max(1, math.ceil(max(0, total) / PAGE_SIZE))


def human_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(0, size))
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def format_search_results(
    keyword: str,
    page: int,
    total: int,
    items: list[SearchItem],
    *,
    require_reply: bool,
    loaded_count: int = 0,
    has_more: bool = False,
    total_exact: bool = True,
) -> str:
    page_total = total if total_exact else max(total, loaded_count)
    current_page = min(max(1, page), total_pages(page_total))
    lines = [f"检索结果：{keyword}", ""]
    if not items:
        lines.append("当前页没有结果。")
    else:
        for index, item in enumerate(items, start=1):
            lines.append(f"{index}. {item.name} 【{human_size(item.size)}】")
    lines.append("")
    if total_exact:
        lines.append(f"第 {current_page}/{total_pages(page_total)} 页，共 {total} 条")
    else:
        lines.append(
            f"第 {current_page}/{total_pages(page_total)}+ 页，已加载前 {max(page_total, loaded_count)} 条，结果较多，可继续翻页"
        )
    lines.append(
        "引用本条后发送序号 / n / p / q" if require_reply else "发送序号 / n / p / q"
    )
    return "\n".join(lines)
