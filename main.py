from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import mimetypes
import posixpath
import re
import secrets
import shutil
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlparse, urlsplit, urlunsplit
from uuid import uuid4

import astrbot.api.message_components as Comp
import httpx
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

OPENLIST_SEARCH_API = "/api/fs/search"
OPENLIST_GET_API = "/api/fs/get"
OPENLIST_LIST_API = "/api/fs/list"
OPENLIST_LINK_API = "/api/fs/link"
OPENLIST_MKDIR_API = "/api/fs/mkdir"
OPENLIST_REMOVE_API = "/api/fs/remove"
OPENLIST_ADD_OFFLINE_DOWNLOAD_API = "/api/fs/add_offline_download"
OPENLIST_LOGIN_HASH_API = "/api/auth/login/hash"
OPENLIST_STATIC_HASH_SALT = "https://github.com/alist-org/alist"

COMMAND_NAME = "sh"
PAGE_SIZE = 10
MAX_SOURCES = 5
DEFAULT_SELECTION_DEDUP_WINDOW_SECONDS = 5
DOWNLOAD_DIR_RETENTION_SECONDS = 3600
DOWNLOAD_SEND_GRACE_SECONDS = 180
FILE_PREPARE_RETRY_COUNT = 3
FILE_PREPARE_RETRY_DELAY_SECONDS = 1.5
FILE_OPERATION_TIMEOUT_SECONDS = 180
FILE_SEND_TOTAL_TIMEOUT_SECONDS = 90
FILE_LINK_CACHE_TTL_SECONDS = 90
FILE_MISSING_CACHE_TTL_SECONDS = 15
OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS = 2.0
ACTIVATION_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ACTIVATION_PACKAGE_MIN_TEXT_BYTES = 24 * 1024 * 1024
ACTIVATION_PACKAGE_PADDING_SEED = b"astrbot_plugin_lanzou_search_activation_padding_v1"
ACTIVATION_COMMAND_ALIASES = ("激活", "jh")
ACTIVATION_STATE_FILE = "activation_state.json"
ACTIVATION_PENDING_STATE_FILE = "activation_pending_state.json"
ACTIVATION_USERS_FILE = "activation_users.json"
ACTIVATION_REQUESTS_FILE = "activation_requests.json"
ACTIVATION_ADMIN_ORIGINS_FILE = "activation_admin_origins.json"
QUARK_BASE_URL = "https://drive-pc.quark.cn/1/clouddrive"
QUARK_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json;charset=UTF-8",
    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "Referer": "https://pan.quark.cn/",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ),
}
QUARK_PARAMS = {"pr": "ucpro", "fr": "pc", "uc_param_str": ""}
QUARK_OSS_USER_AGENT = "aliyun-sdk-js/6.18.0 Chrome/134.0.0.0 on Windows 10 64-bit"


@dataclass(slots=True)
class SourceConfig:
    slot: int
    label: str
    mount_path: str
    search_path: str


@dataclass(slots=True)
class SearchItem:
    name: str
    full_path: str
    size: int = 0


@dataclass(slots=True)
class SearchPage:
    keyword: str
    page: int
    total: int
    items: list[SearchItem]
    loaded_count: int = 0
    has_more: bool = False
    total_exact: bool = True


class FileRecordExpiredError(RuntimeError):
    pass


@dataclass(slots=True)
class TransferResult:
    success: bool
    title: str = ""
    share_url: str = ""
    password: str = ""
    message: str = ""
    cleanup_job: dict[str, Any] | None = None


class NapCatStreamClient:
    def __init__(
        self,
        *,
        action_sender: Any,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        self.action_sender = action_sender
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _connect(self):
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("未安装 websockets 依赖，请先重载插件依赖") from exc

        headers: dict[str, str] = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return await websockets.connect(
            self.ws_url,
            additional_headers=headers or None,
            open_timeout=self.timeout,
            close_timeout=min(10.0, self.timeout),
            max_size=None,
        )

    async def _send_action(self, websocket: Any, action: str, params: dict[str, Any], echo: str | None = None) -> dict[str, Any]:
        payload = {
            "action": action,
            "params": params,
            "echo": echo or uuid4().hex,
        }
        await websocket.send(json.dumps(payload, ensure_ascii=False))
        while True:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=self.timeout)
            data = json.loads(raw_message)
            if str(data.get("echo", "") or "") == payload["echo"]:
                return data

    def _calculate_file_chunks(self, file_path: Path) -> tuple[list[str], str, int]:
        chunks: list[str] = []
        hasher = hashlib.sha256()
        total_size = 0
        with file_path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(self.chunk_size_bytes)
                if not chunk:
                    break
                chunks.append(base64.b64encode(chunk).decode("utf-8"))
                hasher.update(chunk)
                total_size += len(chunk)
        return chunks, hasher.hexdigest(), total_size

    async def upload_file(self, file_path: Path) -> str:
        if not self.ws_url:
            raise RuntimeError("未配置 napcat.ws_url")
        if not file_path.exists():
            raise RuntimeError(f"待上传文件不存在：{file_path}")

        chunks, sha256_hash, total_size = self._calculate_file_chunks(file_path)
        if not chunks:
            raise RuntimeError("待上传文件为空")

        stream_id = uuid4().hex
        websocket = await self._connect()
        try:
            total_chunks = len(chunks)
            for chunk_index, chunk_base64 in enumerate(chunks):
                response = await self._send_action(
                    websocket,
                    "upload_file_stream",
                    {
                        "stream_id": stream_id,
                        "chunk_data": chunk_base64,
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "file_size": total_size,
                        "expected_sha256": sha256_hash,
                        "filename": file_path.name,
                        "file_retention": self.file_retention_ms,
                    },
                )
                if str(response.get("status", "") or "").lower() != "ok":
                    raise RuntimeError(str(response.get("wording") or response.get("message") or "NapCat Stream 分片上传失败"))

            response = await self._send_action(
                websocket,
                "upload_file_stream",
                {"stream_id": stream_id, "is_complete": True},
            )
            if str(response.get("status", "") or "").lower() != "ok":
                raise RuntimeError(str(response.get("wording") or response.get("message") or "NapCat Stream 合并失败"))

            result = response.get("data") or {}
            if str(result.get("status", "") or "") != "file_complete":
                raise RuntimeError("NapCat Stream 文件状态异常")
            uploaded_path = str(result.get("file_path", "") or "").strip()
            if not uploaded_path:
                raise RuntimeError("NapCat Stream 未返回文件路径")
            return uploaded_path
        finally:
            await websocket.close()


class NapCatActionStreamClient:
    def __init__(
        self,
        *,
        action_sender: Any,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        self.action_sender = action_sender
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _send_action(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        if not callable(self.action_sender):
            raise RuntimeError("NapCat Stream action sender unavailable")
        response = await self.action_sender(action, **params)
        return response if isinstance(response, dict) else {}

    def _calculate_file_chunks(self, file_path: Path) -> tuple[list[str], str, int]:
        chunks: list[str] = []
        hasher = hashlib.sha256()
        total_size = 0
        with file_path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(self.chunk_size_bytes)
                if not chunk:
                    break
                chunks.append(base64.b64encode(chunk).decode("utf-8"))
                hasher.update(chunk)
                total_size += len(chunk)
        return chunks, hasher.hexdigest(), total_size

    async def upload_file(self, file_path: Path) -> str:
        if not file_path.exists():
            raise RuntimeError(f"stream source file not found: {file_path}")

        chunks, sha256_hash, total_size = self._calculate_file_chunks(file_path)
        if not chunks:
            raise RuntimeError("stream source file is empty")

        stream_id = uuid4().hex
        total_chunks = len(chunks)
        for chunk_index, chunk_base64 in enumerate(chunks):
            await self._send_action(
                "upload_file_stream",
                {
                    "stream_id": stream_id,
                    "chunk_data": chunk_base64,
                    "chunk_index": chunk_index,
                    "total_chunks": total_chunks,
                    "file_size": total_size,
                    "expected_sha256": sha256_hash,
                    "filename": file_path.name,
                    "file_retention": self.file_retention_ms,
                },
            )

        result = await self._send_action(
            "upload_file_stream",
            {"stream_id": stream_id, "is_complete": True},
        )
        if str(result.get("status", "") or "") != "file_complete":
            raise RuntimeError(f"unexpected stream completion payload: {result}")
        uploaded_path = str(result.get("file_path", "") or "").strip()
        if not uploaded_path:
            raise RuntimeError(f"stream upload did not return file path: {result}")
        return uploaded_path


class NapCatWsStreamClient:
    def __init__(
        self,
        *,
        ws_url: str,
        access_token: str = "",
        timeout: float = 180.0,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        self.ws_url = str(ws_url or "").strip()
        self.access_token = str(access_token or "").strip()
        self.timeout = max(10.0, float(timeout))
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _connect(self):
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("未安装 websockets 依赖，请先重载插件依赖") from exc

        headers: dict[str, str] = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return await websockets.connect(
            self.ws_url,
            additional_headers=headers or None,
            open_timeout=self.timeout,
            close_timeout=min(10.0, self.timeout),
            max_size=None,
        )

    async def _send_action(self, websocket: Any, action: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "action": action,
            "params": params,
            "echo": uuid4().hex,
        }
        await websocket.send(json.dumps(payload, ensure_ascii=False))
        while True:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=self.timeout)
            data = json.loads(raw_message)
            if str(data.get("echo", "") or "") != payload["echo"]:
                continue
            if str(data.get("status", "") or "").lower() != "ok":
                raise RuntimeError(str(data.get("wording") or data.get("message") or "NapCat Stream 调用失败"))
            result = data.get("data") or {}
            return result if isinstance(result, dict) else {}

    def _calculate_file_chunks(self, file_path: Path) -> tuple[list[str], str, int]:
        chunks: list[str] = []
        hasher = hashlib.sha256()
        total_size = 0
        with file_path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(self.chunk_size_bytes)
                if not chunk:
                    break
                chunks.append(base64.b64encode(chunk).decode("utf-8"))
                hasher.update(chunk)
                total_size += len(chunk)
        return chunks, hasher.hexdigest(), total_size

    async def upload_file(self, file_path: Path) -> str:
        if not self.ws_url:
            raise RuntimeError("未配置 napcat.ws_url")
        if not file_path.exists():
            raise RuntimeError(f"stream source file not found: {file_path}")

        chunks, sha256_hash, total_size = self._calculate_file_chunks(file_path)
        if not chunks:
            raise RuntimeError("stream source file is empty")

        stream_id = uuid4().hex
        websocket = await self._connect()
        try:
            total_chunks = len(chunks)
            for chunk_index, chunk_base64 in enumerate(chunks):
                await self._send_action(
                    websocket,
                    "upload_file_stream",
                    {
                        "stream_id": stream_id,
                        "chunk_data": chunk_base64,
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "file_size": total_size,
                        "expected_sha256": sha256_hash,
                        "filename": file_path.name,
                        "file_retention": self.file_retention_ms,
                    },
                )

            result = await self._send_action(
                websocket,
                "upload_file_stream",
                {"stream_id": stream_id, "is_complete": True},
            )
            if str(result.get("status", "") or "") != "file_complete":
                raise RuntimeError(f"unexpected stream completion payload: {result}")
            uploaded_path = str(result.get("file_path", "") or "").strip()
            if not uploaded_path:
                raise RuntimeError(f"stream upload did not return file path: {result}")
            return uploaded_path
        finally:
            await websocket.close()


def safe_int(value: Any, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
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
    match = re.match(rf"^(?:[/!#.])?{re.escape(command)}(?:\s+(.*))?$", text, flags=re.IGNORECASE)
    if match:
        return (match.group(1) or "").strip()
    return text


def parse_search_command(raw_text: str) -> str | None:
    text = (raw_text or "").strip()
    if not text:
        return None
    match = re.match(rf"^(?:[/!#.])?{re.escape(COMMAND_NAME)}(?:\s+(.*))?$", text, flags=re.IGNORECASE)
    if match:
        return (match.group(1) or "").strip()
    return None


def parse_activation_command(raw_text: str) -> str | None:
    text = (raw_text or "").strip()
    if not text:
        return None
    for alias in ACTIVATION_COMMAND_ALIASES:
        match = re.match(rf"^(?:[/!#.])?{re.escape(alias)}(?:\s+(.*))?$", text, flags=re.IGNORECASE)
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
    if normalized_search == normalized_mount or normalized_search.startswith(normalized_mount + "/"):
        return normalized_search
    return normalize_path(posixpath.join(normalized_mount, normalized_search.lstrip("/")))


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
    return "".join(secrets.choice(ACTIVATION_CODE_ALPHABET) for _ in range(normalized_length))


def append_fixed_activation_padding(file_path: Path, minimum_bytes: int = ACTIVATION_PACKAGE_MIN_TEXT_BYTES) -> None:
    target_size = max(1024, int(minimum_bytes))
    prefix = "\n\n----- 以下为固定填充内容，请勿删除 -----\n"
    written = file_path.stat().st_size if file_path.exists() else 0
    counter = 0
    with file_path.open("a", encoding="utf-8", newline="\n") as file_obj:
        if written < target_size:
            file_obj.write(prefix)
            written += len(prefix.encode("utf-8"))
        while written < target_size:
            block_lines: list[str] = []
            for _ in range(256):
                digest = hashlib.sha256(
                    ACTIVATION_PACKAGE_PADDING_SEED + counter.to_bytes(8, "big")
                ).hexdigest()
                block_lines.append(f"{counter:08d}:{digest}")
                counter += 1
            block = "\n".join(block_lines) + "\n"
            file_obj.write(block)
            written += len(block.encode("utf-8"))


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
        lines.append(f"第 {current_page}/{total_pages(page_total)}+ 页，已加载前 {max(page_total, loaded_count)} 条，结果较多，可继续翻页")
    lines.append("引用本条后发送序号 / n / p / q" if require_reply else "发送序号 / n / p / q")
    return "\n".join(lines)


class OpenListClient:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        base_url: str,
        *,
        token: str = "",
        username: str = "",
        password: str = "",
        request_timeout_seconds: int = 60,
    ) -> None:
        self.http_client = http_client
        self.base_url = str(base_url or "").rstrip("/")
        self.supplied_token = str(token or "").strip()
        self.username = str(username or "").strip()
        self.password = str(password or "")
        self.request_timeout = max(10.0, float(request_timeout_seconds or 60))
        self.token = self.supplied_token
        self._token_lock = asyncio.Lock()

    def _has_credentials(self) -> bool:
        return bool(self.username and self.password)

    def _static_hash(self, password: str) -> str:
        raw = f"{password}-{OPENLIST_STATIC_HASH_SALT}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _parse_json(self, response: httpx.Response, source: str) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f"{source} 返回的不是 JSON：{response.text[:200]}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"{source} 返回的数据格式不正确。")
        return data

    def _extract_error_message(self, data: dict[str, Any], *, default: str) -> str:
        for key in ("message", "msg", "error"):
            value = str(data.get(key, "") or "").strip()
            if value:
                return value
        detail = data.get("data")
        if isinstance(detail, dict):
            for key in ("message", "msg", "error"):
                value = str(detail.get(key, "") or "").strip()
                if value:
                    return value
        return default

    async def _ensure_token(self, force: bool = False) -> str:
        if self.supplied_token:
            self.token = self.supplied_token
            return self.supplied_token
        if not self.username and not self.password:
            return ""
        if not self._has_credentials():
            raise RuntimeError("OpenList 用户名和密码需要一起填写。")

        async with self._token_lock:
            if self.token and not force:
                return self.token

            payload = {
                "username": self.username,
                "password": self._static_hash(self.password),
            }
            response = await self.http_client.post(
                self.base_url + OPENLIST_LOGIN_HASH_API,
                json=payload,
                timeout=self.request_timeout,
            )
            data = self._parse_json(response, "OpenList 登录")
            if response.status_code >= 400:
                raise RuntimeError(
                    self._extract_error_message(data, default=f"OpenList 登录失败：HTTP {response.status_code}")
                )
            code = data.get("code")
            if code not in (None, 200):
                raise RuntimeError(self._extract_error_message(data, default=f"OpenList 登录失败：{code}"))

            token = str((data.get("data") or {}).get("token", "")).strip()
            if not token:
                raise RuntimeError("OpenList 登录成功但没有返回 token。")
            self.token = token
            return token

    async def request(
        self,
        method: str,
        path: str,
        *,
        json_data: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = {}
        token = await self._ensure_token(force=False)
        if token:
            headers["Authorization"] = token
        request_timeout = max(10.0, float(timeout_seconds or self.request_timeout))

        response = await self.http_client.request(
            method,
            self.base_url + path,
            json=json_data,
            headers=headers,
            timeout=request_timeout,
        )
        data = self._parse_json(response, "OpenList")

        if response.status_code == 401 and not self.supplied_token and self._has_credentials():
            token = await self._ensure_token(force=True)
            headers["Authorization"] = token
            response = await self.http_client.request(
                method,
                self.base_url + path,
                json=json_data,
                headers=headers,
                timeout=request_timeout,
            )
            data = self._parse_json(response, "OpenList")

        if response.status_code >= 400:
            raise RuntimeError(self._extract_error_message(data, default=f"OpenList HTTP {response.status_code}"))
        code = data.get("code")
        if code not in (None, 200):
            raise RuntimeError(self._extract_error_message(data, default=f"OpenList 返回错误：{code}"))
        return data

    async def search(self, parent: str, keyword: str, page: int, per_page: int) -> dict[str, Any]:
        payload = {
            "parent": normalize_path(parent),
            "keywords": keyword,
            "scope": 2,
            "page": max(1, page),
            "per_page": max(1, per_page),
        }
        return await self.request("POST", OPENLIST_SEARCH_API, json_data=payload)

    async def get_file(self, path: str) -> dict[str, Any]:
        payload = {
            "path": normalize_path(path),
        }
        return await self.request(
            "POST",
            OPENLIST_GET_API,
            json_data=payload,
            timeout_seconds=max(self.request_timeout, float(FILE_OPERATION_TIMEOUT_SECONDS)),
        )

    async def get_link(self, path: str) -> dict[str, Any]:
        payload = {
            "path": normalize_path(path),
        }
        return await self.request(
            "POST",
            OPENLIST_LINK_API,
            json_data=payload,
            timeout_seconds=max(self.request_timeout, float(FILE_OPERATION_TIMEOUT_SECONDS)),
        )

    async def list_dir(self, path: str, page: int, per_page: int, *, refresh: bool = False) -> dict[str, Any]:
        payload = {
            "path": normalize_path(path),
            "page": max(1, page),
            "per_page": max(1, per_page),
            "refresh": bool(refresh),
        }
        return await self.request(
            "POST",
            OPENLIST_LIST_API,
            json_data=payload,
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def mkdir(self, path: str) -> dict[str, Any]:
        payload = {
            "path": normalize_path(path),
        }
        return await self.request(
            "POST",
            OPENLIST_MKDIR_API,
            json_data=payload,
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def remove(self, dir_path: str, names: list[str]) -> dict[str, Any]:
        filtered_names = [str(name or "").strip() for name in names if str(name or "").strip()]
        payload = {
            "dir": normalize_path(dir_path),
            "names": filtered_names,
        }
        return await self.request(
            "POST",
            OPENLIST_REMOVE_API,
            json_data=payload,
            timeout_seconds=max(self.request_timeout, 120.0),
        )

    async def add_offline_download(
        self,
        urls: list[str],
        path: str,
        *,
        tool: str = "SimpleHttp",
        delete_policy: str = "upload_download_stream",
    ) -> dict[str, Any]:
        payload = {
            "urls": [str(url or "").strip() for url in urls if str(url or "").strip()],
            "path": normalize_path(path),
            "tool": str(tool or "SimpleHttp").strip() or "SimpleHttp",
            "delete_policy": str(delete_policy or "upload_download_stream").strip() or "upload_download_stream",
        }
        return await self.request(
            "POST",
            OPENLIST_ADD_OFFLINE_DOWNLOAD_API,
            json_data=payload,
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def get_task_info(self, task_group: str, task_id: str) -> dict[str, Any]:
        group_name = str(task_group or "").strip().strip("/")
        task_key = quote(str(task_id or "").strip(), safe="")
        return await self.request(
            "POST",
            f"/api/task/{group_name}/info?tid={task_key}",
            json_data={},
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def upload_form(self, local_path: Path, target_path: str, *, overwrite: bool = True) -> dict[str, Any]:
        if not local_path.exists():
            raise RuntimeError(f"待上传文件不存在：{local_path}")

        headers: dict[str, str] = {
            "File-Path": quote(normalize_path(target_path), safe="/"),
            "Overwrite": "true" if overwrite else "false",
            "As-Task": "false",
        }
        token = await self._ensure_token(force=False)
        if token:
            headers["Authorization"] = token

        content_type, _ = mimetypes.guess_type(local_path.name)
        with local_path.open("rb") as file_obj:
            response = await self.http_client.put(
                self.base_url + "/api/fs/form",
                headers=headers,
                files={"file": (local_path.name, file_obj, content_type or "application/octet-stream")},
                timeout=max(self.request_timeout, 300.0),
            )
        data = self._parse_json(response, "OpenList 上传")
        if response.status_code >= 400:
            raise RuntimeError(self._extract_error_message(data, default=f"OpenList 上传失败：HTTP {response.status_code}"))
        code = data.get("code")
        if code not in (None, 200):
            raise RuntimeError(self._extract_error_message(data, default=f"OpenList 上传失败：{code}"))
        return data

    def _normalize_download_url(self, url: str) -> str:
        text = str(url or "").strip()
        if not text:
            return ""
        if text.startswith("/"):
            return self.base_url + text
        return text

    def _is_same_openlist_host(self, url: str) -> bool:
        text = self._normalize_download_url(url)
        try:
            left = httpx.URL(text)
            right = httpx.URL(self.base_url)
        except Exception:
            return False
        return left.scheme == right.scheme and left.host == right.host and left.port == right.port

    async def download(self, url: str, target_path: Path) -> None:
        normalized_url = self._normalize_download_url(url)
        if not normalized_url:
            raise RuntimeError("没有可用的下载地址。")

        headers: dict[str, str] = {}
        if self._is_same_openlist_host(normalized_url):
            token = await self._ensure_token(force=False)
            if token:
                headers["Authorization"] = token

        async with self.http_client.stream(
            "GET",
            normalized_url,
            headers=headers,
            timeout=max(self.request_timeout, float(FILE_OPERATION_TIMEOUT_SECONDS)),
        ) as response:
            if response.status_code >= 400:
                raise RuntimeError(f"文件下载失败：HTTP {response.status_code}")
            with target_path.open("wb") as file_obj:
                async for chunk in response.aiter_bytes():
                    if chunk:
                        file_obj.write(chunk)

class QuarkPanClient:
    def __init__(self, cookie: str, save_fid: str, timeout: int, verify_ssl: bool, share_expired_type: int):
        self.cookie = cookie.strip()
        self.save_fid = (save_fid or "").strip() or "0"
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.share_expired_type = share_expired_type
        self.headers = {**QUARK_HEADERS, "Cookie": self.cookie}
        self.callback: dict[str, Any] = {}

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        merged_params = {**QUARK_PARAMS, **(params or {})}
        async with httpx.AsyncClient(timeout=timeout or self.timeout, verify=self.verify_ssl) as client:
            if method.upper() == "GET":
                response = await client.get(path, params=merged_params, headers=self.headers)
            else:
                response = await client.post(path, params=merged_params, json=payload or {}, headers=self.headers)
            response.raise_for_status()
            return response.json()

    async def _poll_task(self, task_id: str, error_hint: str, max_retries: int = 50) -> dict[str, Any]:
        for retry_index in range(max_retries):
            response = await self._request_json(
                "GET",
                f"{QUARK_BASE_URL}/task",
                params={"task_id": task_id, "retry_index": str(retry_index)},
            )
            if quark_response_message(response, "") == "capacity limit[{0}]":
                raise RuntimeError("夸克空间不足")
            if not quark_response_ok(response):
                await asyncio.sleep(0.5)
                continue
            task_data = response.get("data") or {}
            if safe_int(task_data.get("status"), 0) == 2:
                return task_data
            await asyncio.sleep(0.5)
        raise RuntimeError(error_hint)

    async def create_share(self, fid_list: list[str], title: str) -> tuple[str, str, str]:
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/share",
            payload={
                "fid_list": fid_list,
                "expired_type": self.share_expired_type,
                "title": title,
                "url_type": 1,
            },
        )
        if not quark_response_ok(response):
            raise RuntimeError(quark_response_message(response, "创建夸克分享失败"))

        task_id = as_text((response.get("data") or {}).get("task_id"))
        if not task_id:
            raise RuntimeError("夸克分享任务缺少 task_id")

        share_task = await self._poll_task(task_id, "夸克分享任务超时")
        share_id = as_text(share_task.get("share_id"))
        if not share_id:
            raise RuntimeError("夸克分享结果缺少 share_id")

        password_response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/share/password",
            payload={"share_id": share_id},
        )
        if not quark_response_ok(password_response):
            raise RuntimeError(quark_response_message(password_response, "获取夸克分享链接失败"))

        data = password_response.get("data") or {}
        return share_id, as_text(data.get("share_url")), as_text(data.get("passcode"))

    async def delete_fids(self, fids: list[Any]) -> str | None:
        normalized_fids = [str(fid).strip() for fid in fids if str(fid).strip()]
        if not normalized_fids:
            return None
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/file/delete",
            payload={"action_type": 2, "exclude_fids": [], "filelist": normalized_fids},
        )
        if not quark_response_ok(response):
            return quark_response_message(response, "自动删除夸克旧文件失败")
        return None

    async def list_dir(self, pdir_fid: str = "0") -> list[dict[str, Any]]:
        response = await self._request_json(
            "GET",
            f"{QUARK_BASE_URL}/file/sort",
            params={
                "pdir_fid": pdir_fid or "0",
                "_page": "1",
                "_size": "200",
                "_fetch_total": "1",
                "_fetch_sub_dirs": "0",
                "_sort": "file_type:asc,updated_at:desc",
            },
        )
        if not quark_response_ok(response):
            raise RuntimeError(quark_response_message(response, "获取夸克目录失败"))
        return (response.get("data") or {}).get("list") or []

    async def _compute_file_hash(self, file_path: Path) -> tuple[str, str]:
        def run() -> tuple[str, str]:
            md5_hash = hashlib.md5()
            sha1_hash = hashlib.sha1()
            with file_path.open("rb") as file_obj:
                while True:
                    chunk = file_obj.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    md5_hash.update(chunk)
                    sha1_hash.update(chunk)
            return md5_hash.hexdigest(), sha1_hash.hexdigest()

        return await asyncio.to_thread(run)

    async def _check_fast_upload(self, task_id: str, md5: str, sha1: str) -> dict[str, Any] | None:
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/file/update/hash",
            payload={"task_id": task_id, "md5": md5, "sha1": sha1},
        )
        if quark_response_ok(response) and ((response.get("data") or {}).get("finish") is True):
            return response.get("data") or {}
        return None

    async def _get_upload_auth(
        self,
        task_id: str,
        method: str,
        content_type: str,
        upload_url: str,
        upload_id: str,
        part_number: int | None = None,
        content_md5: str | None = None,
        callback: dict[str, Any] | None = None,
    ) -> tuple[str | None, str | None]:
        parsed_url = urlparse(upload_url)
        resource = parsed_url.path
        query_params: dict[str, str] = {}
        if upload_id:
            query_params["uploadId"] = upload_id
        if part_number is not None:
            query_params["partNumber"] = str(part_number)
        sorted_query = "&".join(f"{key}={value}" for key, value in sorted(query_params.items()))
        if sorted_query:
            resource += f"?{sorted_query}"

        current_time_gmt = get_gmt_date_string()
        oss_headers_list = [
            f"x-oss-date:{current_time_gmt}",
            f"x-oss-user-agent:{QUARK_OSS_USER_AGENT}",
        ]

        if callback and callback.get("callbackUrl") and callback.get("callbackBody"):
            callback_json = json.dumps(callback, ensure_ascii=False, separators=(",", ":"))
            callback_value = base64.b64encode(callback_json.encode("utf-8")).decode("utf-8")
            oss_headers_list.append(f"x-oss-callback:{callback_value}")
        oss_headers_list.sort()
        canonicalized_oss_headers = "\n".join(oss_headers_list) + "\n"

        auth_meta = (
            f"{method.upper()}\n"
            f"{content_md5 or ''}\n"
            f"{content_type or ''}\n"
            f"{current_time_gmt}\n"
            f"{canonicalized_oss_headers}{resource}"
        )

        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/file/upload/auth",
            payload={"task_id": task_id, "auth_meta": auth_meta},
        )
        if not quark_response_ok(response):
            return None, None
        auth_key = as_text((response.get("data") or {}).get("auth_key"))
        if not auth_key:
            return None, None
        return auth_key, current_time_gmt

    async def _complete_multipart_upload(
        self,
        task_id: str,
        complete_oss_url: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> bool:
        xml_body = ['<?xml version="1.0" encoding="UTF-8"?>', "<CompleteMultipartUpload>"]
        for part in parts:
            etag = as_text(part.get("etag")).strip('"')
            xml_body.append("  <Part>")
            xml_body.append(f"    <PartNumber>{safe_int(part.get('part_number'), 0, minimum=0)}</PartNumber>")
            xml_body.append(f'    <ETag>"{etag}"</ETag>')
            xml_body.append("  </Part>")
        xml_body.append("</CompleteMultipartUpload>")
        xml_bytes = "\n".join(xml_body).encode("utf-8")
        content_md5 = base64.b64encode(hashlib.md5(xml_bytes).digest()).decode("utf-8")

        auth_key, oss_date = await self._get_upload_auth(
            task_id=task_id,
            method="POST",
            content_type="application/xml",
            upload_url=complete_oss_url,
            upload_id=upload_id,
            content_md5=content_md5,
            callback=self.callback,
        )
        if not auth_key or not oss_date:
            return False

        headers = {
            "Content-Type": "application/xml",
            "Content-MD5": content_md5,
            "Authorization": auth_key,
            "x-oss-date": oss_date,
            "x-oss-user-agent": QUARK_OSS_USER_AGENT,
            "Host": urlparse(complete_oss_url).hostname or "",
        }
        if self.callback and self.callback.get("callbackUrl") and self.callback.get("callbackBody"):
            callback_json = json.dumps(self.callback, ensure_ascii=False, separators=(",", ":"))
            headers["x-oss-callback"] = base64.b64encode(callback_json.encode("utf-8")).decode("utf-8")

        async with httpx.AsyncClient(timeout=180, verify=self.verify_ssl) as client:
            response = await client.post(
                f"{complete_oss_url}?uploadId={quote(upload_id)}",
                headers=headers,
                content=xml_bytes,
            )
            return response.status_code == 200

    async def _finish_upload(self, task_id: str, obj_key: str) -> dict[str, Any] | None:
        retry_delay = 2.0
        for _ in range(5):
            response = await self._request_json(
                "POST",
                f"{QUARK_BASE_URL}/file/upload/finish",
                payload={"task_id": task_id, "obj_key": obj_key},
            )
            data = response.get("data") or {}
            if quark_response_ok(response) and data.get("finish") is True:
                return data
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 10.0)
        return None

    async def upload_and_share(self, file_path: Path, share_title: str) -> TransferResult:
        if not self.cookie:
            return TransferResult(success=False, message="未配置夸克 Cookie")
        if not file_path.exists():
            return TransferResult(success=False, message=f"待上传文件不存在：{file_path}")

        file_size = file_path.stat().st_size
        file_name = safe_name(file_path.name, "activation.zip")
        content_type, _ = mimetypes.guess_type(file_name)
        content_type = content_type or "application/octet-stream"
        file_mtime = int(file_path.stat().st_mtime * 1000)
        file_ctime = int(file_path.stat().st_ctime * 1000)

        try:
            md5_hash, sha1_hash = await self._compute_file_hash(file_path)
        except Exception as exc:
            return TransferResult(success=False, message=f"计算激活包哈希失败：{exc}")

        try:
            pre_upload_response = await self._request_json(
                "POST",
                f"{QUARK_BASE_URL}/file/upload/pre",
                payload={
                    "ccp_hash_update": True,
                    "parallel_upload": True,
                    "pdir_fid": self.save_fid,
                    "dir_name": "",
                    "file_name": file_name,
                    "format_type": content_type,
                    "l_created_at": file_ctime,
                    "l_updated_at": file_mtime,
                    "size": file_size,
                },
            )
        except Exception as exc:
            return TransferResult(success=False, message=f"夸克预上传失败：{exc}")

        if not quark_response_ok(pre_upload_response):
            return TransferResult(success=False, message=quark_response_message(pre_upload_response, "夸克预上传失败"))

        pre_data = pre_upload_response.get("data") or {}
        metadata = pre_upload_response.get("metadata") or {}
        task_id = as_text(pre_data.get("task_id"))
        upload_id = as_text(pre_data.get("upload_id"))
        obj_key = as_text(pre_data.get("obj_key"))
        fid = as_text(pre_data.get("fid"))
        bucket = as_text(pre_data.get("bucket"))
        region = as_text(pre_data.get("region")) or "oss-cn-zhangjiakou"
        self.callback = pre_data.get("callback") or {}
        part_size = safe_int(metadata.get("part_size"), 4 * 1024 * 1024, minimum=1024 * 1024)

        if not task_id or not upload_id or not obj_key or not bucket:
            return TransferResult(success=False, message="夸克预上传返回缺少必要字段")

        complete_oss_url = f"https://{bucket}.{region}.aliyuncs.com/{obj_key}"

        try:
            fast_upload_data = await self._check_fast_upload(task_id, md5_hash, sha1_hash)
            if fast_upload_data:
                finish_result = await self._finish_upload(task_id, obj_key)
                if not finish_result:
                    return TransferResult(success=False, message="夸克秒传确认失败")
                final_fid = as_text(finish_result.get("fid")) or fid
            else:
                total_parts = max(1, math.ceil(file_size / part_size))
                parts: list[dict[str, Any]] = []
                async with httpx.AsyncClient(timeout=180, verify=self.verify_ssl) as client:
                    with file_path.open("rb") as file_obj:
                        for part_number in range(1, total_parts + 1):
                            part_data = file_obj.read(part_size)
                            if not part_data:
                                break
                            part_md5_b64 = base64.b64encode(hashlib.md5(part_data).digest()).decode("utf-8")
                            auth_key, oss_date = await self._get_upload_auth(
                                task_id=task_id,
                                method="PUT",
                                content_type=content_type,
                                upload_url=complete_oss_url,
                                upload_id=upload_id,
                                part_number=part_number,
                                content_md5=part_md5_b64,
                            )
                            if not auth_key or not oss_date:
                                return TransferResult(success=False, message=f"夸克分片 {part_number} 授权失败")

                            headers = {
                                "Content-Type": content_type,
                                "Content-Length": str(len(part_data)),
                                "Content-MD5": part_md5_b64,
                                "Authorization": auth_key,
                                "x-oss-date": oss_date,
                                "x-oss-user-agent": QUARK_OSS_USER_AGENT,
                                "Host": f"{bucket}.{region}.aliyuncs.com",
                            }

                            response = await client.put(
                                f"{complete_oss_url}?partNumber={part_number}&uploadId={quote(upload_id)}",
                                headers=headers,
                                content=part_data,
                            )
                            response.raise_for_status()
                            etag = as_text(response.headers.get("ETag")).strip('"')
                            if not etag:
                                return TransferResult(success=False, message=f"夸克分片 {part_number} 缺少 ETag")
                            parts.append({"part_number": part_number, "etag": etag})

                completed = await self._complete_multipart_upload(task_id, complete_oss_url, upload_id, parts)
                if not completed:
                    return TransferResult(success=False, message="夸克分片合并失败")

                finish_result = await self._finish_upload(task_id, obj_key)
                if not finish_result:
                    return TransferResult(success=False, message="夸克上传完成确认失败")
                final_fid = as_text(finish_result.get("fid")) or fid

            if not final_fid:
                return TransferResult(success=False, message="夸克未返回上传后的文件 ID")

            _, share_url, password = await self.create_share([final_fid], share_title)
            return TransferResult(
                success=True,
                title=share_title,
                share_url=share_url,
                password=password,
                cleanup_job={"provider": "quark", "payload": {"fids": [final_fid]}},
            )
        except RuntimeError as exc:
            return TransferResult(success=False, message=str(exc))
        except Exception as exc:
            logger.error(f"夸克上传失败：{exc}", exc_info=True)
            return TransferResult(success=False, message=f"夸克上传失败：{exc}")


@register(
    "lanzou_search",
    "Codex",
    "通过 OpenList 搜索已挂载目录中的资源并直接发送文件。",
    "1.4.0",
)
class LanzouSearchPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        runtime_root = Path("data") / "plugin_data" / "astrbot_plugin_lanzou_search"
        activation_root = runtime_root / "activation"
        self._client: Optional[httpx.AsyncClient] = None
        self._cleanup_task: Optional[asyncio.Task[Any]] = None
        self._activation_refresh_task: Optional[asyncio.Task[Any]] = None
        self._activation_prewarm_task: Optional[asyncio.Task[Any]] = None
        self._search_cache: dict[str, dict[str, Any]] = {}
        self._search_locks: dict[str, asyncio.Lock] = {}
        self._search_sessions: dict[str, dict[str, Any]] = {}
        self._group_search_threads: dict[str, dict[str, Any]] = {}
        self._group_search_renders: dict[str, dict[str, Any]] = {}
        self._selection_event_cache: dict[str, float] = {}
        self._pending_download_cleanup: dict[str, float] = {}
        self._file_link_cache: dict[str, dict[str, Any]] = {}
        self._activation_state: dict[str, Any] = {}
        self._activation_pending_state: dict[str, Any] = {}
        self._activation_users: dict[str, dict[str, Any]] = {}
        self._activation_requests: dict[str, dict[str, Any]] = {}
        self._activation_admin_origins: dict[str, dict[str, Any]] = {}
        self._runtime_root = runtime_root
        self._activation_root = activation_root
        self._activation_state_path = runtime_root / ACTIVATION_STATE_FILE
        self._activation_pending_state_path = runtime_root / ACTIVATION_PENDING_STATE_FILE
        self._activation_users_path = runtime_root / ACTIVATION_USERS_FILE
        self._activation_requests_path = runtime_root / ACTIVATION_REQUESTS_FILE
        self._activation_admin_origins_path = runtime_root / ACTIVATION_ADMIN_ORIGINS_FILE
        self._state_lock = asyncio.Lock()
        self._selection_event_lock = asyncio.Lock()
        self._file_link_cache_lock = asyncio.Lock()
        self._activation_lock = asyncio.Lock()
        self._activation_generation_lock = asyncio.Lock()
        self._file_prepare_condition = asyncio.Condition()
        self._file_prepare_next_ticket = 1
        self._file_prepare_current_ticket = 1
        self._file_prepare_cancelled_tickets: set[int] = set()
        self._file_prepare_active = 0
        self._file_prepare_last_finished_at = 0.0

    async def initialize(self) -> None:
        timeout_seconds = float(self._request_timeout_seconds())
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds, connect=min(10.0, timeout_seconds)),
            follow_redirects=True,
        )
        self._runtime_root.mkdir(parents=True, exist_ok=True)
        self._activation_root.mkdir(parents=True, exist_ok=True)
        self._downloads_root().mkdir(parents=True, exist_ok=True)
        self._activation_state = self._load_json_file(self._activation_state_path, default={})
        self._activation_pending_state = self._load_json_file(self._activation_pending_state_path, default={})
        self._activation_users = self._load_json_file(self._activation_users_path, default={})
        self._activation_requests = self._load_json_file(self._activation_requests_path, default={})
        self._activation_admin_origins = self._load_json_file(self._activation_admin_origins_path, default={})
        self._cleanup_download_dirs(force_all=True)
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        if self._activation_enabled():
            self._activation_refresh_task = asyncio.create_task(self._warmup_activation_package())

    async def terminate(self) -> None:
        if self._activation_prewarm_task is not None:
            self._activation_prewarm_task.cancel()
            try:
                await self._activation_prewarm_task
            except asyncio.CancelledError:
                pass
        if self._activation_refresh_task is not None:
            self._activation_refresh_task.cancel()
            try:
                await self._activation_refresh_task
            except asyncio.CancelledError:
                pass
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            await self._client.aclose()

    @filter.command(COMMAND_NAME)
    async def lanzou_search(self, event: AstrMessageEvent):
        await self._remember_admin_notify_origin(event)
        if not await self._ensure_group_activation_access(event):
            return
        keyword = extract_command_keyword(event.message_str, COMMAND_NAME)
        async for result in self._search_flow(event, keyword):
            yield result

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def handle_search_session_input(self, event: AstrMessageEvent):
        await self._remember_admin_notify_origin(event)
        text = (event.message_str or "").strip()
        if not text:
            return
        activation_code = parse_activation_command(text)
        if activation_code is not None:
            await self._handle_activation_command(event, activation_code)
            return
        if parse_search_command(text) is not None:
            return

        await self._prune_runtime_state()

        if self._is_group_chat(event):
            command = text.lower()
            is_numeric = bool(re.fullmatch(r"\d+", text))
            if command not in {"q", "退出", "n", "p"} and not is_numeric:
                return
            render, thread = await self._resolve_group_render_from_reply(event)
            if not render or not thread:
                return
            if not await self._ensure_group_activation_access(event):
                return
            await self._handle_group_search_input(event, text)
            return

        session_key = self._event_session_key(event)
        session = await self._get_private_search_session(session_key)
        if not session:
            return
        if str(session.get("sender_id", "")) != self._event_sender_id(event):
            return

        command = text.lower()
        is_numeric = bool(re.fullmatch(r"\d+", text))
        if command not in {"q", "退出", "n", "p"} and not is_numeric:
            return

        if not await self._consume_selection_event(event, session_key):
            event.stop_event()
            return

        if command in {"q", "退出"}:
            closed = await self._close_private_search_session(session_key, self._event_sender_id(event))
            if closed:
                await event.send(event.plain_result("当前检索会话已结束。"))
                event.stop_event()
            return

        if command in {"n", "p"}:
            active_session = await self._begin_private_search_action(session_key, self._event_sender_id(event))
            if not active_session:
                event.stop_event()
                return

            token = str(active_session.get("token", ""))
            try:
                current_page = safe_int(active_session.get("current_page"), 1, minimum=1)
                target_page = self._target_page_for_command(
                    current_page=current_page,
                    command=command,
                    total=safe_int(active_session.get("total"), 0, minimum=0),
                    has_more=bool(active_session.get("has_more", False)),
                )
                if target_page == current_page:
                    event.stop_event()
                    return
                page_state = await self._search_page(str(active_session.get("keyword", "")), target_page)
                updated = await self._replace_private_search_page(session_key, token, page_state)
                if updated:
                    await event.send(event.plain_result(self._render_private_page(updated)))
                    event.stop_event()
            except Exception as exc:
                logger.exception("private search page failed")
                await event.send(event.plain_result(f"处理失败：{exc}"))
                event.stop_event()
            finally:
                await self._finish_private_search_action(session_key, token)
            return

        active_session = await self._touch_private_search_session(session_key, self._event_sender_id(event))
        if not active_session:
            event.stop_event()
            return

        try:
            index = int(text)
            items = list(active_session.get("items") or [])
            if index < 1 or index > len(items):
                event.stop_event()
                return
            await event.send(event.plain_result("文件正在调取中，请稍后"))
            await self._send_file_result(event, items[index - 1])
            event.stop_event()
        finally:
            await self._touch_private_search_session(session_key, self._event_sender_id(event))

    async def _search_flow(self, event: AstrMessageEvent, keyword: str):
        keyword = keyword.strip()
        if not keyword:
            yield event.plain_result(
                "用法：sh 关键词\n"
                "私聊直接发送序号 / n / p / q，群里请引用结果消息后再发送序号 / n / p / q。"
            )
            event.stop_event()
            return

        await self._prune_runtime_state()

        try:
            self._ensure_required_config()
        except Exception as exc:
            yield event.plain_result(f"启动检索失败：{exc}")
            event.stop_event()
            return

        yield event.plain_result(f"正在检索“{keyword}”，请稍后...")

        try:
            page_state = await self._search_page(keyword, 1)
        except Exception as exc:
            logger.exception("lanzou search failed")
            yield event.plain_result(f"启动检索失败：{exc}")
            event.stop_event()
            return

        if page_state.total <= 0:
            yield event.plain_result(f"没有找到包含“{keyword}”的资源。")
            event.stop_event()
            return

        if self._is_group_chat(event):
            thread = await self._create_group_search_thread(event, page_state)
            _, body = await self._build_group_render_page(thread, page_state)
            if body:
                yield self._group_results_chain(event, body)
            event.stop_event()
            return

        session_key = self._event_session_key(event)
        session = {
            "token": uuid4().hex,
            "session_key": session_key,
            "sender_id": self._event_sender_id(event),
            "keyword": keyword,
            "total": page_state.total,
            "loaded_count": page_state.loaded_count,
            "has_more": page_state.has_more,
            "total_exact": page_state.total_exact,
            "current_page": page_state.page,
            "items": page_state.items,
            "busy": False,
            "updated_at": time.time(),
        }
        await self._set_private_search_session(session_key, session)
        yield event.plain_result(self._render_private_page(session))
        event.stop_event()

    async def _handle_group_search_input(self, event: AstrMessageEvent, text: str) -> None:
        command = text.lower()
        is_numeric = bool(re.fullmatch(r"\d+", text))
        if command not in {"q", "退出", "n", "p"} and not is_numeric:
            return

        group_key = self._event_group_id(event) or self._event_sender_scope(event)
        if not await self._consume_selection_event(event, group_key):
            event.stop_event()
            return

        render, thread = await self._resolve_group_render_from_reply(event)
        if not render or not thread:
            return

        search_id = str(thread.get("id", ""))
        if command in {"q", "退出"}:
            closed = await self._close_group_search_thread(search_id)
            if closed:
                await event.send(event.plain_result("当前检索会话已结束。"))
                event.stop_event()
            return

        if command in {"n", "p"}:
            thread_for_page = await self._begin_group_search_action(render)
            if not thread_for_page:
                event.stop_event()
                return

            try:
                current_page = safe_int(render.get("page"), 1, minimum=1)
                target_page = self._target_page_for_command(
                    current_page=current_page,
                    command=command,
                    total=safe_int(thread_for_page.get("total"), 0, minimum=0),
                    has_more=bool(thread_for_page.get("has_more", False)),
                )
                if target_page == current_page:
                    event.stop_event()
                    return
                page_state = await self._search_page(str(thread_for_page.get("keyword", "")), target_page)
                updated_thread = await self._replace_group_thread_page(search_id, page_state)
                if not updated_thread:
                    event.stop_event()
                    return
                _, body = await self._build_group_render_page(updated_thread, page_state)
                if body:
                    await event.send(self._group_results_chain(event, body))
                    event.stop_event()
            except Exception as exc:
                logger.exception("group search page failed")
                await event.send(event.plain_result(f"处理失败：{exc}"))
                event.stop_event()
            finally:
                await self._finish_group_search_action(search_id)
            return

        thread_for_send = await self._touch_group_search_thread(render)
        if not thread_for_send:
            event.stop_event()
            return

        try:
            index = int(text)
            items = list(render.get("items") or [])
            if index < 1 or index > len(items):
                event.stop_event()
                return
            await event.send(event.plain_result("文件正在调取中，请稍后"))
            await self._send_file_result(event, items[index - 1])
            event.stop_event()
        finally:
            await self._touch_group_search_thread(render)

    async def _search_page(self, keyword: str, page: int) -> SearchPage:
        state = await self._search_all(keyword, page)
        results = list(state.get("items") or [])
        loaded_count = len(results)
        total_exact = bool(state.get("complete", False))
        total = loaded_count
        current_page = min(max(1, page), total_pages(total))
        start = (current_page - 1) * PAGE_SIZE
        return SearchPage(
            keyword=keyword,
            page=current_page,
            total=total,
            items=results[start : start + PAGE_SIZE],
            loaded_count=loaded_count,
            has_more=not total_exact,
            total_exact=total_exact,
        )

    async def _search_all(self, keyword: str, page: int) -> dict[str, Any]:
        sources = self._sources()
        cache_key = self._search_cache_key(keyword, sources)
        required_items = page * PAGE_SIZE
        preload_target = self._search_prefetch_result_count()

        async with await self._get_search_lock(cache_key):
            cached_state = self._search_cache.get(cache_key)
            state = cached_state if isinstance(cached_state, dict) else None
            if not state or float(state.get("expires_at", 0) or 0) <= time.time():
                state = {
                    "expires_at": 0.0,
                    "items": [],
                    "seen_paths": set(),
                    "source_index": 0,
                    "next_page": 1,
                    "complete": False,
                }

            self._touch_search_state(state)

            if bool(state.get("complete", False)) or len(state.get("items") or []) >= required_items:
                self._search_cache[cache_key] = state
                return self._public_search_state(state)

            client = self._build_openlist_client()
            first_error: Exception | None = None
            success_count = 0
            target_items = max(required_items, len(state.get("items") or []) + preload_target)

            while not bool(state.get("complete", False)) and len(state.get("items") or []) < target_items:
                source_index = safe_int(state.get("source_index"), 0, minimum=0)
                if source_index >= len(sources):
                    state["complete"] = True
                    break

                source = sources[source_index]
                page_number = safe_int(state.get("next_page"), 1, minimum=1)

                try:
                    content, total, parent = await self._search_source_page(client, source, keyword, page_number)
                    success_count += 1
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                    logger.warning("source search failed for %s page %s: %s", source.mount_path, page_number, exc)
                    state["source_index"] = source_index + 1
                    state["next_page"] = 1
                    continue

                for raw in content:
                    name = str(raw.get("name", "") or "")
                    if not name or bool(raw.get("is_dir", False)):
                        continue
                    item_parent = normalize_path(str(raw.get("parent", parent) or parent))
                    full_path = normalize_path(posixpath.join(item_parent, name))
                    seen_paths = state.get("seen_paths")
                    if isinstance(seen_paths, set) and full_path in seen_paths:
                        continue
                    if isinstance(seen_paths, set):
                        seen_paths.add(full_path)
                    state.setdefault("items", []).append(
                        SearchItem(
                            name=name,
                            full_path=full_path,
                            size=safe_int(raw.get("size"), 0, minimum=0),
                        )
                    )

                if len(content) < self._openlist_search_fetch_page_size() or page_number * self._openlist_search_fetch_page_size() >= total:
                    state["source_index"] = source_index + 1
                    state["next_page"] = 1
                else:
                    state["next_page"] = page_number + 1

                self._touch_search_state(state)

            if not state.get("items") and success_count == 0 and first_error is not None:
                raise first_error

            self._touch_search_state(state)
            self._search_cache[cache_key] = state
            return self._public_search_state(state)

    async def _search_source_page(
        self,
        client: OpenListClient,
        source: SourceConfig,
        keyword: str,
        page: int,
    ) -> tuple[list[dict[str, Any]], int, str]:
        per_page = self._openlist_search_fetch_page_size()
        parent = resolve_search_root(source.mount_path, source.search_path)
        response_data = await client.search(parent, keyword, page, per_page)
        wrapper = response_data.get("data", response_data)
        content = wrapper.get("content", [])
        total = safe_int(wrapper.get("total"), 0, minimum=0)
        return content if isinstance(content, list) else [], total, parent

    async def _send_file_result(self, event: AstrMessageEvent, item: SearchItem) -> None:
        temp_dir = self._downloads_root() / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        target_path = (temp_dir / sanitize_filename(item.name)).resolve()
        client = self._build_openlist_client()
        cleanup_scheduled = False
        slot_acquired = False

        try:
            queue_ahead = await self._acquire_file_prepare_slot()
            slot_acquired = True
            if queue_ahead > 0:
                await event.send(event.plain_result(f"当前调取较多，已进入队列，前方还有 {queue_ahead} 个"))
            await self._apply_file_prepare_interval()
            send_timeout_seconds = FILE_SEND_TOTAL_TIMEOUT_SECONDS
            if self._offline_fallback_path():
                send_timeout_seconds = max(send_timeout_seconds, self._offline_fallback_timeout_seconds() + 30)
            async with asyncio.timeout(send_timeout_seconds):
                resolved_path, download_url = await self._resolve_download_target(client, item.full_path)
                if await self._try_send_remote_file(event, item.name, download_url, resolved_path):
                    return
                if await self._try_send_group_remote_file(event, item.name, download_url, resolved_path):
                    return
                try:
                    await self._download_and_send_local_file(
                        event,
                        client,
                        item.name,
                        item.full_path,
                        resolved_path,
                        download_url,
                        target_path,
                    )
                except Exception as exc:
                    if not await self._try_send_via_offline_fallback(
                        event,
                        client,
                        item.name,
                        item.full_path,
                        resolved_path,
                        download_url,
                        target_path,
                    ):
                        raise exc
                self._schedule_download_dir_cleanup(temp_dir, DOWNLOAD_SEND_GRACE_SECONDS)
                cleanup_scheduled = True
        except FileRecordExpiredError:
            logger.info("search record expired for %s", item.full_path)
            await event.send(event.plain_result("当前记录已失效，请重新检索后再试。"))
        except TimeoutError:
            logger.warning("send file timed out for %s", item.full_path)
            await event.send(event.plain_result("当前调取较多，请稍后再试。"))
        except Exception:
            logger.exception("send file failed for %s", item.full_path)
            await event.send(event.plain_result("文件准备失败，请稍后再试。"))
        finally:
            if slot_acquired:
                await self._release_file_prepare_slot()
            if not cleanup_scheduled:
                self._pending_download_cleanup.pop(str(temp_dir.resolve()), None)
                shutil.rmtree(temp_dir, ignore_errors=True)

    async def _resolve_download_target(
        self,
        client: OpenListClient,
        full_path: str,
    ) -> tuple[str, str]:
        cached = await self._get_cached_file_link(full_path)
        if cached:
            if str(cached.get("status", "")) == "missing":
                raise FileRecordExpiredError(full_path)
            cached_url = str(cached.get("raw_url", "") or "").strip()
            cached_path = normalize_path(str(cached.get("resolved_path", full_path) or full_path))
            if cached_url:
                return cached_path, cached_url

        resolved_path = normalize_path(full_path)
        direct_target = await self._resolve_direct_download_target(client, resolved_path, refresh=True)
        if direct_target:
            direct_path, direct_url = direct_target
            preferred_url = await self._prefer_openlist_file_link(client, direct_path, direct_url)
            await self._cache_file_link(full_path, direct_path, preferred_url)
            if direct_path != resolved_path:
                await self._cache_file_link(direct_path, direct_path, preferred_url)
            return direct_path, preferred_url

        try:
            preferred_url = await self._prefer_openlist_file_link(client, resolved_path, "")
            if preferred_url:
                await self._cache_file_link(full_path, resolved_path, preferred_url)
                return resolved_path, preferred_url
            raw_url = await self._fetch_raw_url(client, resolved_path)
            await self._cache_file_link(full_path, resolved_path, raw_url)
            return resolved_path, raw_url
        except Exception as exc:
            if not self._is_object_not_found_error(exc):
                raise

        recovered = await self._recover_existing_path(client, resolved_path, refresh=True)
        recovered_path = recovered[0] if recovered else None
        if not recovered_path:
            await self._cache_missing_file_link(full_path)
            raise FileRecordExpiredError(full_path)

        recovered_direct_url = recovered[1] if recovered else ""
        if recovered_direct_url:
            preferred_url = await self._prefer_openlist_file_link(client, recovered_path, recovered_direct_url)
            await self._cache_file_link(full_path, recovered_path, preferred_url)
            if recovered_path != resolved_path:
                await self._cache_file_link(recovered_path, recovered_path, preferred_url)
            return recovered_path, preferred_url

        try:
            preferred_url = await self._prefer_openlist_file_link(client, recovered_path, "")
            if preferred_url:
                await self._cache_file_link(full_path, recovered_path, preferred_url)
                if recovered_path != resolved_path:
                    await self._cache_file_link(recovered_path, recovered_path, preferred_url)
                return recovered_path, preferred_url
            raw_url = await self._fetch_raw_url(client, recovered_path)
        except Exception as exc:
            if not self._is_object_not_found_error(exc):
                raise
            refreshed = await self._recover_existing_path(client, recovered_path, refresh=True)
            refreshed_path = refreshed[0] if refreshed else None
            if not refreshed_path:
                await self._cache_missing_file_link(full_path)
                raise FileRecordExpiredError(full_path) from exc
            recovered_path = refreshed_path
            refreshed_direct_url = refreshed[1] if refreshed else ""
            if refreshed_direct_url:
                preferred_url = await self._prefer_openlist_file_link(client, recovered_path, refreshed_direct_url)
                await self._cache_file_link(full_path, recovered_path, preferred_url)
                if recovered_path != resolved_path:
                    await self._cache_file_link(recovered_path, recovered_path, preferred_url)
                return recovered_path, preferred_url
            preferred_url = await self._prefer_openlist_file_link(client, recovered_path, "")
            if preferred_url:
                await self._cache_file_link(full_path, recovered_path, preferred_url)
                if recovered_path != resolved_path:
                    await self._cache_file_link(recovered_path, recovered_path, preferred_url)
                return recovered_path, preferred_url
            raw_url = await self._fetch_raw_url(client, recovered_path)
        await self._cache_file_link(full_path, recovered_path, raw_url)
        if recovered_path != resolved_path:
            await self._cache_file_link(recovered_path, recovered_path, raw_url)
        return recovered_path, raw_url

    async def _download_and_send_local_file(
        self,
        event: AstrMessageEvent,
        client: OpenListClient,
        display_name: str,
        cache_key: str,
        resolved_path: str,
        download_url: str,
        target_path: Path,
    ) -> None:
        self._remove_partial_file(target_path)
        try:
            await self._retry_file_operation(
                lambda: client.download(download_url, target_path),
                action=f"download file {resolved_path}",
            )
        except Exception as exc:
            if not self._is_download_link_refreshable_error(exc):
                raise
            await self._clear_cached_file_link(cache_key)
            if resolved_path != cache_key:
                await self._clear_cached_file_link(resolved_path)
            resolved_path, download_url = await self._resolve_download_target(client, cache_key)
            self._remove_partial_file(target_path)
            await self._retry_file_operation(
                lambda: client.download(download_url, target_path),
                action=f"download file {resolved_path}",
            )
        if await self._try_upload_group_file_stream(event, display_name, target_path):
            return
        if await self._try_upload_group_file(event, display_name, target_path):
            return
        if await self._try_send_private_file_fallback(event, display_name, target_path, download_url):
            await event.send(event.plain_result("群内直发失败，已改为私发，请查收"))
            return
        await event.send(event.chain_result([Comp.File(name=display_name, file=str(target_path))]))

    async def _prefer_openlist_file_link(
        self,
        client: OpenListClient,
        resolved_path: str,
        fallback_url: str,
    ) -> str:
        copied_link_url = await self._fetch_openlist_file_link(client, resolved_path)
        if copied_link_url:
            return self._rewrite_download_url_base(copied_link_url)
        return fallback_url

    async def _fetch_openlist_file_link(self, client: OpenListClient, full_path: str) -> str:
        try:
            data = await self._retry_file_operation(
                lambda: client.get_link(full_path),
                action=f"get file link api {full_path}",
                attempts=2,
            )
        except Exception as exc:
            if self._is_permission_denied_error(exc):
                return ""
            if self._is_object_not_found_error(exc):
                raise
            logger.warning("openlist fs/link failed for %s: %s", full_path, exc)
            return ""
        wrapper = data.get("data", data)
        link_url = str(wrapper.get("url", "") or "").strip() if isinstance(wrapper, dict) else ""
        return link_url

    async def _try_send_via_offline_fallback(
        self,
        event: AstrMessageEvent,
        client: OpenListClient,
        display_name: str,
        cache_key: str,
        resolved_path: str,
        download_url: str,
        target_path: Path,
    ) -> bool:
        fallback_root = self._offline_fallback_path()
        if not fallback_root:
            return False

        task_dir = normalize_path(posixpath.join(fallback_root, f"astrbot_lanzou_search_{uuid4().hex}"))
        source_url = await self._resolve_offline_fallback_source_url(client, resolved_path, download_url)
        created_dir = False
        try:
            await self._retry_file_operation(
                lambda: client.mkdir(task_dir),
                action=f"mkdir offline fallback dir {task_dir}",
                attempts=2,
            )
            created_dir = True
            task_id = await self._start_offline_fallback_task(client, source_url, task_dir)
            if task_id:
                await self._wait_offline_download_task(client, task_id)
            fallback_path, fallback_url = await self._wait_offline_fallback_output(client, task_dir)
            await self._download_and_send_local_file(
                event,
                client,
                display_name,
                fallback_path,
                fallback_path,
                fallback_url,
                target_path,
            )
            return True
        except Exception:
            logger.exception("offline fallback failed for %s via %s", cache_key, task_dir)
            return False
        finally:
            if created_dir:
                await self._cleanup_offline_fallback_dir(client, task_dir)

    async def _resolve_offline_fallback_source_url(
        self,
        client: OpenListClient,
        resolved_path: str,
        current_download_url: str,
    ) -> str:
        direct_target = await self._resolve_direct_download_target(client, resolved_path, refresh=True)
        if direct_target:
            return direct_target[1]
        return current_download_url

    async def _start_offline_fallback_task(
        self,
        client: OpenListClient,
        download_url: str,
        task_dir: str,
    ) -> str:
        response = await self._retry_file_operation(
            lambda: client.add_offline_download(
                [download_url],
                task_dir,
                tool="SimpleHttp",
                delete_policy="upload_download_stream",
            ),
            action=f"create offline download for {task_dir}",
            attempts=2,
        )
        wrapper = response.get("data", response)
        tasks = wrapper.get("tasks", []) if isinstance(wrapper, dict) else []
        if not isinstance(tasks, list) or not tasks:
            return ""
        task_id = str((tasks[0] or {}).get("id", "")).strip()
        return task_id

    async def _wait_offline_download_task(self, client: OpenListClient, task_id: str) -> None:
        deadline = time.monotonic() + self._offline_fallback_timeout_seconds()
        while time.monotonic() < deadline:
            response = await self._retry_file_operation(
                lambda: client.get_task_info("offline_download", task_id),
                action=f"poll offline download task {task_id}",
                attempts=2,
            )
            wrapper = response.get("data", response)
            if not isinstance(wrapper, dict):
                await asyncio.sleep(OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS)
                continue
            error_text = str(wrapper.get("error", "") or "").strip()
            end_time = wrapper.get("end_time")
            if error_text:
                raise RuntimeError(error_text)
            if end_time:
                return
            await asyncio.sleep(OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS)
        raise TimeoutError(f"offline fallback task timed out: {task_id}")

    async def _wait_offline_fallback_output(
        self,
        client: OpenListClient,
        task_dir: str,
    ) -> tuple[str, str]:
        deadline = time.monotonic() + self._offline_fallback_timeout_seconds()
        while time.monotonic() < deadline:
            data = await self._retry_file_operation(
                lambda: client.list_dir(task_dir, 1, 20, refresh=True),
                action=f"list offline fallback dir {task_dir}",
                attempts=2,
            )
            wrapper = data.get("data", data)
            content = wrapper.get("content", [])
            for raw in (content if isinstance(content, list) else []):
                if bool(raw.get("is_dir", False)):
                    continue
                candidate_name = str(raw.get("name", "") or "")
                if not candidate_name:
                    continue
                resolved_path = normalize_path(posixpath.join(task_dir, candidate_name))
                sign = str(raw.get("sign", "") or "").strip()
                return resolved_path, self._build_openlist_download_url(resolved_path, sign)
            await asyncio.sleep(OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS)
        raise TimeoutError(f"offline fallback output timed out: {task_dir}")

    async def _cleanup_offline_fallback_dir(self, client: OpenListClient, task_dir: str) -> None:
        parent_path, name = posixpath.split(normalize_path(task_dir))
        parent_path = normalize_path(parent_path or "/")
        if not name:
            return
        try:
            await self._retry_file_operation(
                lambda: client.remove(parent_path, [name]),
                action=f"cleanup offline fallback dir {task_dir}",
                attempts=2,
            )
        except Exception:
            logger.warning("cleanup offline fallback dir failed: %s", task_dir, exc_info=True)

    async def _fetch_raw_url(self, client: OpenListClient, full_path: str) -> str:
        data = await self._retry_file_operation(
            lambda: client.get_file(full_path),
            action=f"get file link {full_path}",
        )
        wrapper = data.get("data", data)
        raw_url = str(wrapper.get("raw_url", "") or "").strip()
        if not raw_url:
            raise RuntimeError("OpenList did not return raw_url")
        return raw_url

    async def _recover_existing_path(
        self,
        client: OpenListClient,
        full_path: str,
        *,
        refresh: bool,
    ) -> tuple[str, str] | None:
        normalized_path = normalize_path(full_path)
        parent_path, name = posixpath.split(normalized_path)
        parent_path = normalize_path(parent_path or "/")
        if not name:
            return None
        try:
            match = await self._find_child_in_parent(client, parent_path, name, refresh=refresh)
            if not match:
                return None
            return match
        except Exception as exc:
            if self._is_object_not_found_error(exc):
                return None
            raise

    async def _resolve_direct_download_target(
        self,
        client: OpenListClient,
        full_path: str,
        *,
        refresh: bool,
    ) -> tuple[str, str] | None:
        recovered = await self._recover_existing_path(client, full_path, refresh=refresh)
        if not recovered:
            return None
        return recovered

    async def _find_child_in_parent(
        self,
        client: OpenListClient,
        parent_path: str,
        name: str,
        *,
        refresh: bool,
    ) -> tuple[str, str] | None:
        page_size = self._file_parent_scan_page_size()
        max_pages = self._file_parent_scan_max_pages()
        page = 1
        expected_name = name.casefold()
        while page <= max_pages:
            data = await self._retry_file_operation(
                lambda: client.list_dir(parent_path, page, page_size, refresh=refresh and page == 1),
                action=f"list parent {parent_path}",
                attempts=2,
            )
            wrapper = data.get("data", data)
            content = wrapper.get("content", [])
            total = safe_int(wrapper.get("total"), 0, minimum=0)
            for raw in (content if isinstance(content, list) else []):
                candidate_name = str(raw.get("name", "") or "")
                if not candidate_name or bool(raw.get("is_dir", False)):
                    continue
                if candidate_name == name or candidate_name.casefold() == expected_name:
                    resolved_path = normalize_path(posixpath.join(parent_path, candidate_name))
                    sign = str(raw.get("sign", "") or "").strip()
                    return resolved_path, self._build_openlist_download_url(resolved_path, sign)
            if not isinstance(content, list) or len(content) < page_size:
                return None
            if total > 0 and page * page_size >= total:
                return None
            page += 1
        return None

    def _build_openlist_download_url(self, full_path: str, sign_value: str = "") -> str:
        encoded_path = encode_openlist_path(full_path)
        url = f"{self._openlist_download_base_url()}/d{encoded_path}"
        sign_text = str(sign_value or "").strip()
        if sign_text:
            return f"{url}?sign={sign_text}"
        return url

    def _rewrite_download_url_base(self, url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            return ""
        target_base = self._openlist_download_base_url()
        if not target_base:
            return raw
        try:
            current = urlsplit(raw)
            if not current.scheme or not current.netloc:
                return raw
            source_base = urlsplit(self._openlist_base_url())
            if current.scheme != source_base.scheme or current.netloc != source_base.netloc:
                return raw
            desired = urlsplit(target_base)
            return urlunsplit((desired.scheme, desired.netloc, current.path, current.query, current.fragment))
        except Exception:
            return raw

    async def _get_cached_file_link(self, full_path: str) -> dict[str, Any] | None:
        cache_key = normalize_path(full_path)
        async with self._file_link_cache_lock:
            cached = self._file_link_cache.get(cache_key)
            if not cached:
                return None
            if float(cached.get("expires_at", 0) or 0) <= time.time():
                self._file_link_cache.pop(cache_key, None)
                return None
            return dict(cached)

    async def _cache_file_link(self, cache_key: str, resolved_path: str, raw_url: str) -> None:
        async with self._file_link_cache_lock:
            self._file_link_cache[normalize_path(cache_key)] = {
                "status": "ready",
                "resolved_path": normalize_path(resolved_path),
                "raw_url": str(raw_url or "").strip(),
                "expires_at": time.time() + FILE_LINK_CACHE_TTL_SECONDS,
            }

    async def _cache_missing_file_link(self, cache_key: str) -> None:
        async with self._file_link_cache_lock:
            self._file_link_cache[normalize_path(cache_key)] = {
                "status": "missing",
                "resolved_path": normalize_path(cache_key),
                "raw_url": "",
                "expires_at": time.time() + FILE_MISSING_CACHE_TTL_SECONDS,
            }

    async def _clear_cached_file_link(self, cache_key: str) -> None:
        async with self._file_link_cache_lock:
            self._file_link_cache.pop(normalize_path(cache_key), None)

    def _is_object_not_found_error(self, exc: Exception) -> bool:
        return "object not found" in str(exc or "").strip().lower()

    def _is_permission_denied_error(self, exc: Exception) -> bool:
        text = str(exc or "").strip().lower()
        return "permission denied" in text or "403" in text

    def _is_download_link_refreshable_error(self, exc: Exception) -> bool:
        text = str(exc or "").strip().lower()
        return "http 401" in text or "http 403" in text

    def _prune_cancelled_file_prepare_tickets_locked(self) -> None:
        while not self._file_prepare_active and self._file_prepare_current_ticket in self._file_prepare_cancelled_tickets:
            self._file_prepare_cancelled_tickets.discard(self._file_prepare_current_ticket)
            self._file_prepare_current_ticket += 1

    def _file_prepare_queue_ahead_locked(self, ticket: int) -> int:
        ahead = 0
        for current in range(self._file_prepare_current_ticket, ticket):
            if current in self._file_prepare_cancelled_tickets:
                continue
            ahead += 1
        return ahead

    async def _acquire_file_prepare_slot(self) -> int:
        async with self._file_prepare_condition:
            self._prune_cancelled_file_prepare_tickets_locked()
            ticket = self._file_prepare_next_ticket
            self._file_prepare_next_ticket += 1
            queue_ahead = self._file_prepare_queue_ahead_locked(ticket)

            while True:
                self._prune_cancelled_file_prepare_tickets_locked()
                if ticket == self._file_prepare_current_ticket and not self._file_prepare_active:
                    self._file_prepare_active = 1
                    return queue_ahead
                try:
                    await self._file_prepare_condition.wait()
                except asyncio.CancelledError:
                    self._file_prepare_cancelled_tickets.add(ticket)
                    self._prune_cancelled_file_prepare_tickets_locked()
                    self._file_prepare_condition.notify_all()
                    raise

    async def _release_file_prepare_slot(self) -> None:
        async with self._file_prepare_condition:
            self._file_prepare_active = 0
            self._file_prepare_last_finished_at = time.monotonic()
            self._file_prepare_current_ticket += 1
            self._prune_cancelled_file_prepare_tickets_locked()
            self._file_prepare_condition.notify_all()

    async def _apply_file_prepare_interval(self) -> None:
        interval_seconds = self._file_prepare_interval_seconds()
        if interval_seconds <= 0:
            return
        async with self._file_prepare_condition:
            last_finished_at = self._file_prepare_last_finished_at
        if last_finished_at <= 0:
            return
        wait_seconds = last_finished_at + interval_seconds - time.monotonic()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)

    async def _try_send_remote_file(
        self,
        event: AstrMessageEvent,
        name: str,
        download_url: str,
        resolved_path: str,
    ) -> bool:
        if self._is_group_chat(event):
            return False
        try:
            await event.send(event.chain_result([Comp.File(name=name, url=download_url)]))
            return True
        except Exception:
            logger.exception("send remote file failed for %s", resolved_path)
            return False

    async def _try_send_group_remote_file(
        self,
        event: AstrMessageEvent,
        name: str,
        download_url: str,
        resolved_path: str,
    ) -> bool:
        if not self._is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        group_id = self._event_group_id(event)
        remote_url = str(download_url or "").strip()
        if not group_id or not remote_url:
            return False
        try:
            await call_action(
                "send_group_msg",
                group_id=group_id,
                message=[{"type": "file", "data": {"file": remote_url, "name": name}}],
            )
            return True
        except Exception:
            logger.exception("send_group_msg remote file failed for %s via %s", resolved_path, remote_url)
            return False

    async def _try_upload_group_file(self, event: AstrMessageEvent, name: str, target_path: Path) -> bool:
        if not self._is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        group_id = self._event_group_id(event)
        if not group_id or not target_path.exists():
            return False
        try:
            await call_action("upload_group_file", group_id=group_id, file=str(target_path), name=name)
            return True
        except Exception:
            logger.exception("upload_group_file failed for %s", target_path)
            return False

    def _private_file_resource_variants(self, target_path: Path, download_url: str) -> list[str]:
        variants: list[str] = []
        local_path = str(target_path)
        if local_path:
            variants.append(local_path)
            variants.append(f"file://{local_path}")
        remote_url = str(download_url or "").strip()
        if remote_url:
            variants.append(remote_url)
        deduped: list[str] = []
        for item in variants:
            if item and item not in deduped:
                deduped.append(item)
        return deduped

    async def _try_send_private_file_fallback(
        self,
        event: AstrMessageEvent,
        name: str,
        target_path: Path,
        download_url: str,
    ) -> bool:
        if not self._is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        user_id = self._event_sender_id(event)
        if not user_id:
            return False
        for resource in self._private_file_resource_variants(target_path, download_url):
            try:
                await call_action(
                    "send_private_msg",
                    user_id=user_id,
                    message=[{"type": "file", "data": {"file": resource, "name": name}}],
                )
                return True
            except Exception:
                logger.exception("send_private_msg file fallback failed for %s via %s", name, resource)
        return False

    async def _try_send_group_file_resource(self, event: AstrMessageEvent, name: str, resource: str) -> bool:
        if not self._is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        group_id = self._event_group_id(event)
        if not group_id:
            return False
        try:
            await call_action(
                "send_group_msg",
                group_id=group_id,
                message=[{"type": "file", "data": {"file": resource, "name": name}}],
            )
            return True
        except Exception:
            logger.exception("send_group_msg file resource failed for %s via %s", name, resource)
            return False

    def _napcat_stream_resource_variants(self, stream_path: str) -> list[str]:
        raw_value = str(stream_path or "").strip()
        if not raw_value:
            return []
        variants: list[str] = []
        if raw_value.startswith("file://"):
            variants.append(raw_value)
        else:
            variants.append(f"file://{raw_value}")
            variants.append(raw_value)
        deduped: list[str] = []
        for item in variants:
            if item and item not in deduped:
                deduped.append(item)
        return deduped

    async def _try_upload_group_file_stream(self, event: AstrMessageEvent, name: str, target_path: Path) -> bool:
        if not self._is_group_chat(event):
            return False
        if not self._napcat_group_file_stream_enabled():
            return False
        if not target_path.exists():
            return False

        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False

        group_id = self._event_group_id(event)
        if not group_id:
            return False

        try:
            stream_path = await self._select_napcat_stream_client(call_action).upload_file(target_path)
        except Exception:
            logger.exception("upload_file_stream failed for %s", target_path)
            return False

        for resource in self._napcat_stream_resource_variants(stream_path):
            if await self._try_send_group_file_resource(event, name, resource):
                return True

        try:
            await call_action("upload_group_file", group_id=group_id, file=stream_path, name=name)
            return True
        except Exception:
            logger.exception("upload_group_file via stream temp failed for %s", target_path)
            return False

    async def _retry_file_operation(
        self,
        operation: Any,
        *,
        action: str,
        attempts: int = FILE_PREPARE_RETRY_COUNT,
    ) -> Any:
        last_error: Exception | None = None
        total_attempts = max(1, attempts)
        for attempt in range(1, total_attempts + 1):
            try:
                return await operation()
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                last_error = exc
                if attempt >= total_attempts:
                    break
                delay = FILE_PREPARE_RETRY_DELAY_SECONDS * attempt
                logger.warning("%s failed on attempt %s/%s, retrying in %.1fs: %s", action, attempt, total_attempts, delay, exc)
                await asyncio.sleep(delay)
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"{action} failed")

    def _remove_partial_file(self, path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass

    def _schedule_download_dir_cleanup(self, path: Path, delay_seconds: int) -> None:
        self._pending_download_cleanup[str(path.resolve())] = time.time() + max(15, int(delay_seconds))

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cleanup_interval_seconds())
            await self._prune_runtime_state()
            self._cleanup_download_dirs(force_all=False)
            if self._activation_enabled() and self._activation_auto_rotate_enabled():
                try:
                    await self._ensure_activation_package_ready()
                except Exception as exc:
                    logger.warning(f"激活包轮换检查失败：{exc}")

    async def _prune_runtime_state(self) -> None:
        expire_before = time.time() - self._session_timeout_seconds()
        dedup_now = time.monotonic()

        async with self._state_lock:
            self._search_sessions = {
                key: value
                for key, value in self._search_sessions.items()
                if float(value.get("updated_at", 0) or 0) > expire_before
            }
            self._group_search_threads = {
                key: value
                for key, value in self._group_search_threads.items()
                if float(value.get("updated_at", 0) or 0) > expire_before
            }
            active_search_ids = set(self._group_search_threads.keys())
            self._group_search_renders = {
                key: value
                for key, value in self._group_search_renders.items()
                if str(value.get("search_id", "")) in active_search_ids
                and float(value.get("created_at", 0) or 0) > expire_before
            }
            self._selection_event_cache = {
                key: value
                for key, value in self._selection_event_cache.items()
                if (dedup_now - value) < DEFAULT_SELECTION_DEDUP_WINDOW_SECONDS
            }
            self._search_cache = {
                key: value
                for key, value in self._search_cache.items()
                if isinstance(value, dict) and float(value.get("expires_at", 0) or 0) > time.time()
            }
        async with self._file_link_cache_lock:
            self._file_link_cache = {
                key: value
                for key, value in self._file_link_cache.items()
                if isinstance(value, dict) and float(value.get("expires_at", 0) or 0) > time.time()
            }
        async with self._activation_lock:
            current_batch_id = str(self._activation_state.get("batch_id", "") or "")
            if current_batch_id:
                pruned_users = {
                    key: value
                    for key, value in self._activation_users.items()
                    if isinstance(value, dict) and str(value.get("batch_id", "") or "") == current_batch_id
                }
                pruned_requests = {
                    key: value
                    for key, value in self._activation_requests.items()
                    if isinstance(value, dict)
                    and str(value.get("batch_id", "") or "") == current_batch_id
                    and float(value.get("expires_at", 0) or 0) > time.time()
                }
            else:
                pruned_users = {}
                pruned_requests = {}
            if pruned_users != self._activation_users:
                self._activation_users = pruned_users
                self._write_json_file(self._activation_users_path, self._activation_users)
            if pruned_requests != self._activation_requests:
                self._activation_requests = pruned_requests
                self._write_json_file(self._activation_requests_path, self._activation_requests)

    def _cleanup_download_dirs(self, *, force_all: bool) -> None:
        root = self._downloads_root()
        if not root.exists():
            return

        now = time.time()
        active_cleanup = self._pending_download_cleanup
        for child in root.iterdir():
            if not child.exists():
                continue
            child_key = str(child.resolve())
            if not child.is_dir():
                try:
                    child.unlink()
                except OSError:
                    pass
                active_cleanup.pop(child_key, None)
                continue
            scheduled_at = active_cleanup.get(child_key)
            should_delete = force_all
            if not should_delete and scheduled_at is not None:
                should_delete = now >= scheduled_at
            if not should_delete:
                should_delete = (now - child.stat().st_mtime) > DOWNLOAD_DIR_RETENTION_SECONDS
            if should_delete:
                active_cleanup.pop(child_key, None)
                shutil.rmtree(child, ignore_errors=True)

        stale_keys = [key for key in active_cleanup if not Path(key).exists()]
        for key in stale_keys:
            active_cleanup.pop(key, None)

    def _load_json_file(self, path: Path, *, default: Any) -> Any:
        try:
            if not path.exists():
                return default
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _write_json_file(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(path.name + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

    def _activation_enabled(self) -> bool:
        return safe_bool(self._cfg("activation", "enabled", default=False), False)

    def _activation_auto_rotate_enabled(self) -> bool:
        return safe_bool(self._cfg("activation", "auto_rotate_enabled", default=True), True)

    def _activation_rotate_hours(self) -> int:
        return max(1, min(720, self._cfg_int("activation", "rotate_hours", default=24)))

    def _activation_code_length(self) -> int:
        return max(4, min(16, self._cfg_int("activation", "code_length", default=8)))

    def _activation_quark_cookie(self) -> str:
        return self._cfg_str("activation", "quark_cookie", default="").strip()

    def _activation_quark_save_fid(self) -> str:
        return self._cfg_str("activation", "quark_save_fid", default="").strip()

    def _activation_quark_verify_ssl(self) -> bool:
        return safe_bool(self._cfg("activation", "quark_verify_ssl", default=True), True)

    def _activation_quark_share_expired_type(self) -> int:
        return max(1, min(4, self._cfg_int("activation", "quark_share_expired_type", default=1)))

    def _build_activation_quark_client(self) -> QuarkPanClient:
        cookie = self._activation_quark_cookie()
        if not cookie:
            raise RuntimeError("未配置 activation.quark_cookie")
        return QuarkPanClient(
            cookie=cookie,
            save_fid=self._activation_quark_save_fid(),
            timeout=max(30, self._request_timeout_seconds()),
            verify_ssl=self._activation_quark_verify_ssl(),
            share_expired_type=self._activation_quark_share_expired_type(),
        )

    def _activation_state_valid(self, state: dict[str, Any] | None = None) -> bool:
        target = state if isinstance(state, dict) else self._activation_state
        batch_id = str(target.get("batch_id", "") or "").strip()
        code = normalize_activation_code(str(target.get("code", "") or ""))
        share_url = str(target.get("share_url", "") or "").strip()
        expires_at = float(target.get("expires_at", 0) or 0)
        return bool(batch_id and code and share_url and expires_at > time.time())

    async def _is_user_activated(self, user_id: str) -> bool:
        if not user_id:
            return False
        async with self._activation_lock:
            if not self._activation_state_valid():
                return False
            batch_id = str(self._activation_state.get("batch_id", "") or "")
            record = self._activation_users.get(user_id) or {}
            return str(record.get("batch_id", "") or "") == batch_id

    async def _mark_user_activated(self, user_id: str) -> None:
        if not user_id:
            return
        async with self._activation_lock:
            batch_id = str(self._activation_state.get("batch_id", "") or "")
            if not batch_id:
                return
            self._activation_users[user_id] = {
                "batch_id": batch_id,
                "activated_at": time.time(),
            }
            self._write_json_file(self._activation_users_path, self._activation_users)

    def _build_activation_text(self, code: str, created_at: float, expires_at: float) -> str:
        created_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_at))
        expires_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expires_at))
        return (
            "激活说明\n"
            f"当前激活码：{code}\n"
            f"生成时间：{created_text}\n"
            f"失效时间：{expires_text}\n\n"
            "使用方法：\n"
            "1. 回到机器人聊天窗口\n"
            "2. 发送：激活 激活码\n"
            f"例如：激活 {code}\n"
        )

    async def _cleanup_previous_activation_upload(self, previous_state: dict[str, Any]) -> None:
        return

    async def _generate_activation_package_state(self) -> dict[str, Any]:
        created_at = time.time()
        expires_at = created_at + self._activation_rotate_hours() * 3600
        batch_id = time.strftime("%Y%m%d%H%M%S", time.localtime(created_at)) + "_" + uuid4().hex[:8]
        code = generate_activation_code(self._activation_code_length())
        share_title = f"激活包_{time.strftime('%Y%m%d_%H%M%S', time.localtime(created_at))}"
        work_dir = self._activation_root / batch_id
        zip_path = work_dir / f"{share_title}.zip"
        txt_path = work_dir / "激活码.txt"

        work_dir.mkdir(parents=True, exist_ok=True)
        try:
            txt_path.write_text(self._build_activation_text(code, created_at, expires_at), encoding="utf-8")
            append_fixed_activation_padding(txt_path)
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.write(txt_path, arcname="激活码.txt")

            result = await self._build_activation_quark_client().upload_and_share(zip_path, share_title)
            if not result.success:
                raise RuntimeError(result.message or "激活包上传失败")

            return {
                "batch_id": batch_id,
                "code": code,
                "created_at": created_at,
                "expires_at": expires_at,
                "share_url": result.share_url,
                "password": result.password,
                "title": result.title,
                "cleanup_job": result.cleanup_job or {},
            }
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    async def _ensure_activation_package_ready(self, force: bool = False) -> dict[str, Any]:
        if not self._activation_enabled():
            return {}

        async with self._activation_lock:
            current = dict(self._activation_state)
        if not force and self._activation_state_valid(current):
            return current

        async with self._activation_generation_lock:
            async with self._activation_lock:
                current = dict(self._activation_state)
            if not force and self._activation_state_valid(current):
                return current

            new_state = await self._generate_activation_package_state()
            previous_state = current
            async with self._activation_lock:
                self._activation_state = dict(new_state)
                self._activation_users = {}
                self._write_json_file(self._activation_state_path, self._activation_state)
                self._write_json_file(self._activation_users_path, self._activation_users)
            if previous_state:
                await self._cleanup_previous_activation_upload(previous_state)
            return dict(new_state)

    async def _warmup_activation_package(self) -> None:
        try:
            await self._ensure_activation_package_ready()
        except Exception as exc:
            logger.warning(f"激活包预热失败：{exc}")

    def _activation_prompt_text(self, state: dict[str, Any]) -> str:
        lines = [
            "尚未激活，请打开分享链接解压后获取激活码，然后私聊机器人发送：激活 激活码",
            f"链接：{str(state.get('share_url', '') or '').strip()}",
        ]
        return "\n".join(lines)

    async def _ensure_group_activation_access(self, event: AstrMessageEvent) -> bool:
        if not self._activation_enabled() or not self._is_group_chat(event):
            return True
        sender_id = self._event_sender_id(event)
        if await self._is_user_activated(sender_id):
            return True

        try:
            state = await self._ensure_activation_package_ready()
        except Exception as exc:
            logger.exception("activation package prepare failed")
            await event.send(
                event.chain_result(
                    [
                        Comp.At(qq=sender_id),
                        Comp.Plain(text=f"\n激活包准备失败，请稍后再试\n原因：{exc}"),
                    ]
                )
            )
            event.stop_event()
            return False

        await self._mark_private_activation_request(sender_id, self._event_group_id(event), state)
        await event.send(
            event.chain_result(
                [
                    Comp.At(qq=sender_id),
                    Comp.Plain(text=f"\n{self._activation_prompt_text(state)}"),
                ]
            )
        )
        event.stop_event()
        return False

    async def _handle_activation_command(self, event: AstrMessageEvent, code: str) -> None:
        if not self._activation_enabled():
            await event.send(event.plain_result("当前未开启激活"))
            event.stop_event()
            return

        if self._is_group_chat(event):
            await event.send(event.plain_result("请私聊机器人发送：激活 激活码"))
            event.stop_event()
            return

        await self._remember_admin_notify_origin(event)

        normalized_code = normalize_activation_code(code)
        if not normalized_code:
            await event.send(event.plain_result("用法：激活 激活码"))
            event.stop_event()
            return

        try:
            state = await self._ensure_activation_package_ready()
        except Exception as exc:
            logger.exception("activation package prepare failed")
            await event.send(event.plain_result(f"激活包准备失败，请稍后再试\n原因：{exc}"))
            event.stop_event()
            return

        if not self._is_admin_event(event):
            if not await self._can_activate_via_private(self._event_sender_id(event)):
                await event.send(event.plain_result("请先在群里使用一次，再私聊机器人发送：激活 激活码"))
                event.stop_event()
                return

        current_code = normalize_activation_code(str(state.get("code", "") or ""))
        if normalized_code != current_code:
            await event.send(event.plain_result("激活码不正确，请重新查看激活包"))
            event.stop_event()
            return

        sender_id = self._event_sender_id(event)
        await self._mark_user_activated(sender_id)
        await self._clear_private_activation_request(sender_id)
        await event.send(event.plain_result("激活成功！请返回群内使用！"))
        event.stop_event()

    def _event_sender_scope(self, event: AstrMessageEvent) -> str:
        sender_id = self._event_sender_id(event)
        group_id = self._event_group_id(event)
        if group_id:
            return f"group:{group_id}:user:{sender_id or 'unknown'}"
        return f"private:{sender_id or 'unknown'}"

    def _event_sender_id(self, event: AstrMessageEvent) -> str:
        try:
            return str(event.get_sender_id() or "").strip()
        except Exception:
            return ""

    def _event_group_id(self, event: AstrMessageEvent) -> str:
        try:
            return str(event.get_group_id() or "").strip()
        except Exception:
            return ""

    def _event_self_id(self, event: AstrMessageEvent) -> str:
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

    def _event_message_token(self, event: AstrMessageEvent) -> str:
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

    def _is_group_chat(self, event: AstrMessageEvent) -> bool:
        return bool(self._event_group_id(event))

    def _event_session_key(self, event: AstrMessageEvent) -> str:
        return self._event_sender_scope(event)

    def _event_message_components(self, event: AstrMessageEvent) -> list[Any]:
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

    def _event_reply_component(self, event: AstrMessageEvent) -> Any | None:
        for component in self._event_message_components(event):
            if isinstance(component, Comp.Reply):
                return component
        return None

    def _event_unified_msg_origin(self, event: AstrMessageEvent) -> str:
        try:
            return str(getattr(event, "unified_msg_origin", "") or "").strip()
        except Exception:
            return ""

    def _event_role(self, event: AstrMessageEvent) -> str:
        try:
            return str(getattr(event, "role", "") or "").strip().lower()
        except Exception:
            return ""

    def _is_admin_event(self, event: AstrMessageEvent) -> bool:
        return self._event_role(event) == "admin"

    def _reply_body_text(self, reply_component: Any) -> str:
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
        return normalize_session_body(str(getattr(reply_component, "message_str", "") or ""))

    async def _get_private_search_session(self, session_key: str) -> dict[str, Any] | None:
        async with self._state_lock:
            session = self._search_sessions.get(session_key)
            return dict(session) if session else None

    async def _set_private_search_session(self, session_key: str, session: dict[str, Any]) -> None:
        async with self._state_lock:
            self._search_sessions[session_key] = dict(session)

    async def _clear_private_search_session(self, session_key: str, token: str = "") -> None:
        async with self._state_lock:
            current = self._search_sessions.get(session_key)
            if current and token and str(current.get("token", "")) != token:
                return
            self._search_sessions.pop(session_key, None)

    async def _close_private_search_session(self, session_key: str, sender_id: str) -> bool:
        session = await self._get_private_search_session(session_key)
        if not session or str(session.get("sender_id", "")) != sender_id:
            return False
        await self._clear_private_search_session(session_key, str(session.get("token", "")))
        return True

    async def _begin_private_search_action(self, session_key: str, sender_id: str) -> dict[str, Any] | None:
        async with self._state_lock:
            session = self._search_sessions.get(session_key)
            if not session or str(session.get("sender_id", "")) != sender_id:
                return None
            if session.get("busy"):
                return None
            session["busy"] = True
            session["updated_at"] = time.time()
            self._search_sessions[session_key] = session
            return dict(session)

    async def _finish_private_search_action(self, session_key: str, token: str) -> None:
        async with self._state_lock:
            session = self._search_sessions.get(session_key)
            if not session or str(session.get("token", "")) != token:
                return
            session["busy"] = False
            session["updated_at"] = time.time()
            self._search_sessions[session_key] = session

    async def _touch_private_search_session(self, session_key: str, sender_id: str) -> dict[str, Any] | None:
        async with self._state_lock:
            session = self._search_sessions.get(session_key)
            if not session or str(session.get("sender_id", "")) != sender_id:
                return None
            session["updated_at"] = time.time()
            self._search_sessions[session_key] = session
            return dict(session)

    async def _replace_private_search_page(
        self,
        session_key: str,
        token: str,
        page_state: SearchPage,
    ) -> dict[str, Any] | None:
        async with self._state_lock:
            session = self._search_sessions.get(session_key)
            if not session or str(session.get("token", "")) != token:
                return None
            session["current_page"] = page_state.page
            session["total"] = page_state.total
            session["loaded_count"] = page_state.loaded_count
            session["has_more"] = page_state.has_more
            session["total_exact"] = page_state.total_exact
            session["items"] = page_state.items
            session["updated_at"] = time.time()
            self._search_sessions[session_key] = session
            return dict(session)

    async def _consume_selection_event(self, event: AstrMessageEvent, session_key: str) -> bool:
        message_token = self._event_message_token(event)
        if message_token:
            dedup_key = f"msg:{message_token}"
        else:
            dedup_key = f"fallback:{session_key}:{(event.message_str or '').strip()}"

        now = time.monotonic()
        async with self._selection_event_lock:
            self._selection_event_cache = {
                key: timestamp
                for key, timestamp in self._selection_event_cache.items()
                if (now - timestamp) < DEFAULT_SELECTION_DEDUP_WINDOW_SECONDS
            }
            if dedup_key in self._selection_event_cache:
                return False
            self._selection_event_cache[dedup_key] = now
            return True

    async def _create_group_search_thread(self, event: AstrMessageEvent, page_state: SearchPage) -> dict[str, Any]:
        thread = {
            "id": uuid4().hex,
            "sender_id": self._event_sender_id(event),
            "group_id": self._event_group_id(event),
            "keyword": page_state.keyword,
            "total": page_state.total,
            "loaded_count": page_state.loaded_count,
            "has_more": page_state.has_more,
            "total_exact": page_state.total_exact,
            "current_page": page_state.page,
            "items": page_state.items,
            "busy": False,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        async with self._state_lock:
            self._group_search_threads[str(thread["id"])] = thread
        return dict(thread)

    async def _get_group_search_thread(self, search_id: str) -> dict[str, Any] | None:
        async with self._state_lock:
            thread = self._group_search_threads.get(search_id)
            return dict(thread) if thread else None

    async def _store_group_render(self, thread: dict[str, Any], page_state: SearchPage, body: str) -> dict[str, Any]:
        render = {
            "id": uuid4().hex,
            "search_id": str(thread.get("id", "")),
            "sender_id": str(thread.get("sender_id", "")),
            "group_id": str(thread.get("group_id", "")),
            "page": page_state.page,
            "items": page_state.items,
            "body": normalize_session_body(body),
            "body_hash": hash_session_body(body),
            "created_at": time.time(),
        }
        async with self._state_lock:
            self._group_search_renders[str(render["id"])] = render
        return dict(render)

    async def _close_group_search_thread(self, search_id: str) -> bool:
        removed = False
        async with self._state_lock:
            if search_id in self._group_search_threads:
                removed = True
            self._group_search_threads.pop(search_id, None)
            self._group_search_renders = {
                key: value
                for key, value in self._group_search_renders.items()
                if str(value.get("search_id", "")) != search_id
            }
        return removed

    async def _begin_group_search_action(self, render: dict[str, Any]) -> dict[str, Any] | None:
        search_id = str(render.get("search_id", "")).strip()
        if not search_id:
            return None
        async with self._state_lock:
            thread = self._group_search_threads.get(search_id)
            if not thread or thread.get("busy"):
                return None
            thread["busy"] = True
            thread["updated_at"] = time.time()
            self._group_search_threads[search_id] = thread
            return dict(thread)

    async def _finish_group_search_action(self, search_id: str) -> None:
        async with self._state_lock:
            thread = self._group_search_threads.get(search_id)
            if not thread:
                return
            thread["busy"] = False
            thread["updated_at"] = time.time()
            self._group_search_threads[search_id] = thread

    async def _touch_group_search_thread(self, render: dict[str, Any]) -> dict[str, Any] | None:
        sender_id = str(render.get("sender_id", ""))
        group_id = str(render.get("group_id", ""))
        search_id = str(render.get("search_id", ""))
        async with self._state_lock:
            thread = self._group_search_threads.get(search_id)
            if not thread:
                return None
            if str(thread.get("sender_id", "")) != sender_id or str(thread.get("group_id", "")) != group_id:
                return None
            thread["updated_at"] = time.time()
            self._group_search_threads[search_id] = thread
            return dict(thread)

    async def _replace_group_thread_page(self, search_id: str, page_state: SearchPage) -> dict[str, Any] | None:
        async with self._state_lock:
            thread = self._group_search_threads.get(search_id)
            if not thread:
                return None
            thread["keyword"] = page_state.keyword
            thread["total"] = page_state.total
            thread["loaded_count"] = page_state.loaded_count
            thread["has_more"] = page_state.has_more
            thread["total_exact"] = page_state.total_exact
            thread["current_page"] = page_state.page
            thread["items"] = page_state.items
            thread["updated_at"] = time.time()
            self._group_search_threads[search_id] = thread
            return dict(thread)

    async def _resolve_group_render_from_reply(
        self,
        event: AstrMessageEvent,
    ) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
        reply_component = self._event_reply_component(event)
        if reply_component is None:
            return None, None

        self_id = self._event_self_id(event)
        reply_sender_id = str(getattr(reply_component, "sender_id", "") or "").strip()
        if self_id and reply_sender_id and self_id != reply_sender_id:
            return None, None

        group_id = self._event_group_id(event)
        sender_id = self._event_sender_id(event)
        if not group_id or not sender_id:
            return None, None

        reply_body = self._reply_body_text(reply_component)
        if not reply_body:
            return None, None
        reply_hash = hash_session_body(reply_body)
        reply_time = safe_int(getattr(reply_component, "time", 0), 0, minimum=0)

        async with self._state_lock:
            candidates = [
                dict(render)
                for render in self._group_search_renders.values()
                if str(render.get("group_id", "")) == group_id
                and str(render.get("sender_id", "")) == sender_id
                and str(render.get("body_hash", "")) == reply_hash
            ]

        if not candidates:
            return None, None

        if reply_time > 0:
            candidates.sort(key=lambda item: abs(safe_int(item.get("created_at"), 0, minimum=0) - reply_time))
        else:
            candidates.sort(key=lambda item: safe_int(item.get("created_at"), 0, minimum=0), reverse=True)

        for render in candidates:
            thread = await self._get_group_search_thread(str(render.get("search_id", "")))
            if not thread:
                continue
            if str(thread.get("sender_id", "")) != sender_id or str(thread.get("group_id", "")) != group_id:
                continue
            return render, thread

        return None, None

    async def _build_group_render_page(
        self,
        thread: dict[str, Any],
        page_state: SearchPage,
    ) -> tuple[dict[str, Any], str] | tuple[None, None]:
        body = format_search_results(
            str(thread.get("keyword", "")),
            page_state.page,
            page_state.total,
            page_state.items,
            require_reply=True,
            loaded_count=page_state.loaded_count,
            has_more=page_state.has_more,
            total_exact=page_state.total_exact,
        )
        render = await self._store_group_render(thread, page_state, body)
        return render, body

    def _render_private_page(self, session: dict[str, Any]) -> str:
        return format_search_results(
            str(session.get("keyword", "")),
            safe_int(session.get("current_page"), 1, minimum=1),
            safe_int(session.get("total"), 0, minimum=0),
            list(session.get("items") or []),
            require_reply=False,
            loaded_count=safe_int(session.get("loaded_count"), 0, minimum=0),
            has_more=bool(session.get("has_more", False)),
            total_exact=bool(session.get("total_exact", True)),
        )

    def _group_results_chain(self, event: AstrMessageEvent, body: str):
        return event.chain_result(
            [
                Comp.At(qq=self._event_sender_id(event)),
                Comp.Plain(text=f"\n{body}"),
            ]
        )

    def _search_cache_key(self, keyword: str, sources: list[SourceConfig]) -> str:
        signature = "|".join(f"{source.mount_path}>{source.search_path}" for source in sources)
        return hashlib.sha1(
            f"{self._openlist_base_url()}|{keyword}|{signature}|{self._openlist_search_fetch_page_size()}".encode("utf-8")
        ).hexdigest()

    def _target_page_for_command(self, *, current_page: int, command: str, total: int, has_more: bool) -> int:
        if command == "p":
            return max(1, current_page - 1)
        next_page = current_page + 1
        max_page = total_pages(total)
        if has_more:
            return next_page
        return min(next_page, max_page)

    def _search_prefetch_result_count(self) -> int:
        return max(20, 200)

    def _search_state_ttl_seconds(self) -> int:
        return max(self._search_cache_ttl_seconds(), self._session_timeout_seconds())

    def _touch_search_state(self, state: dict[str, Any]) -> None:
        state["expires_at"] = time.time() + self._search_state_ttl_seconds()

    def _public_search_state(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "items": list(state.get("items") or []),
            "complete": bool(state.get("complete", False)),
        }

    async def _get_search_lock(self, cache_key: str) -> asyncio.Lock:
        async with self._state_lock:
            lock = self._search_locks.get(cache_key)
            if lock is None:
                lock = asyncio.Lock()
                self._search_locks[cache_key] = lock
            return lock

    def _build_openlist_client(self) -> OpenListClient:
        return OpenListClient(
            self._require_client(),
            self._openlist_base_url(),
            token=self._cfg_str("openlist", "token", default="").strip(),
            username=self._cfg_str("openlist", "username", default="").strip(),
            password=self._cfg_str("openlist", "password", default=""),
            request_timeout_seconds=self._request_timeout_seconds(),
        )

    def _downloads_root(self) -> Path:
        return self._runtime_root / "downloads"

    def _openlist_base_url(self) -> str:
        return self._cfg_str("openlist", "base_url", default="http://host.docker.internal:5244").rstrip("/")

    def _openlist_download_base_url(self) -> str:
        configured = self._cfg_str("openlist", "download_base_url", default="").rstrip("/")
        return configured or self._openlist_base_url()

    def _request_timeout_seconds(self) -> int:
        return max(10, self._cfg_int("openlist", "request_timeout_seconds", default=60))

    def _offline_fallback_path(self) -> str:
        raw_value = self._cfg_str("openlist", "offline_fallback_path", default="").strip()
        return normalize_path(raw_value) if raw_value else ""

    def _offline_fallback_timeout_seconds(self) -> int:
        return max(30, min(1800, self._cfg_int("openlist", "offline_fallback_timeout_seconds", default=300)))

    def _napcat_group_file_stream_enabled(self) -> bool:
        return safe_bool(self._cfg("napcat", "group_file_stream_enabled", default=False), False)

    def _napcat_ws_url(self) -> str:
        return self._cfg_str("napcat", "ws_url", default="").strip()

    def _napcat_access_token(self) -> str:
        return self._cfg_str("napcat", "access_token", default="").strip()

    def _napcat_stream_chunk_size_bytes(self) -> int:
        kb = max(16, min(1024, self._cfg_int("napcat", "stream_chunk_size_kb", default=64)))
        return kb * 1024

    def _napcat_stream_file_retention_seconds(self) -> int:
        return max(10, min(600, self._cfg_int("napcat", "stream_file_retention_seconds", default=30)))

    def _build_napcat_stream_client(self, action_sender: Any) -> Any:
        ws_url = self._napcat_ws_url()
        if ws_url:
            raise RuntimeError("未配置 napcat.ws_url")
        return NapCatActionStreamClient(
            action_sender=action_sender,
            chunk_size_bytes=self._napcat_stream_chunk_size_bytes(),
            file_retention_seconds=self._napcat_stream_file_retention_seconds(),
        )

    def _select_napcat_stream_client(self, action_sender: Any) -> Any:
        ws_url = self._napcat_ws_url()
        if ws_url:
            return NapCatWsStreamClient(
                ws_url=ws_url,
                access_token=self._napcat_access_token(),
                timeout=max(30.0, float(self._request_timeout_seconds())),
                chunk_size_bytes=self._napcat_stream_chunk_size_bytes(),
                file_retention_seconds=self._napcat_stream_file_retention_seconds(),
            )
        return NapCatActionStreamClient(
            action_sender=action_sender,
            chunk_size_bytes=self._napcat_stream_chunk_size_bytes(),
            file_retention_seconds=self._napcat_stream_file_retention_seconds(),
        )

    def _session_timeout_seconds(self) -> int:
        return max(15, self._cfg_int("session_timeout_seconds", default=600))

    def _file_prepare_interval_seconds(self) -> float:
        raw_value = self._cfg("file_prepare_interval_seconds", default=2)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = 2.0
        return max(0.0, min(30.0, value))

    def _search_cache_ttl_seconds(self) -> int:
        return max(0, self._cfg_int("scan_cache_ttl_seconds", default=300))

    def _openlist_search_fetch_page_size(self) -> int:
        return max(10, min(500, self._cfg_int("openlist_search_fetch_page_size", default=100)))

    def _file_parent_scan_page_size(self) -> int:
        return max(100, min(1000, self._cfg_int("file_parent_scan_page_size", default=500)))

    def _file_parent_scan_max_pages(self) -> int:
        return max(1, min(200, self._cfg_int("file_parent_scan_max_pages", default=40)))

    def _cleanup_interval_seconds(self) -> int:
        return 60

    def _sources(self) -> list[SourceConfig]:
        raw_sources = self._cfg("sources", default=[])
        if isinstance(raw_sources, dict):
            items = list(raw_sources.values())
        elif isinstance(raw_sources, list):
            items = raw_sources
        else:
            items = []

        sources: list[SourceConfig] = []
        for index, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            if not bool(item.get("enabled", True)):
                continue
            raw_mount_path = str(item.get("mount_path", "") or "").strip()
            if not raw_mount_path:
                continue
            mount_path = normalize_path(raw_mount_path)
            label = str(item.get("label", "") or "").strip() or f"来源{index}"
            search_path = str(item.get("search_path", "/") or "/").strip() or "/"
            sources.append(
                SourceConfig(
                    slot=index,
                    label=label,
                    mount_path=mount_path,
                    search_path=search_path,
                )
            )
            if len(sources) >= MAX_SOURCES:
                break
        return sources

    def _ensure_required_config(self) -> None:
        missing: list[str] = []
        if not self._openlist_base_url():
            missing.append("openlist.base_url")
        if not self._sources():
            missing.append("sources")
        if missing:
            raise RuntimeError("请先完善配置：" + "、".join(missing))

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("HTTP 客户端尚未初始化。")
        return self._client

    def _cfg(self, *keys: str, default: Any = None) -> Any:
        current: Any = self.config
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
            if current is None:
                return default
        return current

    def _cfg_str(self, *keys: str, default: str = "") -> str:
        value = self._cfg(*keys, default=default)
        if value is None:
            return default
        return str(value)

    def _cfg_int(self, *keys: str, default: int = 0) -> int:
        value = self._cfg(*keys, default=default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _activation_openlist_mount_path(self) -> str:
        return normalize_path(self._cfg_str("activation", "openlist_mount_path", default="/"))

    def _activation_openlist_upload_path(self) -> str:
        raw = self._cfg_str("activation", "openlist_upload_path", default="/激活包").strip()
        return normalize_path(raw or "/激活包")

    def _activation_prewarm_minutes(self) -> int:
        return max(5, min(720, self._cfg_int("activation", "prewarm_minutes", default=30)))

    def _activation_private_request_ttl_seconds(self) -> int:
        if self._activation_auto_rotate_enabled():
            return max(1800, self._activation_rotate_hours() * 3600)
        return 86400

    def _activation_notify_group_uids(self) -> list[str]:
        raw_value = self._cfg("activation", "notify_group_uid", default="")
        values: list[str] = []
        if isinstance(raw_value, list):
            values = [str(item or "").strip() for item in raw_value]
        else:
            text = str(raw_value or "").strip()
            if text:
                values = [part.strip() for part in re.split(r"[\r\n,，;；]+", text) if part.strip()]
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _activation_admin_message(self, state: dict[str, Any], *, title: str = "激活码已更新") -> str:
        lines = [title]
        code = normalize_activation_code(str(state.get("code", "") or ""))
        if code:
            lines.append(f"激活码：{code}")
        share_url = str(state.get("share_url", "") or "").strip()
        if share_url:
            lines.append(f"链接：{share_url}")
        password = str(state.get("password", "") or "").strip()
        if password:
            lines.append(f"提取码：{password}")
        activate_at = self._activation_state_activate_at(state)
        if activate_at > 0:
            lines.append(f"生效时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(activate_at))}")
        expires_at = self._activation_state_expires_at(state)
        if expires_at > 0:
            lines.append(f"失效时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expires_at))}")
        return "\n".join(lines)

    async def _remember_admin_notify_origin(self, event: AstrMessageEvent) -> None:
        if not self._is_admin_event(event) or self._is_group_chat(event):
            return
        origin = self._event_unified_msg_origin(event)
        user_id = self._event_sender_id(event)
        if not origin or not user_id:
            return
        async with self._activation_lock:
            self._activation_admin_origins[user_id] = {
                "origin": origin,
                "updated_at": time.time(),
            }
            self._persist_activation_state_locked()

    async def _mark_private_activation_request(self, user_id: str, group_id: str, state: dict[str, Any]) -> None:
        if not user_id:
            return
        batch_id = str(state.get("batch_id", "") or "").strip()
        now = time.time()
        expires_at = self._activation_state_expires_at(state)
        if expires_at <= now:
            expires_at = now + self._activation_private_request_ttl_seconds()
        async with self._activation_lock:
            self._activation_requests[user_id] = {
                "batch_id": batch_id,
                "group_id": str(group_id or "").strip(),
                "expires_at": expires_at,
                "updated_at": now,
            }
            self._persist_activation_state_locked()

    async def _can_activate_via_private(self, user_id: str) -> bool:
        if not user_id:
            return False
        async with self._activation_lock:
            batch_id = str(self._activation_state.get("batch_id", "") or "").strip()
            record = self._activation_requests.get(user_id) or {}
            record_batch = str(record.get("batch_id", "") or "").strip()
            try:
                expires_at = float(record.get("expires_at", 0) or 0)
            except (TypeError, ValueError):
                expires_at = 0.0
            if not batch_id or batch_id != record_batch:
                return False
            return expires_at > time.time()

    async def _clear_private_activation_request(self, user_id: str) -> None:
        if not user_id:
            return
        async with self._activation_lock:
            if self._activation_requests.pop(user_id, None) is not None:
                self._persist_activation_state_locked()

    async def _notify_admins_activation_state(self, state: dict[str, Any]) -> int:
        if not self._activation_state_valid(state):
            return 0
        async with self._activation_lock:
            origins = [
                str((record or {}).get("origin", "") or "").strip()
                for record in self._activation_admin_origins.values()
                if isinstance(record, dict) and str((record or {}).get("origin", "") or "").strip()
            ]
        if not origins:
            return 0
        message_text = self._activation_admin_message(state)
        sent_count = 0
        for origin in list(dict.fromkeys(origins)):
            try:
                await self.context.send_message(origin, MessageChain().message(message_text))
                sent_count += 1
            except Exception as exc:
                logger.warning(f"向管理员推送激活码失败：{exc}")
        return sent_count

    async def _notify_group_activation_state(self, state: dict[str, Any]) -> bool:
        target_uids = self._activation_notify_group_uids()
        if not target_uids or not self._activation_state_valid(state):
            return False
        share_url = str(state.get("share_url", "") or "").strip()
        if not share_url:
            return False
        lines = [
            "新的激活链接已更新，请打开分享链接解压后获取激活码，然后私聊机器人发送：激活 激活码",
            f"链接：{share_url}",
        ]
        success = False
        message = MessageChain().message("\n".join(lines))
        for target_uid in target_uids:
            try:
                await self.context.send_message(target_uid, message)
                success = True
            except Exception as exc:
                logger.warning(f"向激活通知群发送链接失败：{exc}")
        return success

    def _activation_openlist_target_dir(self) -> str:
        mount_path = self._activation_openlist_mount_path()
        upload_path = self._activation_openlist_upload_path()
        if upload_path == "/":
            return mount_path
        return normalize_path(posixpath.join(mount_path, upload_path.lstrip("/")))

    def _activation_state_valid(self, state: dict[str, Any] | None = None) -> bool:
        target = state if isinstance(state, dict) else self._activation_state
        batch_id = str(target.get("batch_id", "") or "").strip()
        code = normalize_activation_code(str(target.get("code", "") or ""))
        share_url = str(target.get("share_url", "") or "").strip()
        try:
            activate_at = float(target.get("activate_at", target.get("created_at", 0)) or 0)
        except (TypeError, ValueError):
            activate_at = 0.0
        return bool(batch_id and code and share_url and activate_at > 0)

    def _activation_state_activate_at(self, state: dict[str, Any] | None = None) -> float:
        target = state if isinstance(state, dict) else self._activation_state
        try:
            return float(target.get("activate_at", target.get("created_at", 0)) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _activation_state_expires_at(self, state: dict[str, Any] | None = None) -> float:
        target = state if isinstance(state, dict) else self._activation_state
        try:
            return float(target.get("expires_at", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _activation_state_has_expiry(self, state: dict[str, Any] | None = None) -> bool:
        return self._activation_state_expires_at(state) > 0

    def _activation_pending_matches_current(self, current: dict[str, Any], pending: dict[str, Any]) -> bool:
        if not self._activation_state_valid(pending):
            return False
        prepared_for_batch = str(pending.get("prepared_for_batch_id", "") or "").strip()
        current_batch = str(current.get("batch_id", "") or "").strip()
        if not current_batch:
            return True
        return prepared_for_batch in ("", current_batch)

    def _activation_pending_ready(self, current: dict[str, Any], pending: dict[str, Any]) -> bool:
        if not self._activation_pending_matches_current(current, pending):
            return False
        return self._activation_state_activate_at(pending) <= time.time()

    def _activation_should_prewarm(self, current: dict[str, Any], pending: dict[str, Any]) -> bool:
        if not self._activation_enabled() or not self._activation_auto_rotate_enabled():
            return False
        if not self._activation_state_valid(current):
            return False
        if self._activation_pending_matches_current(current, pending):
            return False
        expires_at = self._activation_state_expires_at(current)
        if expires_at <= 0:
            return False
        lead_seconds = self._activation_prewarm_minutes() * 60
        return expires_at - time.time() <= lead_seconds

    def _remove_file_if_exists(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    def _persist_activation_state_locked(self) -> None:
        self._write_json_file(self._activation_state_path, self._activation_state)
        self._write_json_file(self._activation_users_path, self._activation_users)
        self._write_json_file(self._activation_requests_path, self._activation_requests)
        self._write_json_file(self._activation_admin_origins_path, self._activation_admin_origins)
        if self._activation_pending_state:
            self._write_json_file(self._activation_pending_state_path, self._activation_pending_state)
        else:
            self._remove_file_if_exists(self._activation_pending_state_path)

    async def _cleanup_activation_states(self, *states: dict[str, Any]) -> None:
        seen: set[str] = set()
        for state in states:
            if not isinstance(state, dict):
                continue
            batch_id = str(state.get("batch_id", "") or "").strip()
            if not batch_id or batch_id in seen:
                continue
            seen.add(batch_id)
            await self._cleanup_previous_activation_upload(state)

    async def _activate_state(self, state: dict[str, Any]) -> dict[str, Any]:
        previous_current: dict[str, Any] = {}
        previous_pending: dict[str, Any] = {}
        active_batch_id = str(state.get("batch_id", "") or "").strip()
        async with self._activation_lock:
            previous_current = dict(self._activation_state)
            previous_pending = dict(self._activation_pending_state)
            self._activation_state = dict(state)
            self._activation_pending_state = {}
            self._activation_users = {}
            self._activation_requests = {}
            self._persist_activation_state_locked()
            activated = dict(self._activation_state)
        cleanup_candidates: list[dict[str, Any]] = []
        if str(previous_current.get("batch_id", "") or "").strip() != active_batch_id:
            cleanup_candidates.append(previous_current)
        if str(previous_pending.get("batch_id", "") or "").strip() != active_batch_id:
            cleanup_candidates.append(previous_pending)
        await self._cleanup_activation_states(*cleanup_candidates)
        await self._notify_admins_activation_state(activated)
        await self._notify_group_activation_state(activated)
        return activated

    async def _promote_pending_activation_state(self) -> dict[str, Any]:
        async with self._activation_lock:
            current = dict(self._activation_state)
            pending = dict(self._activation_pending_state)
        if not self._activation_pending_ready(current, pending):
            return current
        return await self._activate_state(pending)

    def _schedule_activation_prewarm(self) -> None:
        if not self._activation_enabled() or not self._activation_auto_rotate_enabled():
            return
        task = self._activation_prewarm_task
        if task is not None and not task.done():
            return
        self._activation_prewarm_task = asyncio.create_task(self._run_activation_prewarm())

    async def _run_activation_prewarm(self) -> None:
        try:
            await self._prepare_pending_activation_state()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"激活码预生成失败：{exc}")
        finally:
            self._activation_prewarm_task = None

    async def _prepare_pending_activation_state(self) -> dict[str, Any]:
        async with self._activation_generation_lock:
            async with self._activation_lock:
                current = dict(self._activation_state)
                pending = dict(self._activation_pending_state)
            if not self._activation_should_prewarm(current, pending):
                return pending

            activate_at = self._activation_state_expires_at(current)
            if activate_at <= 0:
                activate_at = time.time()
            new_state = await self._generate_activation_package_state(
                activate_at=activate_at,
                prepared_for_batch_id=str(current.get("batch_id", "") or ""),
            )

            async with self._activation_lock:
                previous_pending = dict(self._activation_pending_state)
                self._activation_pending_state = dict(new_state)
                self._persist_activation_state_locked()
            await self._cleanup_activation_states(previous_pending)
            return dict(new_state)

    def _build_activation_quark_client(self) -> QuarkPanClient:
        cookie = self._activation_quark_cookie()
        if not cookie:
            raise RuntimeError("未配置 activation.quark_cookie")
        save_fid = self._activation_quark_save_fid()
        if not save_fid:
            raise RuntimeError("未配置 activation.quark_save_fid")
        return QuarkPanClient(
            cookie=cookie,
            save_fid=save_fid,
            timeout=max(30, self._request_timeout_seconds()),
            verify_ssl=self._activation_quark_verify_ssl(),
            share_expired_type=self._activation_quark_share_expired_type(),
        )

    async def _ensure_openlist_dir(self, client: OpenListClient, path: str) -> None:
        normalized = normalize_path(path)
        if normalized == "/":
            return
        current = ""
        for segment in [item for item in normalized.split("/") if item]:
            current = normalize_path(f"{current}/{segment}")
            try:
                await client.mkdir(current)
            except Exception as exc:
                message = str(exc).lower()
                if "exists" in message or "already" in message or "file exists" in message:
                    continue
                raise

    async def _find_quark_uploaded_fid(self, client: QuarkPanClient, file_name: str) -> str:
        normalized_name = str(file_name or "").strip()
        if not normalized_name:
            return ""
        for _ in range(12):
            try:
                items = await client.list_dir(self._activation_quark_save_fid())
            except Exception:
                items = []
            matches = [
                item
                for item in items
                if as_text(item.get("file_name")) == normalized_name and as_text(item.get("file_type")).lower() != "folder"
            ]
            if matches:
                matches.sort(key=lambda item: safe_int(item.get("updated_at"), 0, minimum=0), reverse=True)
                return as_text(matches[0].get("fid"))
            await asyncio.sleep(1.5)
        return ""

    def _build_activation_text(
        self,
        code: str,
        created_at: float,
        expires_at: float,
        *,
        activate_at: float | None = None,
    ) -> str:
        created_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_at))
        effective_activate_at = activate_at if activate_at is not None else created_at
        activate_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(effective_activate_at))
        lines = [
            "激活说明",
            f"当前激活码：{code}",
            f"生成时间：{created_text}",
        ]
        if effective_activate_at > created_at + 5:
            lines.append(f"生效时间：{activate_text}")
        if expires_at > 0:
            expires_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expires_at))
            lines.append(f"失效时间：{expires_text}")
        else:
            lines.append("失效时间：手动更新前有效")
        lines.extend(
            [
                "",
                "使用方法：",
                "1. 回到机器人聊天窗口",
                "2. 发送：激活 激活码",
                f"例如：激活 {code}",
            ]
        )
        return "\n".join(lines)

    async def _generate_activation_package_state(
        self,
        *,
        activate_at: float | None = None,
        prepared_for_batch_id: str = "",
    ) -> dict[str, Any]:
        created_at = time.time()
        effective_activate_at = max(created_at, float(activate_at or created_at))
        expires_at = 0.0
        if self._activation_auto_rotate_enabled():
            expires_at = effective_activate_at + self._activation_rotate_hours() * 3600
        batch_id = time.strftime("%Y%m%d%H%M%S", time.localtime(created_at)) + "_" + uuid4().hex[:8]
        code = generate_activation_code(self._activation_code_length())
        share_title = f"激活包_{time.strftime('%Y%m%d_%H%M%S', time.localtime(created_at))}_{batch_id[-4:]}"
        work_dir = self._activation_root / batch_id
        zip_path = work_dir / f"{share_title}.zip"
        txt_path = work_dir / "激活码.txt"

        work_dir.mkdir(parents=True, exist_ok=True)
        try:
            txt_path.write_text(
                self._build_activation_text(code, created_at, expires_at, activate_at=effective_activate_at),
                encoding="utf-8",
            )
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.write(txt_path, arcname="激活码.txt")

            openlist_client = self._build_openlist_client()
            quark_client = self._build_activation_quark_client()
            target_dir = self._activation_openlist_target_dir()
            target_path = normalize_path(posixpath.join(target_dir, zip_path.name))

            await self._ensure_openlist_dir(openlist_client, target_dir)
            await openlist_client.upload_form(zip_path, target_path, overwrite=True)

            uploaded_fid = await self._find_quark_uploaded_fid(quark_client, zip_path.name)
            if not uploaded_fid:
                raise RuntimeError("激活包已上传，但没有在夸克目录找到对应文件，请检查 activation.quark_save_fid 是否与上传目录一致")

            _, share_url, password = await quark_client.create_share([uploaded_fid], share_title)
            return {
                "batch_id": batch_id,
                "code": code,
                "created_at": created_at,
                "activate_at": effective_activate_at,
                "expires_at": expires_at,
                "share_url": share_url,
                "password": password,
                "title": share_title,
                "cleanup_job": {"provider": "quark", "payload": {"fids": [uploaded_fid]}},
                "openlist_target_path": target_path,
                "prepared_for_batch_id": str(prepared_for_batch_id or "").strip(),
            }
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    async def _ensure_activation_package_ready(self, force: bool = False) -> dict[str, Any]:
        if not self._activation_enabled():
            return {}
        if force:
            return await self._force_refresh_activation_state()

        async with self._activation_lock:
            current = dict(self._activation_state)
            pending = dict(self._activation_pending_state)

        if self._activation_pending_ready(current, pending):
            return await self._promote_pending_activation_state()

        if self._activation_state_valid(current):
            if self._activation_should_prewarm(current, pending):
                self._schedule_activation_prewarm()
            elif (
                self._activation_auto_rotate_enabled()
                and self._activation_state_has_expiry(current)
                and self._activation_state_expires_at(current) <= time.time()
                and not self._activation_pending_matches_current(current, pending)
            ):
                self._schedule_activation_prewarm()
            return current

        if self._activation_state_valid(pending):
            return await self._activate_state(pending)

        async with self._activation_generation_lock:
            async with self._activation_lock:
                current = dict(self._activation_state)
                pending = dict(self._activation_pending_state)

            if self._activation_pending_ready(current, pending):
                return await self._promote_pending_activation_state()
            if self._activation_state_valid(current):
                if self._activation_should_prewarm(current, pending):
                    self._schedule_activation_prewarm()
                return current
            if self._activation_state_valid(pending):
                return await self._activate_state(pending)

            new_state = await self._generate_activation_package_state(activate_at=time.time())
            return await self._activate_state(new_state)

    async def _force_refresh_activation_state(self) -> dict[str, Any]:
        async with self._activation_generation_lock:
            async with self._activation_lock:
                pending = dict(self._activation_pending_state)
            if self._activation_state_valid(pending):
                return await self._activate_state(pending)

            new_state = await self._generate_activation_package_state(activate_at=time.time())
            return await self._activate_state(new_state)

    async def _warmup_activation_package(self) -> None:
        try:
            await self._ensure_activation_package_ready()
        except Exception as exc:
            logger.warning(f"激活包预热失败：{exc}")

    def _activation_refresh_message(self, state: dict[str, Any]) -> str:
        return self._activation_admin_message(state)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("更新激活码")
    async def refresh_activation_code(self, event: AstrMessageEvent):
        if not self._activation_enabled():
            yield event.plain_result("当前未开启激活功能")
            return
        await self._remember_admin_notify_origin(event)
        yield event.plain_result("正在更新激活码，请稍后")
        try:
            state = await self._force_refresh_activation_state()
        except Exception as exc:
            logger.exception("refresh activation code failed")
            yield event.plain_result(f"更新激活码失败：{exc}")
            return
        if self._is_group_chat(event):
            yield event.plain_result("激活码已更新，新的激活码和链接已发送给管理员")
            return
        yield event.plain_result("激活码已更新，新的激活码和链接已发送给管理员")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("刷新激活码")
    async def refresh_activation_code_alias(self, event: AstrMessageEvent):
        async for result in self.refresh_activation_code(event):
            yield result
