from __future__ import annotations

import asyncio
import hashlib
import mimetypes
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from ..core.constants import (
    FILE_OPERATION_TIMEOUT_SECONDS,
    OPENLIST_ADD_OFFLINE_DOWNLOAD_API,
    OPENLIST_GET_API,
    OPENLIST_LINK_API,
    OPENLIST_LIST_API,
    OPENLIST_LOGIN_HASH_API,
    OPENLIST_MKDIR_API,
    OPENLIST_REMOVE_API,
    OPENLIST_SEARCH_API,
    OPENLIST_STATIC_HASH_SALT,
)
from ..core.utils import normalize_path


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
            raise RuntimeError(
                f"{source} 返回的不是 JSON：{response.text[:200]}"
            ) from exc
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
                    self._extract_error_message(
                        data, default=f"OpenList 登录失败：HTTP {response.status_code}"
                    )
                )
            code = data.get("code")
            if code not in (None, 200):
                raise RuntimeError(
                    self._extract_error_message(
                        data, default=f"OpenList 登录失败：{code}"
                    )
                )

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
        json_data: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
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

        # Retry once after an implicit re-login when credential mode is enabled.
        if (
            response.status_code == 401
            and not self.supplied_token
            and self._has_credentials()
        ):
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
            raise RuntimeError(
                self._extract_error_message(
                    data, default=f"OpenList HTTP {response.status_code}"
                )
            )
        code = data.get("code")
        if code not in (None, 200):
            raise RuntimeError(
                self._extract_error_message(data, default=f"OpenList 返回错误：{code}")
            )
        return data

    async def search(
        self, parent: str, keyword: str, page: int, per_page: int
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            OPENLIST_SEARCH_API,
            json_data={
                "parent": normalize_path(parent),
                "keywords": keyword,
                "scope": 2,
                "page": max(1, page),
                "per_page": max(1, per_page),
            },
        )

    async def get_file(self, path: str) -> dict[str, Any]:
        return await self.request(
            "POST",
            OPENLIST_GET_API,
            json_data={"path": normalize_path(path)},
            timeout_seconds=max(
                self.request_timeout, float(FILE_OPERATION_TIMEOUT_SECONDS)
            ),
        )

    async def get_link(self, path: str) -> dict[str, Any]:
        return await self.request(
            "POST",
            OPENLIST_LINK_API,
            json_data={"path": normalize_path(path)},
            timeout_seconds=max(
                self.request_timeout, float(FILE_OPERATION_TIMEOUT_SECONDS)
            ),
        )

    async def list_dir(
        self, path: str, page: int, per_page: int, *, refresh: bool = False
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            OPENLIST_LIST_API,
            json_data={
                "path": normalize_path(path),
                "page": max(1, page),
                "per_page": max(1, per_page),
                "refresh": bool(refresh),
            },
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def mkdir(self, path: str) -> dict[str, Any]:
        return await self.request(
            "POST",
            OPENLIST_MKDIR_API,
            json_data={"path": normalize_path(path)},
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def remove(self, dir_path: str, names: list[str]) -> dict[str, Any]:
        filtered_names = [
            str(name or "").strip() for name in names if str(name or "").strip()
        ]
        return await self.request(
            "POST",
            OPENLIST_REMOVE_API,
            json_data={"dir": normalize_path(dir_path), "names": filtered_names},
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
        return await self.request(
            "POST",
            OPENLIST_ADD_OFFLINE_DOWNLOAD_API,
            json_data={
                "urls": [
                    str(url or "").strip() for url in urls if str(url or "").strip()
                ],
                "path": normalize_path(path),
                "tool": str(tool or "SimpleHttp").strip() or "SimpleHttp",
                "delete_policy": str(delete_policy or "upload_download_stream").strip()
                or "upload_download_stream",
            },
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

    async def upload_form(
        self, local_path: Path, target_path: str, *, overwrite: bool = True
    ) -> dict[str, Any]:
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
                files={
                    "file": (
                        local_path.name,
                        file_obj,
                        content_type or "application/octet-stream",
                    )
                },
                timeout=max(self.request_timeout, 300.0),
            )
        data = self._parse_json(response, "OpenList 上传")
        if response.status_code >= 400:
            raise RuntimeError(
                self._extract_error_message(
                    data, default=f"OpenList 上传失败：HTTP {response.status_code}"
                )
            )
        code = data.get("code")
        if code not in (None, 200):
            raise RuntimeError(
                self._extract_error_message(data, default=f"OpenList 上传失败：{code}")
            )
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
        return (
            left.scheme == right.scheme
            and left.host == right.host
            and left.port == right.port
        )

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
