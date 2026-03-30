"""
OpenList 客户端模块

封装与 OpenList（基于 alist）服务的所有 HTTP API 通信逻辑。
提供文件搜索、目录浏览、文件下载/上传、离线下载、认证鉴权等功能。

OpenList 是一个支持多种网盘挂载的文件列表程序，本客户端通过其
RESTful API 实现文件操作，支持 Token 和用户名密码两种认证方式，
并内置了 Token 自动刷新和请求重试机制。
"""

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
    """OpenList API 客户端

    封装了与 OpenList 服务的所有交互，包括：
    - Token 认证和自动刷新
    - 文件搜索和目录浏览
    - 文件链接获取和下载
    - 文件上传（表单方式）
    - 目录创建和文件删除
    - 离线下载任务管理

    Attributes:
        http_client: 共享的 httpx 异步客户端
        base_url: OpenList 服务的基础 URL
        token: 当前有效的访问令牌
    """

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
        """初始化 OpenList 客户端

        支持两种认证方式（优先级从高到低）：
        1. 直接提供 Token（无需登录）
        2. 提供用户名和密码（自动登录获取 Token）

        Args:
            http_client: 共享的 HTTP 异步客户端实例
            base_url: OpenList 服务地址，如 "http://localhost:5244"
            token: 直接提供的访问令牌（可选）
            username: OpenList 用户名（可选）
            password: OpenList 明文密码（会被自动哈希后发送）
            request_timeout_seconds: 请求超时时间（秒），默认 60 秒
        """
        self.http_client = http_client
        self.base_url = str(base_url or "").rstrip("/")
        self.supplied_token = str(token or "").strip()
        self.username = str(username or "").strip()
        self.password = str(password or "")
        self.request_timeout = max(10.0, float(request_timeout_seconds or 60))
        self.token = self.supplied_token
        self._token_lock = asyncio.Lock()  # 保护 Token 刷新的并发锁

    def _has_credentials(self) -> bool:
        """检查是否配置了完整的用户名和密码"""
        return bool(self.username and self.password)

    def _static_hash(self, password: str) -> str:
        """计算 OpenList 登录所需的密码哈希

        OpenList 使用 SHA256(密码-盐值) 的方式对密码进行哈希，
        盐值固定为 alist 的 GitHub 仓库地址。

        Args:
            password: 明文密码

        Returns:
            SHA256 哈希的十六进制字符串
        """
        raw = f"{password}-{OPENLIST_STATIC_HASH_SALT}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _parse_json(self, response: httpx.Response, source: str) -> dict[str, Any]:
        """解析 HTTP 响应的 JSON 内容

        Args:
            response: HTTP 响应对象
            source: 调用来源描述（用于错误信息）

        Returns:
            解析后的 JSON 字典

        Raises:
            RuntimeError: 响应不是有效的 JSON 或格式不正确时
        """
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
        """从 OpenList 响应中提取错误消息

        依次尝试从顶层和 data 字典中提取 message/msg/error 字段。

        Args:
            data: OpenList 响应数据
            default: 无法提取时的默认消息

        Returns:
            错误消息字符串
        """
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
        """确保拥有有效的访问令牌

        逻辑流程：
        1. 如果直接提供了 Token，直接使用
        2. 如果没有凭据，返回空字符串（匿名访问）
        3. 如果已有 Token 且非强制刷新，复用现有 Token
        4. 使用用户名和哈希密码调用登录接口获取新 Token

        使用异步锁保证并发安全，避免多个请求同时刷新 Token。

        Args:
            force: 是否强制刷新 Token（忽略缓存的 Token）

        Returns:
            有效的访问令牌字符串

        Raises:
            RuntimeError: 登录失败时
        """
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
        """发送 HTTP 请求到 OpenList API

        自动附加认证头，处理 401 未授权响应（自动重新登录后重试一次）。

        Args:
            method: HTTP 方法（GET/POST/PUT 等）
            path: API 路径
            json_data: 请求体 JSON 数据
            timeout_seconds: 本次请求的超时时间（覆盖默认值）

        Returns:
            OpenList 响应的 JSON 数据

        Raises:
            RuntimeError: HTTP 错误或 API 返回错误码时
        """
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

        # 当使用凭据模式收到 401 时，自动重新登录并重试一次
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
        """在指定目录下搜索文件

        Args:
            parent: 搜索的父目录路径
            keyword: 搜索关键词
            page: 页码（从 1 开始）
            per_page: 每页结果数

        Returns:
            搜索结果数据，包含 content（文件列表）和 total（总数）等字段
        """
        return await self.request(
            "POST",
            OPENLIST_SEARCH_API,
            json_data={
                "parent": normalize_path(parent),
                "keywords": keyword,
                "scope": 2,  # 搜索范围：2 表示仅文件（不含目录）
                "page": max(1, page),
                "per_page": max(1, per_page),
            },
        )

    async def get_file(self, path: str) -> dict[str, Any]:
        """获取文件详细信息（包含 raw_url 直链）

        Args:
            path: 文件路径

        Returns:
            文件信息数据，包含 raw_url 等字段
        """
        return await self.request(
            "POST",
            OPENLIST_GET_API,
            json_data={"path": normalize_path(path)},
            timeout_seconds=max(
                self.request_timeout, float(FILE_OPERATION_TIMEOUT_SECONDS)
            ),
        )

    async def get_link(self, path: str) -> dict[str, Any]:
        """获取文件的下载/访问链接

        通过 OpenList 的 fs/link 接口获取文件的访问链接，
        该链接可能带有签名和过期时间。

        Args:
            path: 文件路径

        Returns:
            链接信息数据，包含 url 字段
        """
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
        """列出目录内容

        Args:
            path: 目录路径
            page: 页码（从 1 开始）
            per_page: 每页结果数
            refresh: 是否强制刷新目录缓存

        Returns:
            目录列表数据，包含 content（条目列表）和 total（总数）
        """
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
        """创建目录

        如果目录已存在，OpenList 会返回错误，调用方需要自行处理。

        Args:
            path: 要创建的目录路径

        Returns:
            创建结果数据
        """
        return await self.request(
            "POST",
            OPENLIST_MKDIR_API,
            json_data={"path": normalize_path(path)},
            timeout_seconds=max(self.request_timeout, 60.0),
        )

    async def remove(self, dir_path: str, names: list[str]) -> dict[str, Any]:
        """删除指定目录下的文件或子目录

        Args:
            dir_path: 目标目录路径
            names: 要删除的文件/目录名称列表

        Returns:
            删除结果数据
        """
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
        """添加离线下载任务

        将指定的 URL 添加到 OpenList 的离线下载队列中，
        下载完成后文件会出现在指定目录。

        Args:
            urls: 要下载的 URL 列表
            path: 下载目标目录
            tool: 下载工具，默认 "SimpleHttp"
            delete_policy: 完成后的删除策略，默认 "upload_download_stream"

        Returns:
            下载任务数据，包含 tasks 字段
        """
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
        """查询异步任务的状态

        用于轮询离线下载等异步任务的完成状态。

        Args:
            task_group: 任务组名称，如 "offline_download"
            task_id: 任务 ID

        Returns:
            任务状态数据，包含 progress、error、end_time 等字段
        """
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
        """通过表单方式上传文件到 OpenList

        使用 multipart/form-data 格式上传本地文件到 OpenList 指定路径。

        Args:
            local_path: 本地文件路径
            target_path: OpenList 中的目标文件路径
            overwrite: 是否覆盖已存在的文件，默认 True

        Returns:
            上传结果数据

        Raises:
            RuntimeError: 文件不存在或上传失败时
        """
        if not local_path.exists():
            raise RuntimeError(f"待上传文件不存在：{local_path}")

        headers: dict[str, str] = {
            "File-Path": quote(normalize_path(target_path), safe="/"),
            "Overwrite": "true" if overwrite else "false",
            "As-Task": "false",  # 同步上传，不创建异步任务
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
                timeout=max(self.request_timeout, 300.0),  # 上传使用较长超时
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
        """规范化下载 URL

        如果 URL 是相对路径（以 / 开头），则拼接 OpenList 基础 URL。

        Args:
            url: 原始 URL 或路径

        Returns:
            完整的 URL 字符串
        """
        text = str(url or "").strip()
        if not text:
            return ""
        if text.startswith("/"):
            return self.base_url + text
        return text

    def _is_same_openlist_host(self, url: str) -> bool:
        """判断 URL 是否与 OpenList 服务在同一主机

        用于决定下载时是否需要附加认证头。

        Args:
            url: 待比较的 URL

        Returns:
            是否为同一主机（协议、域名、端口均相同）
        """
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
        """下载文件到本地路径

        使用流式下载方式（避免大文件占用过多内存），
        如果下载 URL 与 OpenList 同主机则自动附加认证头。

        Args:
            url: 文件下载 URL
            target_path: 本地保存路径

        Raises:
            RuntimeError: URL 为空或下载失败时
        """
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
