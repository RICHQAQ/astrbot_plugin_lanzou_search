from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .clients.openlist import OpenListClient
from .core.constants import COMMAND_NAME
from .core.utils import (
    extract_command_keyword,
    parse_activation_command,
    parse_search_command,
)
from .services.activation import ActivationService
from .services.search import SearchService
from .services.transfer import TransferService


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
        self._client: httpx.AsyncClient | None = None
        self._cleanup_task: asyncio.Task[Any] | None = None

        runtime_root = (
            Path(get_astrbot_data_path())
            / "plugin_data"
            / "astrbot_plugin_lanzou_search"
        )
        activation_root = runtime_root / "activation"
        self._runtime_root = runtime_root
        self._activation_root = activation_root

        # Keep the entry class thin: services own their state and behavior.
        self.transfer_service = TransferService(
            config=self.config,
            runtime_root=runtime_root,
            build_openlist_client=self._build_openlist_client,
            request_timeout_seconds=self._request_timeout_seconds,
            openlist_base_url=self._openlist_base_url,
            openlist_download_base_url=self._openlist_download_base_url,
        )
        self.activation_service = ActivationService(
            context=self.context,
            config=self.config,
            runtime_root=runtime_root,
            activation_root=activation_root,
            build_openlist_client=self._build_openlist_client,
            request_timeout_seconds=self._request_timeout_seconds,
        )
        self.search_service = SearchService(
            config=self.config,
            build_openlist_client=self._build_openlist_client,
            send_search_item=self.transfer_service.send_search_item,
            ensure_group_access=self.activation_service.ensure_group_activation_access,
        )

    async def initialize(self) -> None:
        timeout_seconds = float(self._request_timeout_seconds())
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds, connect=min(10.0, timeout_seconds)),
            follow_redirects=True,
        )
        self._runtime_root.mkdir(parents=True, exist_ok=True)
        self._activation_root.mkdir(parents=True, exist_ok=True)
        self.transfer_service.cleanup_download_dirs(force_all=True)
        await self.activation_service.initialize()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def terminate(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        await self.activation_service.terminate()
        if self._client is not None:
            await self._client.aclose()

    @filter.command(COMMAND_NAME)
    async def lanzou_search(self, event: AstrMessageEvent):
        await self.activation_service.remember_admin_notify_origin(event)
        if not await self.activation_service.ensure_group_activation_access(event):
            return
        keyword = extract_command_keyword(event.message_str, COMMAND_NAME)
        async for result in self.search_service.search_flow(event, keyword):
            yield result

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def handle_search_session_input(self, event: AstrMessageEvent):
        await self.activation_service.remember_admin_notify_origin(event)
        text = (event.message_str or "").strip()
        if not text:
            return

        activation_code = parse_activation_command(text)
        if activation_code is not None:
            await self.activation_service.handle_activation_command(
                event, activation_code
            )
            return
        if parse_search_command(text) is not None:
            return

        await self.search_service.handle_followup(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("更新激活码")
    async def refresh_activation_code(self, event: AstrMessageEvent):
        if not self.activation_service.activation_enabled():
            yield event.plain_result("当前未开启激活功能")
            return
        await self.activation_service.remember_admin_notify_origin(event)
        yield event.plain_result("正在更新激活码，请稍后")
        try:
            await self.activation_service.force_refresh_activation_state()
        except Exception as exc:
            yield event.plain_result(f"更新激活码失败：{exc}")
            return
        yield event.plain_result("激活码已更新，新的激活码和链接已发送给管理员")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("刷新激活码")
    async def refresh_activation_code_alias(self, event: AstrMessageEvent):
        async for result in self.refresh_activation_code(event):
            yield result

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            await self.search_service.prune_runtime_state()
            await self.transfer_service.prune_runtime_state()
            await self.activation_service.prune_runtime_state()
            self.transfer_service.cleanup_download_dirs(force_all=False)
            await self.activation_service.tick()

    def _build_openlist_client(self) -> OpenListClient:
        return OpenListClient(
            self._require_client(),
            self._openlist_base_url(),
            token=self._cfg_str("openlist", "token", default="").strip(),
            username=self._cfg_str("openlist", "username", default="").strip(),
            password=self._cfg_str("openlist", "password", default=""),
            request_timeout_seconds=self._request_timeout_seconds(),
        )

    def _openlist_base_url(self) -> str:
        return self._cfg_str(
            "openlist", "base_url", default="http://host.docker.internal:5244"
        ).rstrip("/")

    def _openlist_download_base_url(self) -> str:
        configured = self._cfg_str("openlist", "download_base_url", default="").rstrip(
            "/"
        )
        return configured or self._openlist_base_url()

    def _request_timeout_seconds(self) -> int:
        return max(10, self._cfg_int("openlist", "request_timeout_seconds", default=60))

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
