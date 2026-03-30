"""
蓝奏搜索插件（lanzou_search）主入口模块

本插件通过 OpenList（基于 alist）搜索已挂载的网盘目录中的资源文件，
并将搜索到的文件直接发送给用户。支持私聊和群聊两种使用场景。

主要功能：
- 资源搜索：用户发送 "sh 关键词" 搜索已挂载目录中的文件
- 文件发送：搜索后发送序号即可获取对应文件
- 激活码管理：支持通过夸克网盘分享激活码实现群聊使用权限控制
- 多来源搜索：支持同时搜索多个挂载目录

架构设计：
- LanzouSearchPlugin: 插件主类，负责初始化和事件分发
- SearchService: 搜索服务，处理搜索逻辑和会话管理
- TransferService: 文件传输服务，处理文件下载和发送
- ActivationService: 激活码服务，管理激活码的生成、验证和轮换
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .clients.openlist import OpenListClient
from .core.constants import COMMAND_NAME
from .core.event_utils import event_debug_snapshot
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
    """蓝奏搜索插件主类

    作为 AstrBot 插件的入口点，负责：
    1. 初始化 HTTP 客户端和各业务服务
    2. 注册消息处理命令和事件监听器
    3. 管理插件生命周期（初始化、定时清理、终止）
    4. 提供配置读取的公共方法

    Attributes:
        config: 插件配置字典
        transfer_service: 文件传输服务实例
        activation_service: 激活码服务实例
        search_service: 搜索服务实例
    """

    def __init__(self, context: Context, config: AstrBotConfig):
        """初始化插件

        创建各业务服务实例并传入必要的依赖。入口类保持轻量，
        实际业务逻辑由各服务类自行管理。

        Args:
            context: AstrBot 插件上下文，提供框架级别的 API 访问
            config: 插件配置字典，包含 OpenList 连接信息、搜索来源等配置
        """
        super().__init__(context)
        self.config = config
        self._client: httpx.AsyncClient | None = None  # 共享的 HTTP 异步客户端
        self._cleanup_task: asyncio.Task[Any] | None = None  # 定时清理任务

        # 运行时数据目录，用于存储下载的临时文件、激活状态等
        runtime_root = (
            Path(get_astrbot_data_path())
            / "plugin_data"
            / "astrbot_plugin_lanzou_search"
        )
        activation_root = runtime_root / "activation"
        self._runtime_root = runtime_root
        self._activation_root = activation_root

        # 初始化文件传输服务：负责文件下载、缓存和发送
        self.transfer_service = TransferService(
            config=self.config,
            runtime_root=runtime_root,
            build_openlist_client=self._build_openlist_client,
            request_timeout_seconds=self._request_timeout_seconds,
            openlist_base_url=self._openlist_base_url,
            openlist_download_base_url=self._openlist_download_base_url,
        )
        # 初始化激活码服务：负责激活码生成、验证、轮换
        self.activation_service = ActivationService(
            context=self.context,
            config=self.config,
            runtime_root=runtime_root,
            activation_root=activation_root,
            build_openlist_client=self._build_openlist_client,
            request_timeout_seconds=self._request_timeout_seconds,
        )
        # 初始化搜索服务：负责搜索逻辑和会话管理
        self.search_service = SearchService(
            config=self.config,
            build_openlist_client=self._build_openlist_client,
            send_search_item=self.transfer_service.send_search_item,
            ensure_group_access=self.activation_service.ensure_group_activation_access,
        )

    async def initialize(self) -> None:
        """异步初始化，在插件加载时由框架调用

        执行以下初始化操作：
        1. 创建共享 HTTP 异步客户端（配置超时和自动重定向）
        2. 确保运行时数据目录存在
        3. 清理上次运行遗留的临时下载文件
        4. 从磁盘恢复激活码状态
        5. 启动后台定时清理任务（每 60 秒执行一次）
        """
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
        """异步终止，在插件卸载时由框架调用

        按顺序释放所有资源：
        1. 取消并等待后台清理任务结束
        2. 终止激活码服务（取消预热任务等）
        3. 关闭共享 HTTP 客户端
        """
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
        """处理搜索命令 "sh 关键词"

        当用户发送以 "sh" 开头的消息时触发。流程：
        1. 记录管理员的私信来源（用于后续推送激活码通知）
        2. 检查群聊激活权限（如果启用了激活功能）
        3. 提取搜索关键词并启动搜索流程

        Args:
            event: AstrBot 消息事件对象

        Yields:
            搜索结果消息（可能多条，包含加载提示、结果列表等）
        """
        logger.debug("lanzou_search command received: %s", event_debug_snapshot(event))
        await self.activation_service.remember_admin_notify_origin(event)
        if not await self.activation_service.ensure_group_activation_access(event):
            logger.debug(
                "lanzou_search command blocked by group activation: %s",
                event_debug_snapshot(event),
            )
            return
        keyword = extract_command_keyword(event.message_str, COMMAND_NAME)
        logger.debug('lanzou_search extracted keyword="%s"', keyword)
        async for result in self.search_service.search_flow(event, keyword):
            yield result

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def handle_search_session_input(self, event: AstrMessageEvent):
        """处理搜索会话中的后续输入

        监听所有消息，从中识别：
        1. 激活命令（"激活 激活码" 或 "jh 激活码"）
        2. 搜索会话操作（序号选择、翻页 n/p、退出 q）
        3. 忽略新的搜索命令（由 lanzou_search 处理器处理）

        群聊中需要用户引用搜索结果消息后再发送操作指令，
        私聊中直接发送即可。

        Args:
            event: AstrBot 消息事件对象
        """
        logger.debug(
            "lanzou_search followup event received: %s", event_debug_snapshot(event)
        )
        await self.activation_service.remember_admin_notify_origin(event)
        text = (event.message_str or "").strip()
        if not text:
            logger.debug("lanzou_search followup ignored because text is empty")
            return

        # 优先处理激活命令
        activation_code = parse_activation_command(text)
        if activation_code is not None:
            logger.debug(
                'lanzou_search activation command detected text="%s" code="%s"',
                text,
                activation_code,
            )
            await self.activation_service.handle_activation_command(
                event, activation_code
            )
            return
        # 忽略搜索命令（由 lanzou_search 处理器单独处理）
        if parse_search_command(text) is not None:
            logger.debug(
                'lanzou_search followup ignored because it is a search command: "%s"',
                text,
            )
            return

        # 处理搜索会话的后续操作（选择文件、翻页、退出等）
        logger.debug('lanzou_search dispatching followup text="%s"', text)
        await self.search_service.handle_followup(event, text)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("更新激活码")
    async def refresh_activation_code(self, event: AstrMessageEvent):
        """管理员命令：强制更新激活码

        立即生成新的激活码并上传到夸克网盘，同时通知所有管理员
        和指定的通知群组。

        Args:
            event: AstrBot 消息事件对象

        Yields:
            操作状态消息
        """
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
        """管理员命令：刷新激活码（"更新激活码" 的别名命令）

        Args:
            event: AstrBot 消息事件对象

        Yields:
            操作状态消息（委托给 refresh_activation_code）
        """
        async for result in self.refresh_activation_code(event):
            yield result

    async def _cleanup_loop(self) -> None:
        """后台定时清理循环

        每 60 秒执行一次，依次清理各服务的过期状态：
        1. 搜索服务：清理过期的搜索会话和缓存
        2. 传输服务：清理过期的文件链接缓存和临时下载目录
        3. 激活服务：清理过期的激活请求记录
        4. 激活服务：检查并执行激活码自动轮换
        """
        while True:
            await asyncio.sleep(60)
            await self.search_service.prune_runtime_state()
            await self.transfer_service.prune_runtime_state()
            await self.activation_service.prune_runtime_state()
            self.transfer_service.cleanup_download_dirs(force_all=False)
            await self.activation_service.tick()

    def _build_openlist_client(self) -> OpenListClient:
        """创建 OpenList 客户端实例

        使用当前配置创建一个新的 OpenListClient 实例，
        每次调用创建独立实例以避免并发问题。

        Returns:
            配置好的 OpenListClient 实例
        """
        return OpenListClient(
            self._require_client(),
            self._openlist_base_url(),
            token=self._cfg_str("openlist", "token", default="").strip(),
            username=self._cfg_str("openlist", "username", default="").strip(),
            password=self._cfg_str("openlist", "password", default=""),
            request_timeout_seconds=self._request_timeout_seconds(),
        )

    def _openlist_base_url(self) -> str:
        """获取 OpenList 服务的基础 URL

        Returns:
            去除末尾斜杠的 URL，默认为 http://host.docker.internal:5244
        """
        return self._cfg_str(
            "openlist", "base_url", default="http://host.docker.internal:5244"
        ).rstrip("/")

    def _openlist_download_base_url(self) -> str:
        """获取 OpenList 下载文件时使用的基础 URL

        如果单独配置了 download_base_url 则使用该值（适用于内网穿透场景），
        否则回退到普通的 base_url。

        Returns:
            下载用的基础 URL
        """
        configured = self._cfg_str("openlist", "download_base_url", default="").rstrip(
            "/"
        )
        return configured or self._openlist_base_url()

    def _request_timeout_seconds(self) -> int:
        """获取 HTTP 请求超时时间（秒）

        Returns:
            超时时间，最小为 10 秒，默认 60 秒
        """
        return max(10, self._cfg_int("openlist", "request_timeout_seconds", default=60))

    def _require_client(self) -> httpx.AsyncClient:
        """获取共享的 HTTP 客户端实例

        Returns:
            已初始化的 httpx.AsyncClient 实例

        Raises:
            RuntimeError: 客户端尚未初始化时（在 initialize 之前调用）
        """
        if self._client is None:
            raise RuntimeError("HTTP 客户端尚未初始化。")
        return self._client

    def _cfg(self, *keys: str, default: Any = None) -> Any:
        """安全地从嵌套配置字典中读取值

        支持多级键路径，例如 _cfg("openlist", "base_url") 对应 config["openlist"]["base_url"]。

        Args:
            *keys: 逐层深入的键名
            default: 键不存在时的默认值

        Returns:
            配置值或默认值
        """
        current: Any = self.config
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
            if current is None:
                return default
        return current

    def _cfg_str(self, *keys: str, default: str = "") -> str:
        """从配置中读取字符串值

        Args:
            *keys: 逐层深入的键名
            default: 默认值

        Returns:
            字符串形式的配置值
        """
        value = self._cfg(*keys, default=default)
        if value is None:
            return default
        return str(value)

    def _cfg_int(self, *keys: str, default: int = 0) -> int:
        """从配置中读取整数值

        Args:
            *keys: 逐层深入的键名
            default: 默认值

        Returns:
            整数形式的配置值，转换失败时返回默认值
        """
        value = self._cfg(*keys, default=default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
