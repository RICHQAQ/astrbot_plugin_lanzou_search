"""
激活码服务模块

管理群聊使用权限的激活码系统，主要功能包括：
- 激活码的生成、验证和自动轮换
- 激活包的上传到夸克网盘并创建分享链接
- 已激活用户的管理和权限验证
- 管理员通知和群组通知

激活流程说明：
1. 未激活用户在群聊中使用搜索功能时，收到激活提示
2. 用户打开分享链接下载激活包，解压获取激活码
3. 用户私聊机器人发送"激活 激活码"完成激活
4. 激活成功后用户可在群聊中正常使用搜索功能

激活码会定期自动轮换（可配置轮换周期），旧激活码失效后
已激活用户需要重新激活才能继续使用。
"""

from __future__ import annotations

import asyncio
import json
import posixpath
import re
import shutil
import time
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain

from ..clients.openlist import OpenListClient
from ..clients.quark import QuarkPanClient
from ..core.constants import (
    ACTIVATION_ADMIN_ORIGINS_FILE,
    ACTIVATION_PENDING_STATE_FILE,
    ACTIVATION_REQUESTS_FILE,
    ACTIVATION_STATE_FILE,
    ACTIVATION_USERS_FILE,
)
from ..core.event_utils import (
    event_group_id,
    event_sender_id,
    event_unified_msg_origin,
    is_admin_event,
    is_group_chat,
)
from ..core.utils import (
    as_text,
    generate_activation_code,
    normalize_activation_code,
    normalize_path,
    safe_bool,
    safe_int,
)


class ActivationService:
    """激活码服务

    负责管理群聊使用权限的激活码系统，包括：
    - 激活状态持久化（JSON 文件）
    - 激活包生成和上传
    - 激活码验证和用户管理
    - 自动轮换和预热

    Attributes:
        context: AstrBot 插件上下文
        config: 插件配置字典
        _activation_state: 当前生效的激活状态
        _activation_pending_state: 待生效的下一批激活状态（预生成）
        _activation_users: 已激活用户记录
        _activation_requests: 待验证的激活请求记录
    """

    def __init__(
        self,
        *,
        context: Any,
        config: dict[str, Any],
        runtime_root: Path,
        activation_root: Path,
        build_openlist_client: Callable[[], OpenListClient],
        request_timeout_seconds: Callable[[], int],
    ) -> None:
        """初始化激活码服务

        Args:
            context: AstrBot 插件上下文，用于发送主动消息
            config: 插件配置字典
            runtime_root: 运行时数据根目录
            activation_root: 激活包临时工作目录
            build_openlist_client: 创建 OpenList 客户端的工厂函数
            request_timeout_seconds: 获取请求超时时间的函数
        """
        self.context = context
        self.config = config
        self.runtime_root = runtime_root
        self.activation_root = activation_root
        self._build_openlist_client = build_openlist_client
        self._request_timeout_seconds = request_timeout_seconds

        # 运行时状态
        self._activation_state: dict[str, Any] = {}
        self._activation_pending_state: dict[str, Any] = {}
        self._activation_users: dict[str, dict[str, Any]] = {}
        self._activation_requests: dict[str, dict[str, Any]] = {}
        self._activation_admin_origins: dict[str, dict[str, Any]] = {}

        # 持久化文件路径
        self._activation_state_path = runtime_root / ACTIVATION_STATE_FILE
        self._activation_pending_state_path = (
            runtime_root / ACTIVATION_PENDING_STATE_FILE
        )
        self._activation_users_path = runtime_root / ACTIVATION_USERS_FILE
        self._activation_requests_path = runtime_root / ACTIVATION_REQUESTS_FILE
        self._activation_admin_origins_path = (
            runtime_root / ACTIVATION_ADMIN_ORIGINS_FILE
        )

        # 并发控制锁
        self._activation_lock = asyncio.Lock()
        self._activation_generation_lock = asyncio.Lock()
        self._activation_prewarm_task: asyncio.Task[Any] | None = None

    async def initialize(self) -> None:
        """异步初始化，从磁盘恢复激活状态

        加载所有持久化的 JSON 状态文件，如果启用了激活功能
        则启动激活包预热任务。
        """
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.activation_root.mkdir(parents=True, exist_ok=True)
        self._activation_state = self._load_json_file(
            self._activation_state_path, default={}
        )
        self._activation_pending_state = self._load_json_file(
            self._activation_pending_state_path, default={}
        )
        self._activation_users = self._load_json_file(
            self._activation_users_path, default={}
        )
        self._activation_requests = self._load_json_file(
            self._activation_requests_path, default={}
        )
        self._activation_admin_origins = self._load_json_file(
            self._activation_admin_origins_path, default={}
        )
        if self._activation_enabled():
            self._activation_prewarm_task = asyncio.create_task(
                self._warmup_activation_package()
            )

    async def terminate(self) -> None:
        """异步终止，取消预热任务"""
        if self._activation_prewarm_task is not None:
            self._activation_prewarm_task.cancel()
            try:
                await self._activation_prewarm_task
            except asyncio.CancelledError:
                pass

    async def tick(self) -> None:
        """定时检查，由主插件的清理循环调用

        检查是否需要生成新的激活包（自动轮换场景）。
        """
        if self._activation_enabled() and self._activation_auto_rotate_enabled():
            try:
                await self._ensure_activation_package_ready()
            except Exception as exc:
                logger.warning(f"激活包轮换检查失败：{exc}")

    async def prune_runtime_state(self) -> None:
        """清理过期的运行时状态

        清理不属于当前批次的已激活用户记录，
        清理已过期的激活请求记录。
        """
        async with self._activation_lock:
            current_batch_id = str(self._activation_state.get("batch_id", "") or "")
            if current_batch_id:
                pruned_users = {
                    key: value
                    for key, value in self._activation_users.items()
                    if isinstance(value, dict)
                    and str(value.get("batch_id", "") or "") == current_batch_id
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
                self._write_json_file(
                    self._activation_users_path, self._activation_users
                )
            if pruned_requests != self._activation_requests:
                self._activation_requests = pruned_requests
                self._write_json_file(
                    self._activation_requests_path, self._activation_requests
                )

    async def remember_admin_notify_origin(self, event: AstrMessageEvent) -> None:
        """记录管理员的私信来源地址

        用于在激活码更新时主动推送通知给管理员。
        仅记录来自私聊的管理员事件。

        Args:
            event: 消息事件对象
        """
        if not is_admin_event(event) or is_group_chat(event):
            return
        origin = event_unified_msg_origin(event)
        user_id = event_sender_id(event)
        if not origin or not user_id:
            return
        async with self._activation_lock:
            self._activation_admin_origins[user_id] = {
                "origin": origin,
                "updated_at": time.time(),
            }
            self._persist_activation_state_locked()

    async def ensure_group_activation_access(self, event: AstrMessageEvent) -> bool:
        """检查并确保用户在群聊中有使用权限

        如果激活功能未启用，直接放行。
        如果用户已激活，放行。
        否则提示用户进行激活并阻止后续处理。

        Args:
            event: 消息事件对象

        Returns:
            True 表示用户有权限，False 表示无权限（已发送激活提示）
        """
        if not self._activation_enabled() or not is_group_chat(event):
            return True
        sender_id = event_sender_id(event)
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

        await self._mark_private_activation_request(
            sender_id, event_group_id(event), state
        )
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

    async def handle_activation_command(
        self, event: AstrMessageEvent, code: str
    ) -> None:
        """处理用户发送的激活命令

        验证激活码是否正确，正确则标记用户已激活。

        Args:
            event: 消息事件对象
            code: 用户发送的激活码
        """
        if not self._activation_enabled():
            await event.send(event.plain_result("当前未开启激活"))
            event.stop_event()
            return

        if is_group_chat(event):
            await event.send(event.plain_result("请私聊机器人发送：激活 激活码"))
            event.stop_event()
            return

        await self.remember_admin_notify_origin(event)

        normalized_code = normalize_activation_code(code)
        if not normalized_code:
            await event.send(event.plain_result("用法：激活 激活码"))
            event.stop_event()
            return

        try:
            state = await self._ensure_activation_package_ready()
        except Exception as exc:
            logger.exception("activation package prepare failed")
            await event.send(
                event.plain_result(f"激活包准备失败，请稍后再试\n原因：{exc}")
            )
            event.stop_event()
            return

        if not is_admin_event(event) and not await self._can_activate_via_private(
            event_sender_id(event)
        ):
            await event.send(
                event.plain_result("请先在群里使用一次，再私聊机器人发送：激活 激活码")
            )
            event.stop_event()
            return

        current_code = normalize_activation_code(str(state.get("code", "") or ""))
        if normalized_code != current_code:
            await event.send(event.plain_result("激活码不正确，请重新查看激活包"))
            event.stop_event()
            return

        sender_id = event_sender_id(event)
        await self._mark_user_activated(sender_id)
        await self._clear_private_activation_request(sender_id)
        await event.send(event.plain_result("激活成功！请返回群内使用！"))
        event.stop_event()

    async def force_refresh_activation_state(self) -> dict[str, Any]:
        """强制刷新激活状态（管理员命令调用）

        立即生成新的激活包并通知管理员和群组。

        Returns:
            新生成的激活状态
        """
        return await self._force_refresh_activation_state()

    def activation_enabled(self) -> bool:
        """返回激活功能是否启用"""
        return self._activation_enabled()

    def _load_json_file(self, path: Path, *, default: Any) -> Any:
        """从 JSON 文件加载数据

        Args:
            path: JSON 文件路径
            default: 文件不存在或解析失败时的默认值

        Returns:
            加载的数据或默认值
        """
        try:
            if not path.exists():
                return default
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _write_json_file(self, path: Path, payload: Any) -> None:
        """将数据写入 JSON 文件

        使用原子写入方式：先写入临时文件，再重命名，
        避免写入过程中崩溃导致数据损坏。

        Args:
            path: 目标文件路径
            payload: 要写入的数据
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(path.name + ".tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temp_path.replace(path)

    def _activation_enabled(self) -> bool:
        """从配置读取激活功能是否启用"""
        return safe_bool(self._cfg("activation", "enabled", default=False), False)

    def _activation_auto_rotate_enabled(self) -> bool:
        """从配置读取激活码是否自动轮换"""
        return safe_bool(
            self._cfg("activation", "auto_rotate_enabled", default=True), True
        )

    def _activation_rotate_hours(self) -> int:
        """从配置读取激活码轮换周期（小时）"""
        return max(1, min(720, self._cfg_int("activation", "rotate_hours", default=24)))

    def _activation_code_length(self) -> int:
        """从配置读取激活码长度"""
        return max(4, min(16, self._cfg_int("activation", "code_length", default=8)))

    def _activation_quark_cookies(self) -> str:
        """从配置读取夸克网盘 Cookie"""
        return self._cfg_str("activation", "quark_cookie", default="").strip()

    def _activation_quark_save_fid(self) -> str:
        """从配置读取夸克网盘保存目录 FID"""
        return self._cfg_str("activation", "quark_save_fid", default="").strip()

    def _activation_quark_verify_ssl(self) -> bool:
        """从配置读取是否验证夸克 SSL 证书"""
        return safe_bool(
            self._cfg("activation", "quark_verify_ssl", default=True), True
        )

    def _activation_quark_share_expired_type(self) -> int:
        """从配置读取夸克分享链接过期类型"""
        return max(
            1,
            min(4, self._cfg_int("activation", "quark_share_expired_type", default=1)),
        )

    def _activation_openlist_mount_path(self) -> str:
        """从配置读取 OpenList 挂载路径"""
        return normalize_path(
            self._cfg_str("activation", "openlist_mount_path", default="/")
        )

    def _activation_openlist_upload_path(self) -> str:
        """从配置读取 OpenList 上传路径"""
        raw = self._cfg_str(
            "activation", "openlist_upload_path", default="/激活包"
        ).strip()
        return normalize_path(raw or "/激活包")

    def _activation_prewarm_minutes(self) -> int:
        """从配置读取激活包预热时间（分钟）"""
        return max(
            5, min(720, self._cfg_int("activation", "prewarm_minutes", default=30))
        )

    def _activation_private_request_ttl_seconds(self) -> int:
        """计算激活请求的有效期（秒）

        自动轮换时使用轮换周期作为有效期，
        否则使用固定 24 小时。
        """
        if self._activation_auto_rotate_enabled():
            return max(1800, self._activation_rotate_hours() * 3600)
        return 86400

    def _activation_notify_group_uids(self) -> list[str]:
        """从配置读取激活通知群 UID 列表"""
        raw_value = self._cfg("activation", "notify_group_uid", default="")
        values: list[str] = []
        if isinstance(raw_value, list):
            values = [str(item or "").strip() for item in raw_value]
        else:
            text = str(raw_value or "").strip()
            if text:
                values = [
                    part.strip()
                    for part in re.split(r"[\r\n,，;；]+", text)
                    if part.strip()
                ]
        # 去重
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _activation_admin_message(
        self, state: dict[str, Any], *, title: str = "激活码已更新"
    ) -> str:
        """构建发送给管理员的激活码通知消息

        Args:
            state: 激活状态数据
            title: 消息标题

        Returns:
            格式化的通知消息文本
        """
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
            lines.append(
                f"生效时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(activate_at))}"
            )
        expires_at = self._activation_state_expires_at(state)
        if expires_at > 0:
            lines.append(
                f"失效时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expires_at))}"
            )
        return "\n".join(lines)

    async def _is_user_activated(self, user_id: str) -> bool:
        """检查用户是否已激活

        用户必须属于当前激活批次才算已激活。

        Args:
            user_id: 用户 ID

        Returns:
            用户是否已激活
        """
        if not user_id:
            return False
        async with self._activation_lock:
            if not self._activation_state_valid():
                return False
            batch_id = str(self._activation_state.get("batch_id", "") or "")
            record = self._activation_users.get(user_id) or {}
            return str(record.get("batch_id", "") or "") == batch_id

    async def _mark_user_activated(self, user_id: str) -> None:
        """标记用户为已激活

        Args:
            user_id: 用户 ID
        """
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
            self._persist_activation_state_locked()

    async def _mark_private_activation_request(
        self, user_id: str, group_id: str, state: dict[str, Any]
    ) -> None:
        """记录用户的激活请求（用于私聊激活验证）

        当用户在群聊中触发激活提示时，记录该请求，
        用户随后需要在私聊中发送正确的激活码。

        Args:
            user_id: 用户 ID
            group_id: 群组 ID
            state: 当前激活状态
        """
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
        """检查用户是否可以通过私聊激活

        用户必须先在群聊中触发过激活提示，才能在私聊中激活。

        Args:
            user_id: 用户 ID

        Returns:
            用户是否可以私聊激活
        """
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
        """清除用户的激活请求记录（激活成功后调用）

        Args:
            user_id: 用户 ID
        """
        if not user_id:
            return
        async with self._activation_lock:
            if self._activation_requests.pop(user_id, None) is not None:
                self._persist_activation_state_locked()

    async def _notify_admins_activation_state(self, state: dict[str, Any]) -> int:
        """向所有管理员发送激活码更新通知

        Args:
            state: 新的激活状态

        Returns:
            成功发送的通知数量
        """
        if not self._activation_state_valid(state):
            return 0
        async with self._activation_lock:
            origins = [
                str((record or {}).get("origin", "") or "").strip()
                for record in self._activation_admin_origins.values()
                if isinstance(record, dict)
                and str((record or {}).get("origin", "") or "").strip()
            ]
        if not origins:
            return 0
        message_text = self._activation_admin_message(state)
        sent_count = 0
        for origin in list(dict.fromkeys(origins)):
            try:
                await self.context.send_message(
                    origin, MessageChain().message(message_text)
                )
                sent_count += 1
            except Exception as exc:
                logger.warning(f"向管理员推送激活码失败：{exc}")
        return sent_count

    async def _notify_group_activation_state(self, state: dict[str, Any]) -> bool:
        """向配置的群组发送激活链接更新通知

        Args:
            state: 新的激活状态

        Returns:
            是否至少发送成功一个群组
        """
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
        """计算激活包在 OpenList 中的目标目录"""
        mount_path = self._activation_openlist_mount_path()
        upload_path = self._activation_openlist_upload_path()
        if upload_path == "/":
            return mount_path
        return normalize_path(posixpath.join(mount_path, upload_path.lstrip("/")))

    def _activation_state_valid(self, state: dict[str, Any] | None = None) -> bool:
        """检查激活状态是否有效

        有效的激活状态必须包含：批次 ID、激活码、分享链接、生效时间。

        Args:
            state: 待检查的状态，None 时检查当前状态

        Returns:
            状态是否有效
        """
        target = state if isinstance(state, dict) else self._activation_state
        batch_id = str(target.get("batch_id", "") or "").strip()
        code = normalize_activation_code(str(target.get("code", "") or ""))
        share_url = str(target.get("share_url", "") or "").strip()
        try:
            activate_at = float(
                target.get("activate_at", target.get("created_at", 0)) or 0
            )
        except (TypeError, ValueError):
            activate_at = 0.0
        return bool(batch_id and code and share_url and activate_at > 0)

    def _activation_state_activate_at(
        self, state: dict[str, Any] | None = None
    ) -> float:
        """获取激活状态的生效时间"""
        target = state if isinstance(state, dict) else self._activation_state
        try:
            return float(target.get("activate_at", target.get("created_at", 0)) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _activation_state_expires_at(
        self, state: dict[str, Any] | None = None
    ) -> float:
        """获取激活状态的过期时间"""
        target = state if isinstance(state, dict) else self._activation_state
        try:
            return float(target.get("expires_at", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _activation_state_has_expiry(self, state: dict[str, Any] | None = None) -> bool:
        """检查激活状态是否设置了过期时间"""
        return self._activation_state_expires_at(state) > 0

    def _activation_pending_matches_current(
        self, current: dict[str, Any], pending: dict[str, Any]
    ) -> bool:
        """检查待生效状态是否匹配当前批次

        用于判断是否应该切换到待生效状态。
        """
        if not self._activation_state_valid(pending):
            return False
        prepared_for_batch = str(pending.get("prepared_for_batch_id", "") or "").strip()
        current_batch = str(current.get("batch_id", "") or "").strip()
        if not current_batch:
            return True
        return prepared_for_batch in ("", current_batch)

    def _activation_pending_ready(
        self, current: dict[str, Any], pending: dict[str, Any]
    ) -> bool:
        """检查待生效状态是否已就绪（生效时间已到）"""
        if not self._activation_pending_matches_current(current, pending):
            return False
        return self._activation_state_activate_at(pending) <= time.time()

    def _activation_should_prewarm(
        self, current: dict[str, Any], pending: dict[str, Any]
    ) -> bool:
        """判断是否应该预热新的激活包

        条件：激活功能启用、自动轮换启用、当前状态有效、
        当前状态接近过期（在预热窗口内）。
        """
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
        """安全删除文件（忽略不存在错误）"""
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    def _persist_activation_state_locked(self) -> None:
        """持久化所有激活状态到磁盘（必须在锁内调用）"""
        self._write_json_file(self._activation_state_path, self._activation_state)
        self._write_json_file(self._activation_users_path, self._activation_users)
        self._write_json_file(self._activation_requests_path, self._activation_requests)
        self._write_json_file(
            self._activation_admin_origins_path, self._activation_admin_origins
        )
        if self._activation_pending_state:
            self._write_json_file(
                self._activation_pending_state_path, self._activation_pending_state
            )
        else:
            self._remove_file_if_exists(self._activation_pending_state_path)

    async def _cleanup_previous_activation_upload(
        self, previous_state: dict[str, Any]
    ) -> None:
        """清理之前的激活包上传（预留接口，当前未实现）"""
        return

    async def _cleanup_activation_states(self, *states: dict[str, Any]) -> None:
        """批量清理过期的激活状态"""
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
        """激活新的激活状态

        将给定的状态设为当前生效状态，重置已激活用户列表，
        并通知管理员和群组。

        Args:
            state: 新的激活状态

        Returns:
            激活后的状态
        """
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
        """将待生效状态提升为当前状态"""
        async with self._activation_lock:
            current = dict(self._activation_state)
            pending = dict(self._activation_pending_state)
        if not self._activation_pending_ready(current, pending):
            return current
        return await self._activate_state(pending)

    def _schedule_activation_prewarm(self) -> None:
        """调度激活包预热任务"""
        if not self._activation_enabled() or not self._activation_auto_rotate_enabled():
            return
        task = self._activation_prewarm_task
        if task is not None and not task.done():
            return
        self._activation_prewarm_task = asyncio.create_task(
            self._run_activation_prewarm()
        )

    async def _run_activation_prewarm(self) -> None:
        """执行激活包预热"""
        try:
            await self._prepare_pending_activation_state()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"激活码预生成失败：{exc}")
        finally:
            self._activation_prewarm_task = None

    async def _prepare_pending_activation_state(self) -> dict[str, Any]:
        """准备待生效的下一批激活状态"""
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
        """构建夸克网盘客户端"""
        cookie = self._activation_quark_cookies()
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
        """确保 OpenList 目录存在（递归创建）"""
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
                if (
                    "exists" in message
                    or "already" in message
                    or "file exists" in message
                ):
                    continue
                raise

    async def _find_quark_uploaded_fid(
        self, client: QuarkPanClient, file_name: str
    ) -> str:
        """在夸克网盘中查找刚上传的文件 FID

        上传完成后需要轮询等待夸克索引更新。

        Args:
            client: 夸克网盘客户端
            file_name: 文件名

        Returns:
            文件 FID，未找到返回空字符串
        """
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
                if as_text(item.get("file_name")) == normalized_name
                and as_text(item.get("file_type")).lower() != "folder"
            ]
            if matches:
                matches.sort(
                    key=lambda item: safe_int(item.get("updated_at"), 0, minimum=0),
                    reverse=True,
                )
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
        """构建激活包中的说明文本

        Args:
            code: 激活码
            created_at: 创建时间戳
            expires_at: 过期时间戳
            activate_at: 生效时间戳（可不同于创建时间）

        Returns:
            格式化的说明文本
        """
        created_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_at))
        effective_activate_at = activate_at if activate_at is not None else created_at
        activate_text = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(effective_activate_at)
        )
        lines = [
            "激活说明",
            f"当前激活码：{code}",
            f"生成时间：{created_text}",
        ]
        if effective_activate_at > created_at + 5:
            lines.append(f"生效时间：{activate_text}")
        if expires_at > 0:
            expires_text = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(expires_at)
            )
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
        """生成新的激活包并上传

        完整流程：
        1. 生成激活码和批次 ID
        2. 创建激活说明文本文件
        3. 打包为 ZIP 文件
        4. 上传到 OpenList
        5. 复制到夸克网盘
        6. 创建夸克分享链接

        Args:
            activate_at: 指定生效时间
            prepared_for_batch_id: 为哪个批次预生成的（用于判断是否应该切换）

        Returns:
            新的激活状态字典
        """
        created_at = time.time()
        effective_activate_at = max(created_at, float(activate_at or created_at))
        expires_at = 0.0
        if self._activation_auto_rotate_enabled():
            expires_at = effective_activate_at + self._activation_rotate_hours() * 3600
        batch_id = (
            time.strftime("%Y%m%d%H%M%S", time.localtime(created_at))
            + "_"
            + uuid4().hex[:8]
        )
        code = generate_activation_code(self._activation_code_length())
        share_title = f"激活包_{time.strftime('%Y%m%d_%H%M%S', time.localtime(created_at))}_{batch_id[-4:]}"
        work_dir = self.activation_root / batch_id
        zip_path = work_dir / f"{share_title}.zip"
        txt_path = work_dir / "激活码.txt"

        work_dir.mkdir(parents=True, exist_ok=True)
        try:
            # 创建激活说明文本
            txt_path.write_text(
                self._build_activation_text(
                    code, created_at, expires_at, activate_at=effective_activate_at
                ),
                encoding="utf-8",
            )
            # 打包为 ZIP
            with zipfile.ZipFile(
                zip_path, "w", compression=zipfile.ZIP_DEFLATED
            ) as archive:
                archive.write(txt_path, arcname="激活码.txt")

            # 上传到 OpenList
            openlist_client = self._build_openlist_client()
            quark_client = self._build_activation_quark_client()
            target_dir = self._activation_openlist_target_dir()
            target_path = normalize_path(posixpath.join(target_dir, zip_path.name))

            await self._ensure_openlist_dir(openlist_client, target_dir)
            await openlist_client.upload_form(zip_path, target_path, overwrite=True)

            # 查找夸克网盘中的文件并创建分享
            uploaded_fid = await self._find_quark_uploaded_fid(
                quark_client, zip_path.name
            )
            if not uploaded_fid:
                raise RuntimeError(
                    "激活包已上传，但没有在夸克目录找到对应文件，请检查 activation.quark_save_fid 是否与上传目录一致"
                )

            _, share_url, password = await quark_client.create_share(
                [uploaded_fid], share_title
            )
            return {
                "batch_id": batch_id,
                "code": code,
                "created_at": created_at,
                "activate_at": effective_activate_at,
                "expires_at": expires_at,
                "share_url": share_url,
                "password": password,
                "title": share_title,
                "cleanup_job": {
                    "provider": "quark",
                    "payload": {"fids": [uploaded_fid]},
                },
                "openlist_target_path": target_path,
                "prepared_for_batch_id": str(prepared_for_batch_id or "").strip(),
            }
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    async def _ensure_activation_package_ready(
        self, force: bool = False
    ) -> dict[str, Any]:
        """确保激活包已准备好

        根据当前状态决定是否需要生成新的激活包。

        Args:
            force: 是否强制刷新

        Returns:
            当前有效的激活状态
        """
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

            new_state = await self._generate_activation_package_state(
                activate_at=time.time()
            )
            return await self._activate_state(new_state)

    async def _force_refresh_activation_state(self) -> dict[str, Any]:
        """强制刷新激活状态（管理员命令）"""
        async with self._activation_generation_lock:
            async with self._activation_lock:
                pending = dict(self._activation_pending_state)
            if self._activation_state_valid(pending):
                return await self._activate_state(pending)

            new_state = await self._generate_activation_package_state(
                activate_at=time.time()
            )
            return await self._activate_state(new_state)

    async def _warmup_activation_package(self) -> None:
        """插件初始化时预热激活包"""
        try:
            await self._ensure_activation_package_ready()
        except Exception as exc:
            logger.warning(f"激活包预热失败：{exc}")

    def _activation_prompt_text(self, state: dict[str, Any]) -> str:
        """生成发送给未激活用户的提示文本"""
        lines = [
            "尚未激活，请打开分享链接解压后获取激活码，然后私聊机器人发送：激活 激活码",
            f"链接：{str(state.get('share_url', '') or '').strip()}",
        ]
        return "\n".join(lines)

    def _cfg(self, *keys: str, default: Any = None) -> Any:
        """安全读取嵌套配置"""
        current: Any = self.config
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
            if current is None:
                return default
        return current

    def _cfg_str(self, *keys: str, default: str = "") -> str:
        """安全读取字符串配置"""
        value = self._cfg(*keys, default=default)
        if value is None:
            return default
        return str(value)

    def _cfg_int(self, *keys: str, default: int = 0) -> int:
        """安全读取整数配置"""
        value = self._cfg(*keys, default=default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default