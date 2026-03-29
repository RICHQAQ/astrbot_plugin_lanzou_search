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
        self.context = context
        self.config = config
        self.runtime_root = runtime_root
        self.activation_root = activation_root
        self._build_openlist_client = build_openlist_client
        self._request_timeout_seconds = request_timeout_seconds

        self._activation_state: dict[str, Any] = {}
        self._activation_pending_state: dict[str, Any] = {}
        self._activation_users: dict[str, dict[str, Any]] = {}
        self._activation_requests: dict[str, dict[str, Any]] = {}
        self._activation_admin_origins: dict[str, dict[str, Any]] = {}

        self._activation_state_path = runtime_root / ACTIVATION_STATE_FILE
        self._activation_pending_state_path = (
            runtime_root / ACTIVATION_PENDING_STATE_FILE
        )
        self._activation_users_path = runtime_root / ACTIVATION_USERS_FILE
        self._activation_requests_path = runtime_root / ACTIVATION_REQUESTS_FILE
        self._activation_admin_origins_path = (
            runtime_root / ACTIVATION_ADMIN_ORIGINS_FILE
        )

        self._activation_lock = asyncio.Lock()
        self._activation_generation_lock = asyncio.Lock()
        self._activation_prewarm_task: asyncio.Task[Any] | None = None

    async def initialize(self) -> None:
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
        if self._activation_prewarm_task is not None:
            self._activation_prewarm_task.cancel()
            try:
                await self._activation_prewarm_task
            except asyncio.CancelledError:
                pass

    async def tick(self) -> None:
        if self._activation_enabled() and self._activation_auto_rotate_enabled():
            try:
                await self._ensure_activation_package_ready()
            except Exception as exc:
                logger.warning(f"激活包轮换检查失败：{exc}")

    async def prune_runtime_state(self) -> None:
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
        return await self._force_refresh_activation_state()

    def activation_enabled(self) -> bool:
        return self._activation_enabled()

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
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temp_path.replace(path)

    def _activation_enabled(self) -> bool:
        return safe_bool(self._cfg("activation", "enabled", default=False), False)

    def _activation_auto_rotate_enabled(self) -> bool:
        return safe_bool(
            self._cfg("activation", "auto_rotate_enabled", default=True), True
        )

    def _activation_rotate_hours(self) -> int:
        return max(1, min(720, self._cfg_int("activation", "rotate_hours", default=24)))

    def _activation_code_length(self) -> int:
        return max(4, min(16, self._cfg_int("activation", "code_length", default=8)))

    def _activation_quark_cookie(self) -> str:
        return self._cfg_str("activation", "quark_cookie", default="").strip()

    def _activation_quark_save_fid(self) -> str:
        return self._cfg_str("activation", "quark_save_fid", default="").strip()

    def _activation_quark_verify_ssl(self) -> bool:
        return safe_bool(
            self._cfg("activation", "quark_verify_ssl", default=True), True
        )

    def _activation_quark_share_expired_type(self) -> int:
        return max(
            1,
            min(4, self._cfg_int("activation", "quark_share_expired_type", default=1)),
        )

    def _activation_openlist_mount_path(self) -> str:
        return normalize_path(
            self._cfg_str("activation", "openlist_mount_path", default="/")
        )

    def _activation_openlist_upload_path(self) -> str:
        raw = self._cfg_str(
            "activation", "openlist_upload_path", default="/激活包"
        ).strip()
        return normalize_path(raw or "/激活包")

    def _activation_prewarm_minutes(self) -> int:
        return max(
            5, min(720, self._cfg_int("activation", "prewarm_minutes", default=30))
        )

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
                values = [
                    part.strip()
                    for part in re.split(r"[\r\n,，;；]+", text)
                    if part.strip()
                ]
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
            self._persist_activation_state_locked()

    async def _mark_private_activation_request(
        self, user_id: str, group_id: str, state: dict[str, Any]
    ) -> None:
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
            activate_at = float(
                target.get("activate_at", target.get("created_at", 0)) or 0
            )
        except (TypeError, ValueError):
            activate_at = 0.0
        return bool(batch_id and code and share_url and activate_at > 0)

    def _activation_state_activate_at(
        self, state: dict[str, Any] | None = None
    ) -> float:
        target = state if isinstance(state, dict) else self._activation_state
        try:
            return float(target.get("activate_at", target.get("created_at", 0)) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _activation_state_expires_at(
        self, state: dict[str, Any] | None = None
    ) -> float:
        target = state if isinstance(state, dict) else self._activation_state
        try:
            return float(target.get("expires_at", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _activation_state_has_expiry(self, state: dict[str, Any] | None = None) -> bool:
        return self._activation_state_expires_at(state) > 0

    def _activation_pending_matches_current(
        self, current: dict[str, Any], pending: dict[str, Any]
    ) -> bool:
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
        if not self._activation_pending_matches_current(current, pending):
            return False
        return self._activation_state_activate_at(pending) <= time.time()

    def _activation_should_prewarm(
        self, current: dict[str, Any], pending: dict[str, Any]
    ) -> bool:
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
        # The live implementation already behaves as a no-op here. Keep it
        # unchanged so the refactor does not add new cleanup side effects.
        return

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
        self._activation_prewarm_task = asyncio.create_task(
            self._run_activation_prewarm()
        )

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
            txt_path.write_text(
                self._build_activation_text(
                    code, created_at, expires_at, activate_at=effective_activate_at
                ),
                encoding="utf-8",
            )
            with zipfile.ZipFile(
                zip_path, "w", compression=zipfile.ZIP_DEFLATED
            ) as archive:
                archive.write(txt_path, arcname="激活码.txt")

            openlist_client = self._build_openlist_client()
            quark_client = self._build_activation_quark_client()
            target_dir = self._activation_openlist_target_dir()
            target_path = normalize_path(posixpath.join(target_dir, zip_path.name))

            await self._ensure_openlist_dir(openlist_client, target_dir)
            await openlist_client.upload_form(zip_path, target_path, overwrite=True)

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
