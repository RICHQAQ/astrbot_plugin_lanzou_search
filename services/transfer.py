from __future__ import annotations

import asyncio
import posixpath
import shutil
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent

from ..clients.napcat_stream import NapCatActionStreamClient, NapCatWsStreamClient
from ..clients.openlist import OpenListClient
from ..core.constants import (
    DOWNLOAD_DIR_RETENTION_SECONDS,
    DOWNLOAD_SEND_GRACE_SECONDS,
    FILE_LINK_CACHE_TTL_SECONDS,
    FILE_MISSING_CACHE_TTL_SECONDS,
    FILE_PREPARE_RETRY_COUNT,
    FILE_PREPARE_RETRY_DELAY_SECONDS,
    FILE_SEND_TOTAL_TIMEOUT_SECONDS,
    OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS,
)
from ..core.event_utils import event_group_id, event_sender_id, is_group_chat
from ..core.models import FileRecordExpiredError, SearchItem
from ..core.utils import (
    encode_openlist_path,
    normalize_path,
    safe_bool,
    safe_int,
    sanitize_filename,
)


class TransferService:
    def __init__(
        self,
        *,
        config: dict[str, Any],
        runtime_root: Path,
        build_openlist_client: Callable[[], OpenListClient],
        request_timeout_seconds: Callable[[], int],
        openlist_base_url: Callable[[], str],
        openlist_download_base_url: Callable[[], str],
    ) -> None:
        self.config = config
        self.runtime_root = runtime_root
        self._build_openlist_client = build_openlist_client
        self._request_timeout_seconds = request_timeout_seconds
        self._openlist_base_url = openlist_base_url
        self._openlist_download_base_url = openlist_download_base_url

        self._pending_download_cleanup: dict[str, float] = {}
        self._file_link_cache: dict[str, dict[str, Any]] = {}
        self._file_link_cache_lock = asyncio.Lock()
        self._file_prepare_condition = asyncio.Condition()
        self._file_prepare_next_ticket = 1
        self._file_prepare_current_ticket = 1
        self._file_prepare_cancelled_tickets: set[int] = set()
        self._file_prepare_active = 0
        self._file_prepare_last_finished_at = 0.0

        self._downloads_root().mkdir(parents=True, exist_ok=True)

    async def send_search_item(self, event: AstrMessageEvent, item: SearchItem) -> None:
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
                await event.send(
                    event.plain_result(
                        f"当前调取较多，已进入队列，前方还有 {queue_ahead} 个"
                    )
                )
            await self._apply_file_prepare_interval()
            send_timeout_seconds = FILE_SEND_TOTAL_TIMEOUT_SECONDS
            if self._offline_fallback_path():
                send_timeout_seconds = max(
                    send_timeout_seconds, self._offline_fallback_timeout_seconds() + 30
                )
            async with asyncio.timeout(send_timeout_seconds):
                resolved_path, download_url = await self._resolve_download_target(
                    client, item.full_path
                )
                if await self._try_send_remote_file(
                    event, item.name, download_url, resolved_path
                ):
                    return
                if await self._try_send_group_remote_file(
                    event, item.name, download_url, resolved_path
                ):
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
                self._schedule_download_dir_cleanup(
                    temp_dir, DOWNLOAD_SEND_GRACE_SECONDS
                )
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
            cached_path = normalize_path(
                str(cached.get("resolved_path", full_path) or full_path)
            )
            if cached_url:
                return cached_path, cached_url

        resolved_path = normalize_path(full_path)
        direct_target = await self._resolve_direct_download_target(
            client, resolved_path, refresh=True
        )
        if direct_target:
            direct_path, direct_url = direct_target
            preferred_url = await self._prefer_openlist_file_link(
                client, direct_path, direct_url
            )
            await self._cache_file_link(full_path, direct_path, preferred_url)
            if direct_path != resolved_path:
                await self._cache_file_link(direct_path, direct_path, preferred_url)
            return direct_path, preferred_url

        try:
            preferred_url = await self._prefer_openlist_file_link(
                client, resolved_path, ""
            )
            if preferred_url:
                await self._cache_file_link(full_path, resolved_path, preferred_url)
                return resolved_path, preferred_url
            raw_url = await self._fetch_raw_url(client, resolved_path)
            await self._cache_file_link(full_path, resolved_path, raw_url)
            return resolved_path, raw_url
        except Exception as exc:
            if not self._is_object_not_found_error(exc):
                raise

        recovered = await self._recover_existing_path(
            client, resolved_path, refresh=True
        )
        recovered_path = recovered[0] if recovered else None
        if not recovered_path:
            await self._cache_missing_file_link(full_path)
            raise FileRecordExpiredError(full_path)

        recovered_direct_url = recovered[1] if recovered else ""
        if recovered_direct_url:
            preferred_url = await self._prefer_openlist_file_link(
                client, recovered_path, recovered_direct_url
            )
            await self._cache_file_link(full_path, recovered_path, preferred_url)
            if recovered_path != resolved_path:
                await self._cache_file_link(
                    recovered_path, recovered_path, preferred_url
                )
            return recovered_path, preferred_url

        try:
            preferred_url = await self._prefer_openlist_file_link(
                client, recovered_path, ""
            )
            if preferred_url:
                await self._cache_file_link(full_path, recovered_path, preferred_url)
                if recovered_path != resolved_path:
                    await self._cache_file_link(
                        recovered_path, recovered_path, preferred_url
                    )
                return recovered_path, preferred_url
            raw_url = await self._fetch_raw_url(client, recovered_path)
        except Exception as exc:
            if not self._is_object_not_found_error(exc):
                raise
            refreshed = await self._recover_existing_path(
                client, recovered_path, refresh=True
            )
            refreshed_path = refreshed[0] if refreshed else None
            if not refreshed_path:
                await self._cache_missing_file_link(full_path)
                raise FileRecordExpiredError(full_path) from exc
            recovered_path = refreshed_path
            refreshed_direct_url = refreshed[1] if refreshed else ""
            if refreshed_direct_url:
                preferred_url = await self._prefer_openlist_file_link(
                    client, recovered_path, refreshed_direct_url
                )
                await self._cache_file_link(full_path, recovered_path, preferred_url)
                if recovered_path != resolved_path:
                    await self._cache_file_link(
                        recovered_path, recovered_path, preferred_url
                    )
                return recovered_path, preferred_url
            preferred_url = await self._prefer_openlist_file_link(
                client, recovered_path, ""
            )
            if preferred_url:
                await self._cache_file_link(full_path, recovered_path, preferred_url)
                if recovered_path != resolved_path:
                    await self._cache_file_link(
                        recovered_path, recovered_path, preferred_url
                    )
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
            resolved_path, download_url = await self._resolve_download_target(
                client, cache_key
            )
            self._remove_partial_file(target_path)
            await self._retry_file_operation(
                lambda: client.download(download_url, target_path),
                action=f"download file {resolved_path}",
            )
        if await self._try_upload_group_file_stream(event, display_name, target_path):
            return
        if await self._try_upload_group_file(event, display_name, target_path):
            return
        if await self._try_send_private_file_fallback(
            event, display_name, target_path, download_url
        ):
            await event.send(event.plain_result("群内直发失败，已改为私发，请查收"))
            return
        await event.send(
            event.chain_result([Comp.File(name=display_name, file=str(target_path))])
        )

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

    async def _fetch_openlist_file_link(
        self, client: OpenListClient, full_path: str
    ) -> str:
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
        link_url = (
            str(wrapper.get("url", "") or "").strip()
            if isinstance(wrapper, dict)
            else ""
        )
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

        task_dir = normalize_path(
            posixpath.join(fallback_root, f"astrbot_lanzou_search_{uuid4().hex}")
        )
        source_url = await self._resolve_offline_fallback_source_url(
            client, resolved_path, download_url
        )
        created_dir = False
        try:
            await self._retry_file_operation(
                lambda: client.mkdir(task_dir),
                action=f"mkdir offline fallback dir {task_dir}",
                attempts=2,
            )
            created_dir = True
            task_id = await self._start_offline_fallback_task(
                client, source_url, task_dir
            )
            if task_id:
                await self._wait_offline_download_task(client, task_id)
            fallback_path, fallback_url = await self._wait_offline_fallback_output(
                client, task_dir
            )
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
            logger.exception(
                "offline fallback failed for %s via %s", cache_key, task_dir
            )
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
        direct_target = await self._resolve_direct_download_target(
            client, resolved_path, refresh=True
        )
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

    async def _wait_offline_download_task(
        self, client: OpenListClient, task_id: str
    ) -> None:
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
            for raw in content if isinstance(content, list) else []:
                if bool(raw.get("is_dir", False)):
                    continue
                candidate_name = str(raw.get("name", "") or "")
                if not candidate_name:
                    continue
                resolved_path = normalize_path(posixpath.join(task_dir, candidate_name))
                sign = str(raw.get("sign", "") or "").strip()
                return resolved_path, self._build_openlist_download_url(
                    resolved_path, sign
                )
            await asyncio.sleep(OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS)
        raise TimeoutError(f"offline fallback output timed out: {task_dir}")

    async def _cleanup_offline_fallback_dir(
        self, client: OpenListClient, task_dir: str
    ) -> None:
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
            logger.warning(
                "cleanup offline fallback dir failed: %s", task_dir, exc_info=True
            )

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
            match = await self._find_child_in_parent(
                client, parent_path, name, refresh=refresh
            )
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
        recovered = await self._recover_existing_path(
            client, full_path, refresh=refresh
        )
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
                lambda: client.list_dir(
                    parent_path, page, page_size, refresh=refresh and page == 1
                ),
                action=f"list parent {parent_path}",
                attempts=2,
            )
            wrapper = data.get("data", data)
            content = wrapper.get("content", [])
            total = safe_int(wrapper.get("total"), 0, minimum=0)
            for raw in content if isinstance(content, list) else []:
                candidate_name = str(raw.get("name", "") or "")
                if not candidate_name or bool(raw.get("is_dir", False)):
                    continue
                if candidate_name == name or candidate_name.casefold() == expected_name:
                    resolved_path = normalize_path(
                        posixpath.join(parent_path, candidate_name)
                    )
                    sign = str(raw.get("sign", "") or "").strip()
                    return resolved_path, self._build_openlist_download_url(
                        resolved_path, sign
                    )
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
            if (
                current.scheme != source_base.scheme
                or current.netloc != source_base.netloc
            ):
                return raw
            desired = urlsplit(target_base)
            return urlunsplit(
                (
                    desired.scheme,
                    desired.netloc,
                    current.path,
                    current.query,
                    current.fragment,
                )
            )
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

    async def _cache_file_link(
        self, cache_key: str, resolved_path: str, raw_url: str
    ) -> None:
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
        while (
            not self._file_prepare_active
            and self._file_prepare_current_ticket
            in self._file_prepare_cancelled_tickets
        ):
            self._file_prepare_cancelled_tickets.discard(
                self._file_prepare_current_ticket
            )
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
                if (
                    ticket == self._file_prepare_current_ticket
                    and not self._file_prepare_active
                ):
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
        if is_group_chat(event):
            return False
        try:
            await event.send(
                event.chain_result([Comp.File(name=name, url=download_url)])
            )
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
        if not is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        group_id = event_group_id(event)
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
            logger.exception(
                "send_group_msg remote file failed for %s via %s",
                resolved_path,
                remote_url,
            )
            return False

    async def _try_upload_group_file(
        self, event: AstrMessageEvent, name: str, target_path: Path
    ) -> bool:
        if not is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        group_id = event_group_id(event)
        if not group_id or not target_path.exists():
            return False
        try:
            await call_action(
                "upload_group_file", group_id=group_id, file=str(target_path), name=name
            )
            return True
        except Exception:
            logger.exception("upload_group_file failed for %s", target_path)
            return False

    def _private_file_resource_variants(
        self, target_path: Path, download_url: str
    ) -> list[str]:
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
        if not is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        user_id = event_sender_id(event)
        if not user_id:
            return False
        for resource in self._private_file_resource_variants(target_path, download_url):
            try:
                await call_action(
                    "send_private_msg",
                    user_id=user_id,
                    message=[
                        {"type": "file", "data": {"file": resource, "name": name}}
                    ],
                )
                return True
            except Exception:
                logger.exception(
                    "send_private_msg file fallback failed for %s via %s",
                    name,
                    resource,
                )
        return False

    async def _try_send_group_file_resource(
        self, event: AstrMessageEvent, name: str, resource: str
    ) -> bool:
        if not is_group_chat(event):
            return False
        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False
        group_id = event_group_id(event)
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
            logger.exception(
                "send_group_msg file resource failed for %s via %s", name, resource
            )
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

    async def _try_upload_group_file_stream(
        self, event: AstrMessageEvent, name: str, target_path: Path
    ) -> bool:
        if not is_group_chat(event):
            return False
        if not self._napcat_group_file_stream_enabled():
            return False
        if not target_path.exists():
            return False

        bot = getattr(event, "bot", None)
        call_action = getattr(bot, "call_action", None) if bot is not None else None
        if not callable(call_action):
            return False

        group_id = event_group_id(event)
        if not group_id:
            return False

        try:
            stream_path = await self._select_napcat_stream_client(
                call_action
            ).upload_file(target_path)
        except Exception:
            logger.exception("upload_file_stream failed for %s", target_path)
            return False

        for resource in self._napcat_stream_resource_variants(stream_path):
            if await self._try_send_group_file_resource(event, name, resource):
                return True

        try:
            await call_action(
                "upload_group_file", group_id=group_id, file=stream_path, name=name
            )
            return True
        except Exception:
            logger.exception(
                "upload_group_file via stream temp failed for %s", target_path
            )
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
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
            ) as exc:
                last_error = exc
                if attempt >= total_attempts:
                    break
                delay = FILE_PREPARE_RETRY_DELAY_SECONDS * attempt
                logger.warning(
                    "%s failed on attempt %s/%s, retrying in %.1fs: %s",
                    action,
                    attempt,
                    total_attempts,
                    delay,
                    exc,
                )
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
        self._pending_download_cleanup[str(path.resolve())] = time.time() + max(
            15, int(delay_seconds)
        )

    async def prune_runtime_state(self) -> None:
        async with self._file_link_cache_lock:
            self._file_link_cache = {
                key: value
                for key, value in self._file_link_cache.items()
                if isinstance(value, dict)
                and float(value.get("expires_at", 0) or 0) > time.time()
            }

    def cleanup_download_dirs(self, *, force_all: bool) -> None:
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
                should_delete = (
                    now - child.stat().st_mtime
                ) > DOWNLOAD_DIR_RETENTION_SECONDS
            if should_delete:
                active_cleanup.pop(child_key, None)
                shutil.rmtree(child, ignore_errors=True)

        stale_keys = [key for key in active_cleanup if not Path(key).exists()]
        for key in stale_keys:
            active_cleanup.pop(key, None)

    def _downloads_root(self) -> Path:
        return self.runtime_root / "downloads"

    def _offline_fallback_path(self) -> str:
        raw_value = self._cfg_str(
            "openlist", "offline_fallback_path", default=""
        ).strip()
        return normalize_path(raw_value) if raw_value else ""

    def _offline_fallback_timeout_seconds(self) -> int:
        return max(
            30,
            min(
                1800,
                self._cfg_int(
                    "openlist", "offline_fallback_timeout_seconds", default=300
                ),
            ),
        )

    def _napcat_group_file_stream_enabled(self) -> bool:
        return safe_bool(
            self._cfg("napcat", "group_file_stream_enabled", default=False), False
        )

    def _napcat_ws_url(self) -> str:
        return self._cfg_str("napcat", "ws_url", default="").strip()

    def _napcat_access_token(self) -> str:
        return self._cfg_str("napcat", "access_token", default="").strip()

    def _napcat_stream_chunk_size_bytes(self) -> int:
        kb = max(
            16, min(1024, self._cfg_int("napcat", "stream_chunk_size_kb", default=64))
        )
        return kb * 1024

    def _napcat_stream_file_retention_seconds(self) -> int:
        return max(
            10,
            min(
                600,
                self._cfg_int("napcat", "stream_file_retention_seconds", default=30),
            ),
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

    def _file_prepare_interval_seconds(self) -> float:
        raw_value = self._cfg("file_prepare_interval_seconds", default=2)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = 2.0
        return max(0.0, min(30.0, value))

    def _file_parent_scan_page_size(self) -> int:
        return max(
            100, min(1000, self._cfg_int("file_parent_scan_page_size", default=500))
        )

    def _file_parent_scan_max_pages(self) -> int:
        return max(1, min(200, self._cfg_int("file_parent_scan_max_pages", default=40)))

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
