from __future__ import annotations

import asyncio
import hashlib
import posixpath
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent

from ..clients.openlist import OpenListClient
from ..core.constants import (
    DEFAULT_SELECTION_DEDUP_WINDOW_SECONDS,
    MAX_SOURCES,
    PAGE_SIZE,
)
from ..core.event_utils import (
    event_group_id,
    event_message_token,
    event_reply_component,
    event_self_id,
    event_sender_id,
    event_sender_scope,
    event_session_key,
    is_group_chat,
    reply_body_text,
)
from ..core.models import SearchItem, SearchPage, SourceConfig
from ..core.utils import (
    format_search_results,
    hash_session_body,
    normalize_path,
    normalize_session_body,
    resolve_search_root,
    safe_int,
    total_pages,
)


class SearchService:
    def __init__(
        self,
        *,
        config: dict[str, Any],
        build_openlist_client: Callable[[], OpenListClient],
        send_search_item: Callable[[AstrMessageEvent, SearchItem], Awaitable[None]],
        ensure_group_access: Callable[[AstrMessageEvent], Awaitable[bool]],
    ) -> None:
        self.config = config
        self._build_openlist_client = build_openlist_client
        self._send_search_item = send_search_item
        self._ensure_group_access = ensure_group_access

        self._search_cache: dict[str, dict[str, Any]] = {}
        self._search_locks: dict[str, asyncio.Lock] = {}
        self._search_sessions: dict[str, dict[str, Any]] = {}
        self._group_search_threads: dict[str, dict[str, Any]] = {}
        self._group_search_renders: dict[str, dict[str, Any]] = {}
        self._selection_event_cache: dict[str, float] = {}
        self._state_lock = asyncio.Lock()
        self._selection_event_lock = asyncio.Lock()

    async def search_flow(self, event: AstrMessageEvent, keyword: str):
        keyword = keyword.strip()
        if not keyword:
            yield event.plain_result(
                "用法：sh 关键词\n"
                "私聊直接发送序号 / n / p / q，群里请引用结果消息后再发送序号 / n / p / q。"
            )
            event.stop_event()
            return

        await self.prune_runtime_state()

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

        if is_group_chat(event):
            thread = await self._create_group_search_thread(event, page_state)
            _, body = await self._build_group_render_page(thread, page_state)
            if body:
                yield self._group_results_chain(event, body)
            event.stop_event()
            return

        session_key = event_session_key(event)
        session = {
            "token": uuid4().hex,
            "session_key": session_key,
            "sender_id": event_sender_id(event),
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

    async def handle_followup(self, event: AstrMessageEvent, text: str) -> None:
        await self.prune_runtime_state()

        if is_group_chat(event):
            command = text.lower()
            is_numeric = bool(re.fullmatch(r"\d+", text))
            if command not in {"q", "退出", "n", "p"} and not is_numeric:
                return
            render, thread = await self._resolve_group_render_from_reply(event)
            if not render or not thread:
                return
            if not await self._ensure_group_access(event):
                return
            await self._handle_group_search_input(event, text)
            return

        session_key = event_session_key(event)
        session = await self._get_private_search_session(session_key)
        if not session:
            return
        if str(session.get("sender_id", "")) != event_sender_id(event):
            return

        command = text.lower()
        is_numeric = bool(re.fullmatch(r"\d+", text))
        if command not in {"q", "退出", "n", "p"} and not is_numeric:
            return

        if not await self._consume_selection_event(event, session_key):
            event.stop_event()
            return

        if command in {"q", "退出"}:
            closed = await self._close_private_search_session(
                session_key, event_sender_id(event)
            )
            if closed:
                await event.send(event.plain_result("当前检索会话已结束。"))
                event.stop_event()
            return

        if command in {"n", "p"}:
            active_session = await self._begin_private_search_action(
                session_key, event_sender_id(event)
            )
            if not active_session:
                event.stop_event()
                return

            token = str(active_session.get("token", ""))
            try:
                current_page = safe_int(
                    active_session.get("current_page"), 1, minimum=1
                )
                target_page = self._target_page_for_command(
                    current_page=current_page,
                    command=command,
                    total=safe_int(active_session.get("total"), 0, minimum=0),
                    has_more=bool(active_session.get("has_more", False)),
                )
                if target_page == current_page:
                    event.stop_event()
                    return
                page_state = await self._search_page(
                    str(active_session.get("keyword", "")), target_page
                )
                updated = await self._replace_private_search_page(
                    session_key, token, page_state
                )
                if updated:
                    await event.send(
                        event.plain_result(self._render_private_page(updated))
                    )
                    event.stop_event()
            except Exception as exc:
                logger.exception("private search page failed")
                await event.send(event.plain_result(f"处理失败：{exc}"))
                event.stop_event()
            finally:
                await self._finish_private_search_action(session_key, token)
            return

        active_session = await self._touch_private_search_session(
            session_key, event_sender_id(event)
        )
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
            await self._send_search_item(event, items[index - 1])
            event.stop_event()
        finally:
            await self._touch_private_search_session(
                session_key, event_sender_id(event)
            )

    async def _handle_group_search_input(
        self, event: AstrMessageEvent, text: str
    ) -> None:
        command = text.lower()
        is_numeric = bool(re.fullmatch(r"\d+", text))
        if command not in {"q", "退出", "n", "p"} and not is_numeric:
            return

        group_key = event_group_id(event) or event_sender_scope(event)
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
                page_state = await self._search_page(
                    str(thread_for_page.get("keyword", "")), target_page
                )
                updated_thread = await self._replace_group_thread_page(
                    search_id, page_state
                )
                if not updated_thread:
                    event.stop_event()
                    return
                _, body = await self._build_group_render_page(
                    updated_thread, page_state
                )
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
            await self._send_search_item(event, items[index - 1])
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

        # The cache stores an incremental merge state so later pages can reuse
        # already scanned sources instead of restarting the whole crawl.
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

            if (
                bool(state.get("complete", False))
                or len(state.get("items") or []) >= required_items
            ):
                self._search_cache[cache_key] = state
                return self._public_search_state(state)

            client = self._build_openlist_client()
            first_error: Exception | None = None
            success_count = 0
            target_items = max(
                required_items, len(state.get("items") or []) + preload_target
            )

            while (
                not bool(state.get("complete", False))
                and len(state.get("items") or []) < target_items
            ):
                source_index = safe_int(state.get("source_index"), 0, minimum=0)
                if source_index >= len(sources):
                    state["complete"] = True
                    break

                source = sources[source_index]
                page_number = safe_int(state.get("next_page"), 1, minimum=1)

                try:
                    content, total, parent = await self._search_source_page(
                        client, source, keyword, page_number
                    )
                    success_count += 1
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                    logger.warning(
                        "source search failed for %s page %s: %s",
                        source.mount_path,
                        page_number,
                        exc,
                    )
                    state["source_index"] = source_index + 1
                    state["next_page"] = 1
                    continue

                for raw in content:
                    name = str(raw.get("name", "") or "")
                    if not name or bool(raw.get("is_dir", False)):
                        continue
                    item_parent = normalize_path(
                        str(raw.get("parent", parent) or parent)
                    )
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

                if (
                    len(content) < self._openlist_search_fetch_page_size()
                    or page_number * self._openlist_search_fetch_page_size() >= total
                ):
                    state["source_index"] = source_index + 1
                    state["next_page"] = 1
                else:
                    state["next_page"] = page_number + 1

                self._touch_search_state(state)

            if (
                not state.get("items")
                and success_count == 0
                and first_error is not None
            ):
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

    async def prune_runtime_state(self) -> None:
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
                if isinstance(value, dict)
                and float(value.get("expires_at", 0) or 0) > time.time()
            }

    async def _get_private_search_session(
        self, session_key: str
    ) -> dict[str, Any] | None:
        async with self._state_lock:
            session = self._search_sessions.get(session_key)
            return dict(session) if session else None

    async def _set_private_search_session(
        self, session_key: str, session: dict[str, Any]
    ) -> None:
        async with self._state_lock:
            self._search_sessions[session_key] = dict(session)

    async def _clear_private_search_session(
        self, session_key: str, token: str = ""
    ) -> None:
        async with self._state_lock:
            current = self._search_sessions.get(session_key)
            if current and token and str(current.get("token", "")) != token:
                return
            self._search_sessions.pop(session_key, None)

    async def _close_private_search_session(
        self, session_key: str, sender_id: str
    ) -> bool:
        session = await self._get_private_search_session(session_key)
        if not session or str(session.get("sender_id", "")) != sender_id:
            return False
        await self._clear_private_search_session(
            session_key, str(session.get("token", ""))
        )
        return True

    async def _begin_private_search_action(
        self, session_key: str, sender_id: str
    ) -> dict[str, Any] | None:
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

    async def _touch_private_search_session(
        self, session_key: str, sender_id: str
    ) -> dict[str, Any] | None:
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

    async def _consume_selection_event(
        self, event: AstrMessageEvent, session_key: str
    ) -> bool:
        message_token = event_message_token(event)
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

    async def _create_group_search_thread(
        self, event: AstrMessageEvent, page_state: SearchPage
    ) -> dict[str, Any]:
        thread = {
            "id": uuid4().hex,
            "sender_id": event_sender_id(event),
            "group_id": event_group_id(event),
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

    async def _store_group_render(
        self, thread: dict[str, Any], page_state: SearchPage, body: str
    ) -> dict[str, Any]:
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

    async def _begin_group_search_action(
        self, render: dict[str, Any]
    ) -> dict[str, Any] | None:
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

    async def _touch_group_search_thread(
        self, render: dict[str, Any]
    ) -> dict[str, Any] | None:
        sender_id = str(render.get("sender_id", ""))
        group_id = str(render.get("group_id", ""))
        search_id = str(render.get("search_id", ""))
        async with self._state_lock:
            thread = self._group_search_threads.get(search_id)
            if not thread:
                return None
            if (
                str(thread.get("sender_id", "")) != sender_id
                or str(thread.get("group_id", "")) != group_id
            ):
                return None
            thread["updated_at"] = time.time()
            self._group_search_threads[search_id] = thread
            return dict(thread)

    async def _replace_group_thread_page(
        self, search_id: str, page_state: SearchPage
    ) -> dict[str, Any] | None:
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
        reply_component = event_reply_component(event)
        if reply_component is None:
            return None, None

        self_id = event_self_id(event)
        reply_sender_id = str(getattr(reply_component, "sender_id", "") or "").strip()
        if self_id and reply_sender_id and self_id != reply_sender_id:
            return None, None

        group_id = event_group_id(event)
        sender_id = event_sender_id(event)
        if not group_id or not sender_id:
            return None, None

        reply_body = reply_body_text(reply_component)
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
            candidates.sort(
                key=lambda item: abs(
                    safe_int(item.get("created_at"), 0, minimum=0) - reply_time
                )
            )
        else:
            candidates.sort(
                key=lambda item: safe_int(item.get("created_at"), 0, minimum=0),
                reverse=True,
            )

        for render in candidates:
            thread = await self._get_group_search_thread(
                str(render.get("search_id", ""))
            )
            if not thread:
                continue
            if (
                str(thread.get("sender_id", "")) != sender_id
                or str(thread.get("group_id", "")) != group_id
            ):
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
                Comp.At(qq=event_sender_id(event)),
                Comp.Plain(text=f"\n{body}"),
            ]
        )

    def _search_cache_key(self, keyword: str, sources: list[SourceConfig]) -> str:
        signature = "|".join(
            f"{source.mount_path}>{source.search_path}" for source in sources
        )
        return hashlib.sha1(
            f"{self._openlist_base_url()}|{keyword}|{signature}|{self._openlist_search_fetch_page_size()}".encode()
        ).hexdigest()

    def _target_page_for_command(
        self, *, current_page: int, command: str, total: int, has_more: bool
    ) -> int:
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

    def _openlist_base_url(self) -> str:
        return self._cfg_str(
            "openlist", "base_url", default="http://host.docker.internal:5244"
        ).rstrip("/")

    def _session_timeout_seconds(self) -> int:
        return max(15, self._cfg_int("session_timeout_seconds", default=600))

    def _search_cache_ttl_seconds(self) -> int:
        return max(0, self._cfg_int("scan_cache_ttl_seconds", default=300))

    def _openlist_search_fetch_page_size(self) -> int:
        return max(
            10, min(500, self._cfg_int("openlist_search_fetch_page_size", default=100))
        )

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
