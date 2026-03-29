from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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
