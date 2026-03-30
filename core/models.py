"""
数据模型模块

定义插件中使用的核心数据结构，包括搜索来源配置、搜索结果条目、
搜索分页状态、文件转存结果等。所有模型均使用 dataclass 定义，
启用 slots 以优化内存占用和属性访问性能。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class SourceConfig:
    """搜索来源配置

    对应 OpenList 中已挂载的一个目录来源，描述该来源在搜索时的挂载路径和搜索范围。

    Attributes:
        slot: 来源序号，用于标识和排序（从 1 开始）
        label: 来源的显示名称，用于向用户展示，默认为 "来源N"
        mount_path: OpenList 中的挂载路径，如 "/阿里云盘"
        search_path: 该来源下的搜索起始路径，如 "/影视" 表示只在挂载目录的影视子目录中搜索
    """

    slot: int
    label: str
    mount_path: str
    search_path: str


@dataclass(slots=True)
class SearchItem:
    """单条搜索结果

    表示搜索到的一个文件条目，包含文件名、完整路径和文件大小。

    Attributes:
        name: 文件名（不含路径），如 "movie.mp4"
        full_path: 文件在 OpenList 中的完整路径，如 "/阿里云盘/影视/movie.mp4"
        size: 文件大小（字节），默认为 0
    """

    name: str
    full_path: str
    size: int = 0


@dataclass(slots=True)
class SearchPage:
    """搜索结果分页状态

    表示一次搜索查询的分页结果，包含关键词、当前页码、总数、
    当前页的结果条目列表以及分页元信息。

    Attributes:
        keyword: 搜索关键词
        page: 当前页码（从 1 开始）
        total: 匹配的结果总数（当 total_exact=False 时为已知的最小值）
        items: 当前页的搜索结果列表
        loaded_count: 已加载的结果总数（跨所有来源累加后的去重总数）
        has_more: 是否还有更多未加载的结果（来源尚未全部扫描完毕时为 True）
        total_exact: total 是否为精确值（所有来源均已扫描完毕时为 True）
    """

    keyword: str
    page: int
    total: int
    items: list[SearchItem]
    loaded_count: int = 0
    has_more: bool = False
    total_exact: bool = True


class FileRecordExpiredError(RuntimeError):
    """文件记录已失效异常

    当用户尝试获取一个已过期或已被删除的搜索结果条目时抛出。
    通常发生在用户翻出了很久之前的搜索结果并尝试获取文件时，
    此时底层存储中的文件可能已经被移除。
    """


@dataclass(slots=True)
class TransferResult:
    """文件转存/分享操作的结果

    表示将文件上传到夸克网盘并创建分享链接后的操作结果。

    Attributes:
        success: 操作是否成功
        title: 分享标题
        share_url: 夸克网盘分享链接
        password: 分享链接的提取码（可能为空）
        message: 失败时的错误信息，成功时为空
        cleanup_job: 后续清理任务描述，包含提供者和负载信息，
                     用于在上传后清理临时文件。格式示例：
                     {"provider": "quark", "payload": {"fids": ["xxx"]}}
    """

    success: bool
    title: str = ""
    share_url: str = ""
    password: str = ""
    message: str = ""
    cleanup_job: dict[str, Any] | None = None
