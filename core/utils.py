"""
工具函数模块

提供插件中通用的辅助函数，包括类型安全的值转换、命令解析、
路径规范化、文件名清理、搜索结果格式化等功能。
"""

from __future__ import annotations

import hashlib
import math
import posixpath
import re
import secrets
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from .constants import (
    ACTIVATION_CODE_ALPHABET,
    ACTIVATION_COMMAND_ALIASES,
    COMMAND_NAME,
    PAGE_SIZE,
)
from .models import SearchItem


def safe_int(
    value: Any, default: int, minimum: int | None = None, maximum: int | None = None
) -> int:
    """安全地将任意值转换为整数

    当输入值无法转换时返回默认值，支持可选的最小/最大值约束。

    Args:
        value: 待转换的输入值，可以是任意类型
        default: 转换失败时的默认返回值
        minimum: 可选的最小值约束，转换结果不会小于此值
        maximum: 可选的最大值约束，转换结果不会大于此值

    Returns:
        转换后的整数值，受 minimum/maximum 约束
    """
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def safe_bool(value: Any, default: bool) -> bool:
    """安全地将任意值转换为布尔值

    支持 bool/int/float 类型的直接转换，以及字符串 "true"/"false"/"1"/"0" 等的解析。

    Args:
        value: 待转换的输入值
        default: 无法识别时的默认返回值

    Returns:
        转换后的布尔值
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def extract_command_keyword(raw_text: str, command: str) -> str:
    """从消息文本中提取命令后的关键词

    支持带前缀（/ ! # .）的命令格式，返回命令后面的内容。
    例如 "/sh 电影名" -> "电影名"

    Args:
        raw_text: 原始消息文本
        command: 命令名称（不含前缀）

    Returns:
        提取出的关键词，如果消息不匹配命令格式则返回原始文本
    """
    text = (raw_text or "").strip()
    if not text:
        return ""
    match = re.match(
        rf"^(?:[/!#.])?{re.escape(command)}(?:\s+(.*))?$", text, flags=re.IGNORECASE
    )
    if match:
        return (match.group(1) or "").strip()
    return text


def parse_search_command(raw_text: str) -> str | None:
    """解析搜索命令，提取搜索关键词

    仅当消息文本匹配搜索命令格式时返回关键词，否则返回 None。
    用于判断一条消息是否是搜索命令。

    Args:
        raw_text: 原始消息文本

    Returns:
        搜索关键词（如果匹配搜索命令），或 None（不匹配）
    """
    text = (raw_text or "").strip()
    if not text:
        return None
    match = re.match(
        rf"^(?:[/!#.])?{re.escape(COMMAND_NAME)}(?:\s+(.*))?$",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return (match.group(1) or "").strip()
    return None


def parse_activation_command(raw_text: str) -> str | None:
    """解析激活命令，提取激活码

    支持多种激活命令别名（如"激活"、"jh"），返回提取并规范化后的激活码。
    用于判断一条消息是否是激活命令。

    Args:
        raw_text: 原始消息文本

    Returns:
        规范化后的激活码（大写、去空格），或 None（不匹配）
    """
    text = (raw_text or "").strip()
    if not text:
        return None
    for alias in ACTIVATION_COMMAND_ALIASES:
        match = re.match(
            rf"^(?:[/!#.])?{re.escape(alias)}(?:\s+(.*))?$", text, flags=re.IGNORECASE
        )
        if match:
            code = str(match.group(1) or "").strip()
            return normalize_activation_code(code)
    return None


def normalize_session_body(text: str) -> str:
    """规范化消息正文文本

    统一换行符、去除行尾空白、压缩多余空行，用于消息内容的比对。

    Args:
        text: 原始消息正文

    Returns:
        规范化后的文本
    """
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized


def hash_session_body(text: str) -> str:
    """计算消息正文的 SHA1 哈希值

    先对文本进行规范化处理，再计算哈希，用于群聊中通过引用消息
    匹配搜索结果的渲染记录。

    Args:
        text: 消息正文

    Returns:
        SHA1 哈希的十六进制字符串
    """
    normalized = normalize_session_body(text)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def normalize_activation_code(code: str) -> str:
    """规范化激活码

    去除所有空白字符并转换为大写，统一激活码的存储和比较格式。

    Args:
        code: 原始激活码

    Returns:
        规范化后的激活码（大写、无空格）
    """
    return re.sub(r"\s+", "", str(code or "").upper())


def normalize_path(path: str) -> str:
    """规范化文件路径

    确保路径以 "/" 开头，使用 POSIX 风格路径（正斜杠），
    去除 "." 和 ".." 等相对路径组件。

    Args:
        path: 原始路径字符串

    Returns:
        规范化后的绝对路径，如 "/阿里云盘/影视"
    """
    text = "" if path is None else str(path)
    if not text.strip():
        return "/"
    normalized = posixpath.normpath(text if text.startswith("/") else f"/{text}")
    return "/" if normalized in {"", "."} else normalized


def resolve_search_root(mount_path: str, search_path: str) -> str:
    """计算搜索的根目录路径

    根据 OpenList 的挂载路径和用户配置的搜索路径，确定实际的搜索起始目录。
    规则：
    - search_path 为 "/" 时，返回 mount_path
    - search_path 已经在 mount_path 下时，直接使用 search_path
    - 否则将 search_path 拼接到 mount_path 下

    Args:
        mount_path: OpenList 挂载路径，如 "/阿里云盘"
        search_path: 用户配置的搜索路径，如 "/影视"

    Returns:
        最终的搜索根目录路径
    """
    normalized_mount = normalize_path(mount_path)
    normalized_search = normalize_path(search_path)
    if normalized_search == "/":
        return normalized_mount
    if normalized_search == normalized_mount or normalized_search.startswith(
        normalized_mount + "/"
    ):
        return normalized_search
    return normalize_path(
        posixpath.join(normalized_mount, normalized_search.lstrip("/"))
    )


def encode_openlist_path(path: str) -> str:
    """对 OpenList 路径进行 URL 编码

    对路径中的每个段落分别进行百分号编码，保持 "/" 分隔符不变。
    用于构建 OpenList 的下载 URL。

    Args:
        path: 待编码的路径

    Returns:
        编码后的路径字符串
    """
    normalized = normalize_path(path)
    segments = normalized.split("/")
    return "/".join(quote(segment, safe="") for segment in segments)


def sanitize_filename(name: str) -> str:
    """清理文件名中的非法字符

    将 Windows/Linux 文件系统中不允许的字符替换为下划线，
    如果清理后文件名为空则生成一个随机文件名。

    Args:
        name: 原始文件名

    Returns:
        安全的文件名字符串
    """
    cleaned = re.sub(r'[<>:"/\\\\|?*\\x00-\\x1f]', "_", str(name or "").strip())
    return cleaned or f"download_{uuid4().hex}"


def as_text(value: Any) -> str:
    """将任意值转换为文本字符串

    智能提取值中的文本内容：
    - None 返回空字符串
    - 字符串直接去除首尾空白
    - 列表/元组返回第一个非空元素的文本
    - 字典依次尝试 "title"、"name"、"text" 等常见键名
    - 其他类型调用 str() 转换

    Args:
        value: 任意输入值

    Returns:
        提取出的非空文本字符串
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        for item in value:
            text = as_text(item)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        for key in ("title", "name", "text", "value", "message"):
            text = as_text(value.get(key))
            if text:
                return text
        return ""
    return str(value).strip()


def safe_name(text: str, fallback: str = "未命名") -> str:
    """生成安全的显示名称

    清理文件名中的非法字符，限制长度为 80 个字符，
    确保结果非空且不以空格或句号结尾。

    Args:
        text: 原始文本
        fallback: 当文本为空时的默认名称

    Returns:
        清理后的安全名称字符串
    """
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", as_text(text) or fallback)
    cleaned = cleaned.strip(" .")
    return cleaned[:80] or fallback


def get_gmt_date_string() -> str:
    """获取当前时间的 GMT 格式日期字符串

    用于夸克网盘 OSS 上传签名中的日期字段。

    Returns:
        GMT 格式的日期字符串，如 "Mon, 01 Jan 2024 12:00:00 GMT"
    """
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def quark_response_ok(payload: dict[str, Any]) -> bool:
    """判断夸克网盘 API 响应是否成功

    兼容夸克网盘两种响应格式：
    - {"status": 200, ...} 格式
    - {"code": 0, ...} 格式

    Args:
        payload: 夸克网盘 API 的 JSON 响应体

    Returns:
        响应是否表示操作成功
    """
    if "status" in payload:
        return safe_int(payload.get("status"), 500) == 200
    if "code" in payload:
        return safe_int(payload.get("code"), -1) == 0
    return False


def quark_response_message(payload: dict[str, Any], default: str) -> str:
    """提取夸克网盘 API 响应中的错误/状态消息

    依次尝试 "message"、"msg"、"error" 三个键名提取消息文本。

    Args:
        payload: 夸克网盘 API 的 JSON 响应体
        default: 无法提取消息时的默认返回值

    Returns:
        提取到的消息文本或默认值
    """
    for key in ("message", "msg", "error"):
        text = as_text(payload.get(key))
        if text:
            return text
    return default


def generate_activation_code(length: int) -> str:
    """随机生成指定长度的激活码

    使用加密安全的随机数生成器，从预定义字符集中选取字符。
    激活码长度被限制在 4~16 个字符之间。

    Args:
        length: 期望的激活码长度

    Returns:
        生成的激活码字符串（大写字母和数字，排除易混淆字符）
    """
    normalized_length = max(4, min(16, int(length)))
    return "".join(
        secrets.choice(ACTIVATION_CODE_ALPHABET) for _ in range(normalized_length)
    )


def total_pages(total: int) -> int:
    """根据总数计算总页数

    Args:
        total: 总记录数

    Returns:
        总页数，至少为 1
    """
    return max(1, math.ceil(max(0, total) / PAGE_SIZE))


def human_size(size: int) -> str:
    """将字节数转换为人类可读的文件大小字符串

    自动选择合适的单位（B/KB/MB/GB/TB），保留两位小数。

    Args:
        size: 文件大小（字节）

    Returns:
        格式化后的文件大小字符串，如 "1.50 GB"
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(0, size))
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def format_search_results(
    keyword: str,
    page: int,
    total: int,
    items: list[SearchItem],
    *,
    require_reply: bool,
    loaded_count: int = 0,
    has_more: bool = False,
    total_exact: bool = True,
) -> str:
    """将搜索结果格式化为用户可读的文本消息

    生成包含搜索关键词、结果列表、分页信息和操作提示的完整消息文本。

    Args:
        keyword: 搜索关键词
        page: 当前页码
        total: 结果总数
        items: 当前页的搜索结果列表
        require_reply: 是否需要用户引用消息后操作（群聊为 True，私聊为 False）
        loaded_count: 已加载的结果总数
        has_more: 是否还有更多未加载结果
        total_exact: 总数是否精确

    Returns:
        格式化后的消息文本
    """
    page_total = total if total_exact else max(total, loaded_count)
    current_page = min(max(1, page), total_pages(page_total))
    lines = [f"检索结果：{keyword}", ""]
    if not items:
        lines.append("当前页没有结果。")
    else:
        for index, item in enumerate(items, start=1):
            lines.append(f"{index}. {item.name} 【{human_size(item.size)}】")
    lines.append("")
    if total_exact:
        lines.append(f"第 {current_page}/{total_pages(page_total)} 页，共 {total} 条")
    else:
        lines.append(
            f"第 {current_page}/{total_pages(page_total)}+ 页，已加载前 {max(page_total, loaded_count)} 条，结果较多，可继续翻页"
        )
    lines.append(
        "引用本条后发送序号 / n / p / q" if require_reply else "发送序号 / n / p / q"
    )
    return "\n".join(lines)
