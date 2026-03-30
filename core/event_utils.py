"""
事件工具模块

提供对 AstrBot 消息事件的安全访问工具函数，用于从事件对象中
提取发送者信息、群组信息、消息内容等，所有函数均做了异常保护，
避免因事件对象结构不一致而导致的运行时错误。
"""

from __future__ import annotations

from typing import Any

import astrbot.api.message_components as Comp

from .utils import normalize_session_body


def event_sender_id(event: Any) -> str:
    """从事件中提取消息发送者 ID

    Args:
        event: AstrBot 消息事件对象

    Returns:
        发送者 ID 字符串，提取失败时返回空字符串
    """
    try:
        return str(event.get_sender_id() or "").strip()
    except Exception:
        return ""


def event_group_id(event: Any) -> str:
    """从事件中提取群组 ID

    如果事件来自群聊则返回群号，私聊返回空字符串。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        群组 ID 字符串，私聊时返回空字符串
    """
    try:
        return str(event.get_group_id() or "").strip()
    except Exception:
        return ""


def event_sender_scope(event: Any) -> str:
    """生成发送者的作用域标识

    用于区分不同场景下的用户标识：
    - 群聊：group:{群号}:user:{用户ID}
    - 私聊：private:{用户ID}

    Args:
        event: AstrBot 消息事件对象

    Returns:
        作用域标识字符串
    """
    sender_id = event_sender_id(event)
    group_id = event_group_id(event)
    if group_id:
        return f"group:{group_id}:user:{sender_id or 'unknown'}"
    return f"private:{sender_id or 'unknown'}"


def event_self_id(event: Any) -> str:
    """从事件中提取机器人自身的 ID

    用于判断引用消息是否来自机器人自己（群聊中匹配搜索结果）。
    依次尝试从 message_obj 和 event 对象上获取 self_id 属性。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        机器人自身 ID 字符串
    """
    for obj in (getattr(event, "message_obj", None), event):
        if obj is None:
            continue
        try:
            value = str(getattr(obj, "self_id", "") or "").strip()
        except Exception:
            value = ""
        if value:
            return value
    return ""


def event_message_token(event: Any) -> str:
    """从事件中提取消息唯一标识

    用于选择操作的防抖去重，依次尝试以下方式获取：
    1. event.get_message_id() 方法
    2. event.message_id / event.msg_id / event.id 属性
    3. event.message_obj.message_id 等嵌套属性
    4. event.raw_message.message_id 等更深嵌套属性

    Args:
        event: AstrBot 消息事件对象

    Returns:
        消息唯一标识字符串
    """
    getter = getattr(event, "get_message_id", None)
    if callable(getter):
        try:
            token = str(getter() or "").strip()
            if token:
                return token
        except Exception:
            pass

    for attr_name in ("message_id", "msg_id", "id"):
        try:
            token = str(getattr(event, attr_name, "") or "").strip()
        except Exception:
            token = ""
        if token:
            return token

    for obj_name in ("message_obj", "raw_message", "message"):
        try:
            obj = getattr(event, obj_name, None)
        except Exception:
            obj = None
        if obj is None:
            continue
        for attr_name in ("message_id", "msg_id", "id"):
            try:
                token = str(getattr(obj, attr_name, "") or "").strip()
            except Exception:
                token = ""
            if token:
                return token
    return ""


def is_group_chat(event: Any) -> bool:
    """判断事件是否来自群聊

    通过是否存在群组 ID 来判断消息来源。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        True 表示群聊，False 表示私聊
    """
    return bool(event_group_id(event))


def event_session_key(event: Any) -> str:
    """获取事件对应的会话键

    当前实现与 event_sender_scope 相同，用于标识一个独立的搜索会话。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        会话键字符串
    """
    return event_sender_scope(event)


def event_message_components(event: Any) -> list[Any]:
    """从事件中提取消息组件列表

    消息可能由多种组件组成（文本、@、图片、引用等），
    此函数安全地提取这些组件列表。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        消息组件列表，提取失败时返回空列表
    """
    try:
        message_obj = getattr(event, "message_obj", None)
    except Exception:
        message_obj = None
    if message_obj is None:
        return []
    try:
        components = getattr(message_obj, "message", None)
    except Exception:
        components = None
    return components if isinstance(components, list) else []


def event_reply_component(event: Any) -> Any | None:
    """从事件中提取引用（回复）消息组件

    遍历消息的所有组件，返回第一个 Reply 类型的组件。
    用于群聊中通过引用搜索结果消息来选择文件。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        Reply 组件对象，或 None（如果没有引用消息）
    """
    for component in event_message_components(event):
        if isinstance(component, Comp.Reply):
            return component
    return None


def event_unified_msg_origin(event: Any) -> str:
    """从事件中提取统一消息来源标识

    unified_msg_origin 是 AstrBot 用来标识消息来源的唯一地址，
    可用于向特定用户/群组发送主动消息。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        统一消息来源标识字符串
    """
    try:
        return str(getattr(event, "unified_msg_origin", "") or "").strip()
    except Exception:
        return ""


def event_role(event: Any) -> str:
    """从事件中提取发送者的角色

    返回小写的角色标识，如 "admin" 表示管理员。

    Args:
        event: AstrBot 消息事件对象

    Returns:
        角色字符串（小写）
    """
    try:
        return str(getattr(event, "role", "") or "").strip().lower()
    except Exception:
        return ""


def is_admin_event(event: Any) -> bool:
    """判断事件发送者是否为管理员

    Args:
        event: AstrBot 消息事件对象

    Returns:
        True 表示管理员
    """
    return event_role(event) == "admin"


def reply_body_text(reply_component: Any) -> str:
    """从引用消息组件中提取纯文本内容

    遍历引用消息的消息链（Chain），提取所有文本内容（跳过 @ 组件），
    并进行规范化处理。如果 Chain 中没有文本，则回退到 message_str 属性。

    Args:
        reply_component: 引用消息组件对象（Reply 类型）

    Returns:
        引用消息的纯文本内容（已规范化）
    """
    Chain = getattr(reply_component, "Chain", None) or []
    parts: list[str] = []
    if isinstance(Chain, list):
        for component in Chain:
            if isinstance(component, Comp.At):
                continue
            if isinstance(component, Comp.Plain):
                parts.append(str(getattr(component, "text", "") or ""))
                continue
            text = str(getattr(component, "text", "") or "").strip()
            if text:
                parts.append(text)
    body = normalize_session_body("".join(parts))
    if body:
        return body
    return normalize_session_body(
        str(getattr(reply_component, "message_str", "") or "")
    )
