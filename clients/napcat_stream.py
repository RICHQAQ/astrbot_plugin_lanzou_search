"""
NapCat 流式文件上传客户端

提供通过 NapCat（QQ Bot 框架）的文件流式上传接口发送文件的能力。
支持两种连接方式：
1. NapCatActionStreamClient - 通过已有的 action_sender 回调函数发送（OneBot Action 协议）
2. NapCatWsStreamClient - 通过 WebSocket 连接直接与 NapCat 通信

流式上传的流程为：
1. 将本地文件分块并计算 SHA256 校验和
2. 逐块上传（base64 编码）
3. 发送完成信号，获取 NapCat 临时文件路径
4. 使用该路径通过群文件接口发送文件
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


def _calculate_file_chunks(
    file_path: Path, chunk_size_bytes: int
) -> tuple[list[str], str, int]:
    """将文件分块并计算校验信息

    读取指定文件，按给定大小分块，同时计算整个文件的 SHA256 哈希值。
    每个分块会被编码为 base64 字符串以便通过网络传输。

    Args:
        file_path: 待上传文件的路径
        chunk_size_bytes: 每个分块的大小（字节）

    Returns:
        元组，包含：
        - chunks: base64 编码的分块列表
        - sha256 哈希的十六进制字符串
        - 文件总大小（字节）
    """
    chunks: list[str] = []
    hasher = hashlib.sha256()
    total_size = 0
    with file_path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(chunk_size_bytes)
            if not chunk:
                break
            chunks.append(base64.b64encode(chunk).decode("utf-8"))
            hasher.update(chunk)
            total_size += len(chunk)
    return chunks, hasher.hexdigest(), total_size


class NapCatActionStreamClient:
    """基于 Action 回调的 NapCat 流式上传客户端

    通过已有的 action_sender 回调函数（通常来自 Bot 的 call_action 方法）
    调用 NapCat 的 upload_file_stream 接口，将文件分块上传到 NapCat 服务器。

    适用于已经建立了与 NapCat 的连接（如通过 HTTP API）的场景。
    """

    def __init__(
        self,
        *,
        action_sender: Any,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        """初始化客户端

        Args:
            action_sender: 可调用的 action 发送函数，签名为 async (action, **params) -> dict
            chunk_size_bytes: 分块大小（字节），最小 8KB，默认 64KB
            file_retention_seconds: 上传文件在 NapCat 端的保留时间（秒），最小 5 秒
        """
        self.action_sender = action_sender
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _send_action(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        """通过 action_sender 发送一个 NapCat Action 请求

        Args:
            action: Action 名称
            params: Action 参数

        Returns:
            Action 响应数据（字典格式）

        Raises:
            RuntimeError: 当 action_sender 不可用时
        """
        if not callable(self.action_sender):
            raise RuntimeError("NapCat Stream action sender unavailable")
        response = await self.action_sender(action, **params)
        return response if isinstance(response, dict) else {}

    async def upload_file(self, file_path: Path) -> str:
        """通过流式上传将本地文件上传到 NapCat

        完整流程：
        1. 分块并计算 SHA256
        2. 逐块调用 upload_file_stream 上传
        3. 发送 is_complete=True 完成上传
        4. 获取 NapCat 返回的临时文件路径

        Args:
            file_path: 待上传的本地文件路径

        Returns:
            NapCat 服务器上的临时文件路径，用于后续通过群文件接口发送

        Raises:
            RuntimeError: 文件不存在、文件为空或上传失败时
        """
        if not file_path.exists():
            raise RuntimeError(f"stream source file not found: {file_path}")

        chunks, sha256_hash, total_size = _calculate_file_chunks(
            file_path, self.chunk_size_bytes
        )
        if not chunks:
            raise RuntimeError("stream source file is empty")

        # 生成唯一的流式上传会话 ID
        stream_id = uuid4().hex
        total_chunks = len(chunks)
        for chunk_index, chunk_base64 in enumerate(chunks):
            await self._send_action(
                "upload_file_stream",
                {
                    "stream_id": stream_id,
                    "chunk_data": chunk_base64,
                    "chunk_index": chunk_index,
                    "total_chunks": total_chunks,
                    "file_size": total_size,
                    "expected_sha256": sha256_hash,
                    "filename": file_path.name,
                    "file_retention": self.file_retention_ms,
                },
            )

        # 发送完成信号
        result = await self._send_action(
            "upload_file_stream",
            {"stream_id": stream_id, "is_complete": True},
        )
        if str(result.get("status", "") or "") != "file_complete":
            raise RuntimeError(f"unexpected stream completion payload: {result}")
        uploaded_path = str(result.get("file_path", "") or "").strip()
        if not uploaded_path:
            raise RuntimeError(f"stream upload did not return file path: {result}")
        return uploaded_path


class NapCatWsStreamClient:
    """基于 WebSocket 连接的 NapCat 流式上传客户端

    通过 WebSocket 协议直接连接 NapCat 的 OneBot WebSocket 接口，
    使用标准的 OneBot Action 协议进行文件流式上传。

    适用于无法通过 HTTP API 访问 NapCat，但可以直接连接其 WebSocket 的场景。
    """

    def __init__(
        self,
        *,
        ws_url: str,
        access_token: str = "",
        timeout: float = 180.0,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        """初始化 WebSocket 客户端

        Args:
            ws_url: NapCat WebSocket 连接地址，如 "ws://127.0.0.1:3001"
            access_token: NapCat 访问令牌（用于 Authorization 头）
            timeout: WebSocket 操作超时时间（秒），最小 10 秒
            chunk_size_bytes: 分块大小（字节），最小 8KB，默认 64KB
            file_retention_seconds: 上传文件保留时间（秒），最小 5 秒
        """
        self.ws_url = str(ws_url or "").strip()
        self.access_token = str(access_token or "").strip()
        self.timeout = max(10.0, float(timeout))
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _connect(self):
        """建立 WebSocket 连接

        使用 websockets 库连接 NapCat 的 WebSocket 接口，
        如果配置了 access_token 则在连接时附加 Authorization 头。

        Returns:
            WebSocket 连接对象

        Raises:
            RuntimeError: 当 websockets 依赖未安装时
        """
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("未安装 websockets 依赖，请先重载插件依赖") from exc

        headers: dict[str, str] = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return await websockets.connect(
            self.ws_url,
            additional_headers=headers or None,
            open_timeout=self.timeout,
            close_timeout=min(10.0, self.timeout),
            max_size=None,  # 不限制消息大小，以支持大文件分块
        )

    async def _send_action(
        self, websocket: Any, action: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """通过 WebSocket 发送 OneBot Action 请求并等待响应

        发送 Action 请求时附带唯一的 echo 标识，然后在接收循环中
        匹配对应的响应消息。

        Args:
            websocket: 已建立的 WebSocket 连接
            action: Action 名称
            params: Action 参数

        Returns:
            Action 响应中的 data 字段

        Raises:
            RuntimeError: 当响应状态非 "ok" 时
        """
        payload = {
            "action": action,
            "params": params,
            "echo": uuid4().hex,  # 唯一请求标识，用于匹配响应
        }
        await websocket.send(json.dumps(payload, ensure_ascii=False))
        while True:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=self.timeout)
            data = json.loads(raw_message)
            # 跳过不匹配的响应（可能是其他请求的响应或事件推送）
            if str(data.get("echo", "") or "") != payload["echo"]:
                continue
            if str(data.get("status", "") or "").lower() != "ok":
                raise RuntimeError(
                    str(
                        data.get("wording")
                        or data.get("message")
                        or "NapCat Stream 调用失败"
                    )
                )
            result = data.get("data") or {}
            return result if isinstance(result, dict) else {}

    async def upload_file(self, file_path: Path) -> str:
        """通过 WebSocket 流式上传将本地文件上传到 NapCat

        流程与 NapCatActionStreamClient.upload_file 相同，
        区别在于通过 WebSocket 连接发送数据。

        Args:
            file_path: 待上传的本地文件路径

        Returns:
            NapCat 服务器上的临时文件路径

        Raises:
            RuntimeError: 配置缺失、文件不存在、文件为空或上传失败时
        """
        if not self.ws_url:
            raise RuntimeError("未配置 napcat.ws_url")
        if not file_path.exists():
            raise RuntimeError(f"stream source file not found: {file_path}")

        chunks, sha256_hash, total_size = _calculate_file_chunks(
            file_path, self.chunk_size_bytes
        )
        if not chunks:
            raise RuntimeError("stream source file is empty")

        stream_id = uuid4().hex
        websocket = await self._connect()
        try:
            total_chunks = len(chunks)
            for chunk_index, chunk_base64 in enumerate(chunks):
                await self._send_action(
                    websocket,
                    "upload_file_stream",
                    {
                        "stream_id": stream_id,
                        "chunk_data": chunk_base64,
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "file_size": total_size,
                        "expected_sha256": sha256_hash,
                        "filename": file_path.name,
                        "file_retention": self.file_retention_ms,
                    },
                )

            # 发送完成信号
            result = await self._send_action(
                websocket,
                "upload_file_stream",
                {"stream_id": stream_id, "is_complete": True},
            )
            if str(result.get("status", "") or "") != "file_complete":
                raise RuntimeError(f"unexpected stream completion payload: {result}")
            uploaded_path = str(result.get("file_path", "") or "").strip()
            if not uploaded_path:
                raise RuntimeError(f"stream upload did not return file path: {result}")
            return uploaded_path
        finally:
            await websocket.close()


# 保持向后兼容的别名，NapCatStreamClient 指向 WebSocket 实现
NapCatStreamClient = NapCatWsStreamClient
