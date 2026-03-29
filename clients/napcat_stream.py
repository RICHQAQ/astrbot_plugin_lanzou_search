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
    def __init__(
        self,
        *,
        action_sender: Any,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        self.action_sender = action_sender
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _send_action(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        if not callable(self.action_sender):
            raise RuntimeError("NapCat Stream action sender unavailable")
        response = await self.action_sender(action, **params)
        return response if isinstance(response, dict) else {}

    async def upload_file(self, file_path: Path) -> str:
        if not file_path.exists():
            raise RuntimeError(f"stream source file not found: {file_path}")

        chunks, sha256_hash, total_size = _calculate_file_chunks(
            file_path, self.chunk_size_bytes
        )
        if not chunks:
            raise RuntimeError("stream source file is empty")

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
    def __init__(
        self,
        *,
        ws_url: str,
        access_token: str = "",
        timeout: float = 180.0,
        chunk_size_bytes: int = 64 * 1024,
        file_retention_seconds: int = 30,
    ) -> None:
        self.ws_url = str(ws_url or "").strip()
        self.access_token = str(access_token or "").strip()
        self.timeout = max(10.0, float(timeout))
        self.chunk_size_bytes = max(8 * 1024, int(chunk_size_bytes))
        self.file_retention_ms = max(5, int(file_retention_seconds)) * 1000

    async def _connect(self):
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
            max_size=None,
        )

    async def _send_action(
        self, websocket: Any, action: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        payload = {
            "action": action,
            "params": params,
            "echo": uuid4().hex,
        }
        await websocket.send(json.dumps(payload, ensure_ascii=False))
        while True:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=self.timeout)
            data = json.loads(raw_message)
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


# Keep the legacy public name available after the split.
NapCatStreamClient = NapCatWsStreamClient
