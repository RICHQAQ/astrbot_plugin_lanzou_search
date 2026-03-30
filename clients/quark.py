"""
夸克网盘客户端模块

封装与夸克网盘（Quark Cloud）的 API 通信逻辑，主要功能包括：
- 文件上传（支持秒传和分片上传两种模式）
- 文件分享链接创建
- 文件删除
- 目录浏览

上传流程说明：
1. 调用预上传接口获取上传凭证（task_id、OSS 地址等）
2. 计算文件 MD5 和 SHA1 哈希
3. 尝试秒传（如果文件已存在于夸克服务器则跳过实际上传）
4. 如果秒传失败，则进行分片上传到阿里云 OSS
5. 合并分片并确认上传完成
6. 创建分享链接并获取提取码

本客户端通过模拟 PC 端 Chrome 浏览器访问夸克网盘 API，
使用 Cookie 进行身份认证。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import mimetypes
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from astrbot.api import logger

from ..core.constants import (
    QUARK_BASE_URL,
    QUARK_HEADERS,
    QUARK_OSS_USER_AGENT,
    QUARK_PARAMS,
)
from ..core.models import TransferResult
from ..core.utils import (
    as_text,
    get_gmt_date_string,
    quark_response_message,
    quark_response_ok,
    safe_int,
    safe_name,
)


class QuarkPanClient:
    """夸克网盘 API 客户端

    提供夸克网盘的文件上传、分享、删除和目录浏览功能。
    每次请求创建独立的 HTTP 连接，不依赖外部共享客户端。

    Attributes:
        cookie: 夸克网盘的登录 Cookie
        save_fid: 夸克网盘中保存文件的目录 FID（默认 "0" 为根目录）
        timeout: 请求超时时间（秒）
        verify_ssl: 是否验证 SSL 证书
        share_expired_type: 分享链接过期类型
    """

    def __init__(
        self,
        cookie: str,
        save_fid: str,
        timeout: int,
        verify_ssl: bool,
        share_expired_type: int,
    ):
        """初始化夸克网盘客户端

        Args:
            cookie: 夸克网盘的完整登录 Cookie 字符串
            save_fid: 文件保存目录的 FID（夸克网盘中的目录唯一标识）
            timeout: HTTP 请求超时时间（秒）
            verify_ssl: 是否验证 HTTPS 证书
            share_expired_type: 分享链接的过期类型（1=1天, 7=7天, 等）
        """
        self.cookie = cookie.strip()
        self.save_fid = (save_fid or "").strip() or "0"
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.share_expired_type = share_expired_type
        self.headers = {**QUARK_HEADERS, "Cookie": self.cookie}
        self.callback: dict[str, Any] = {}  # OSS 上传回调配置，由预上传接口返回

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """发送 HTTP 请求并返回 JSON 响应

        自动合并夸克网盘的通用查询参数（pr/fr/uc_param_str）。
        每次请求创建独立的 HTTP 客户端实例。

        Args:
            method: HTTP 方法（GET 或 POST）
            path: 请求路径（完整 URL）
            params: 额外的查询参数
            payload: POST 请求体 JSON 数据
            timeout: 本次请求的超时时间（覆盖默认值）

        Returns:
            解析后的 JSON 响应字典

        Raises:
            httpx.HTTPStatusError: HTTP 状态码异常时
        """
        merged_params = {**QUARK_PARAMS, **(params or {})}
        async with httpx.AsyncClient(
            timeout=timeout or self.timeout, verify=self.verify_ssl
        ) as client:
            if method.upper() == "GET":
                response = await client.get(
                    path, params=merged_params, headers=self.headers
                )
            else:
                response = await client.post(
                    path, params=merged_params, json=payload or {}, headers=self.headers
                )
            response.raise_for_status()
            return response.json()

    async def _poll_task(
        self, task_id: str, error_hint: str, max_retries: int = 50
    ) -> dict[str, Any]:
        """轮询异步任务直到完成

        夸克网盘的某些操作（如创建分享）是异步的，需要通过任务 ID
        轮询直到任务状态变为完成（status=2）。

        Args:
            task_id: 异步任务 ID
            error_hint: 超时时的错误提示信息
            max_retries: 最大轮询次数，默认 50 次（约 25 秒）

        Returns:
            任务完成后的数据

        Raises:
            RuntimeError: 任务超时或空间不足时
        """
        for retry_index in range(max_retries):
            response = await self._request_json(
                "GET",
                f"{QUARK_BASE_URL}/task",
                params={"task_id": task_id, "retry_index": str(retry_index)},
            )
            if quark_response_message(response, "") == "capacity limit[{0}]":
                raise RuntimeError("夸克空间不足")
            if not quark_response_ok(response):
                await asyncio.sleep(0.5)
                continue
            task_data = response.get("data") or {}
            if safe_int(task_data.get("status"), 0) == 2:  # status=2 表示任务完成
                return task_data
            await asyncio.sleep(0.5)
        raise RuntimeError(error_hint)

    async def create_share(
        self, fid_list: list[str], title: str
    ) -> tuple[str, str, str]:
        """创建夸克网盘分享链接

        将指定文件创建为公开分享链接，流程：
        1. 调用分享接口获取任务 ID
        2. 轮询任务直到分享创建完成
        3. 获取分享链接的 URL 和提取码

        Args:
            fid_list: 要分享的文件 FID 列表
            title: 分享标题

        Returns:
            元组，包含 (share_id, share_url, passcode)

        Raises:
            RuntimeError: 创建分享失败时
        """
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/share",
            payload={
                "fid_list": fid_list,
                "expired_type": self.share_expired_type,
                "title": title,
                "url_type": 1,
            },
        )
        if not quark_response_ok(response):
            raise RuntimeError(quark_response_message(response, "创建夸克分享失败"))

        task_id = as_text((response.get("data") or {}).get("task_id"))
        if not task_id:
            raise RuntimeError("夸克分享任务缺少 task_id")

        # 轮询等待分享创建完成
        share_task = await self._poll_task(task_id, "夸克分享任务超时")
        share_id = as_text(share_task.get("share_id"))
        if not share_id:
            raise RuntimeError("夸克分享结果缺少 share_id")

        # 获取分享链接和提取码
        password_response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/share/password",
            payload={"share_id": share_id},
        )
        if not quark_response_ok(password_response):
            raise RuntimeError(
                quark_response_message(password_response, "获取夸克分享链接失败")
            )

        data = password_response.get("data") or {}
        return share_id, as_text(data.get("share_url")), as_text(data.get("passcode"))

    async def delete_fids(self, fids: list[Any]) -> str | None:
        """删除夸克网盘中的文件

        用于清理上传后的临时文件。

        Args:
            fids: 要删除的文件 FID 列表

        Returns:
            None 表示成功，字符串表示错误消息
        """
        normalized_fids = [str(fid).strip() for fid in fids if str(fid).strip()]
        if not normalized_fids:
            return None
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/file/delete",
            payload={"action_type": 2, "exclude_fids": [], "filelist": normalized_fids},
        )
        if not quark_response_ok(response):
            return quark_response_message(response, "自动删除夸克旧文件失败")
        return None

    async def list_dir(self, pdir_fid: str = "0") -> list[dict[str, Any]]:
        """列出夸克网盘目录内容

        Args:
            pdir_fid: 父目录 FID，"0" 表示根目录

        Returns:
            目录中的文件/文件夹信息列表

        Raises:
            RuntimeError: 获取目录失败时
        """
        response = await self._request_json(
            "GET",
            f"{QUARK_BASE_URL}/file/sort",
            params={
                "pdir_fid": pdir_fid or "0",
                "_page": "1",
                "_size": "200",
                "_fetch_total": "1",
                "_fetch_sub_dirs": "0",
                "_sort": "file_type:asc,updated_at:desc",
            },
        )
        if not quark_response_ok(response):
            raise RuntimeError(quark_response_message(response, "获取夸克目录失败"))
        return (response.get("data") or {}).get("list") or []

    async def _compute_file_hash(self, file_path: Path) -> tuple[str, str]:
        """计算文件的 MD5 和 SHA1 哈希值

        用于秒传检测：如果服务器已有相同哈希的文件，可以跳过实际上传。
        在独立线程中执行以避免阻塞异步事件循环。

        Args:
            file_path: 本地文件路径

        Returns:
            元组，包含 (md5_hex, sha1_hex)
        """

        def run() -> tuple[str, str]:
            md5_hash = hashlib.md5()
            sha1_hash = hashlib.sha1()
            with file_path.open("rb") as file_obj:
                while True:
                    chunk = file_obj.read(8 * 1024 * 1024)  # 8MB 缓冲区
                    if not chunk:
                        break
                    md5_hash.update(chunk)
                    sha1_hash.update(chunk)
            return md5_hash.hexdigest(), sha1_hash.hexdigest()

        return await asyncio.to_thread(run)

    async def _check_fast_upload(
        self, task_id: str, md5: str, sha1: str
    ) -> dict[str, Any] | None:
        """检查是否可以秒传（快速上传）

        如果夸克服务器上已存在相同哈希的文件，则无需实际上传数据，
        只需在服务端建立文件引用即可完成上传。

        Args:
            task_id: 上传任务 ID
            md5: 文件的 MD5 哈希
            sha1: 文件的 SHA1 哈希

        Returns:
            秒传成功时返回数据字典，不可秒传时返回 None
        """
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/file/update/hash",
            payload={"task_id": task_id, "md5": md5, "sha1": sha1},
        )
        if quark_response_ok(response) and (
            (response.get("data") or {}).get("finish") is True
        ):
            return response.get("data") or {}
        return None

    async def _get_upload_auth(
        self,
        task_id: str,
        method: str,
        content_type: str,
        upload_url: str,
        upload_id: str,
        part_number: int | None = None,
        content_md5: str | None = None,
        callback: dict[str, Any] | None = None,
    ) -> tuple[str | None, str | None]:
        """获取 OSS 上传的签名授权

        夸克网盘使用阿里云 OSS 作为底层存储，上传前需要获取签名。
        签名的计算遵循阿里云 OSS V1 签名规范。

        Args:
            task_id: 上传任务 ID
            method: HTTP 方法（PUT/POST）
            content_type: 内容类型
            upload_url: OSS 上传地址
            upload_id: 分片上传的 uploadId
            part_number: 分片序号
            content_md5: 内容 MD5（base64 编码）
            callback: OSS 回调配置

        Returns:
            元组，包含 (auth_key, oss_date)，获取失败时对应值为 None
        """
        # 构建签名所需的资源路径和查询参数
        parsed_url = urlparse(upload_url)
        resource = parsed_url.path
        query_params: dict[str, str] = {}
        if upload_id:
            query_params["uploadId"] = upload_id
        if part_number is not None:
            query_params["partNumber"] = str(part_number)
        sorted_query = "&".join(
            f"{key}={value}" for key, value in sorted(query_params.items())
        )
        if sorted_query:
            resource += f"?{sorted_query}"

        # 构建 OSS 签名字符串
        current_time_gmt = get_gmt_date_string()
        oss_headers_list = [
            f"x-oss-date:{current_time_gmt}",
            f"x-oss-user-agent:{QUARK_OSS_USER_AGENT}",
        ]
        if callback and callback.get("callbackUrl") and callback.get("callbackBody"):
            callback_json = json.dumps(
                callback, ensure_ascii=False, separators=(",", ":")
            )
            callback_value = base64.b64encode(callback_json.encode("utf-8")).decode(
                "utf-8"
            )
            oss_headers_list.append(f"x-oss-callback:{callback_value}")
        oss_headers_list.sort()
        canonicalized_oss_headers = "\n".join(oss_headers_list) + "\n"

        auth_meta = (
            f"{method.upper()}\n"
            f"{content_md5 or ''}\n"
            f"{content_type or ''}\n"
            f"{current_time_gmt}\n"
            f"{canonicalized_oss_headers}{resource}"
        )

        # 请求夸克 API 获取签名
        response = await self._request_json(
            "POST",
            f"{QUARK_BASE_URL}/file/upload/auth",
            payload={"task_id": task_id, "auth_meta": auth_meta},
        )
        if not quark_response_ok(response):
            return None, None
        auth_key = as_text((response.get("data") or {}).get("auth_key"))
        if not auth_key:
            return None, None
        return auth_key, current_time_gmt

    async def _complete_multipart_upload(
        self,
        task_id: str,
        complete_oss_url: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> bool:
        """完成分片上传（合并所有分片）

        将所有已上传的分片在 OSS 端合并为完整文件。
        发送 XML 格式的合并请求到阿里云 OSS。

        Args:
            task_id: 上传任务 ID
            complete_oss_url: OSS 合并请求的 URL
            upload_id: 分片上传的 uploadId
            parts: 分片信息列表，每项包含 part_number 和 etag

        Returns:
            合并是否成功
        """
        # 构建 OSS 合并请求的 XML 体
        xml_body = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<CompleteMultipartUpload>",
        ]
        for part in parts:
            etag = as_text(part.get("etag")).strip('"')
            xml_body.append("  <Part>")
            xml_body.append(
                f"    <PartNumber>{safe_int(part.get('part_number'), 0, minimum=0)}</PartNumber>"
            )
            xml_body.append(f'    <ETag>"{etag}"</ETag>')
            xml_body.append("  </Part>")
        xml_body.append("</CompleteMultipartUpload>")
        xml_bytes = "\n".join(xml_body).encode("utf-8")
        content_md5 = base64.b64encode(hashlib.md5(xml_bytes).digest()).decode("utf-8")

        # 获取合并请求的签名
        auth_key, oss_date = await self._get_upload_auth(
            task_id=task_id,
            method="POST",
            content_type="application/xml",
            upload_url=complete_oss_url,
            upload_id=upload_id,
            content_md5=content_md5,
            callback=self.callback,
        )
        if not auth_key or not oss_date:
            return False

        headers = {
            "Content-Type": "application/xml",
            "Content-MD5": content_md5,
            "Authorization": auth_key,
            "x-oss-date": oss_date,
            "x-oss-user-agent": QUARK_OSS_USER_AGENT,
            "Host": urlparse(complete_oss_url).hostname or "",
        }
        if (
            self.callback
            and self.callback.get("callbackUrl")
            and self.callback.get("callbackBody")
        ):
            callback_json = json.dumps(
                self.callback, ensure_ascii=False, separators=(",", ":")
            )
            headers["x-oss-callback"] = base64.b64encode(
                callback_json.encode("utf-8")
            ).decode("utf-8")

        async with httpx.AsyncClient(timeout=180, verify=self.verify_ssl) as client:
            response = await client.post(
                f"{complete_oss_url}?uploadId={quote(upload_id)}",
                headers=headers,
                content=xml_bytes,
            )
            return response.status_code == 200

    async def _finish_upload(self, task_id: str, obj_key: str) -> dict[str, Any] | None:
        """确认文件上传完成

        在分片上传或秒传后，需要调用此接口告知夸克网盘上传已完成。
        包含指数退避重试机制（最多 5 次）。

        Args:
            task_id: 上传任务 ID
            obj_key: OSS 对象键

        Returns:
            上传完成后的文件信息，失败返回 None
        """
        retry_delay = 2.0
        for _ in range(5):
            response = await self._request_json(
                "POST",
                f"{QUARK_BASE_URL}/file/upload/finish",
                payload={"task_id": task_id, "obj_key": obj_key},
            )
            data = response.get("data") or {}
            if quark_response_ok(response) and data.get("finish") is True:
                return data
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 10.0)
        return None

    async def upload_and_share(
        self, file_path: Path, share_title: str
    ) -> TransferResult:
        """上传文件到夸克网盘并创建分享链接

        完整流程：
        1. 检查前置条件（Cookie、文件存在性）
        2. 计算文件哈希（MD5 + SHA1）
        3. 调用预上传接口获取上传凭证
        4. 尝试秒传，如果不行则进行分片上传
        5. 确认上传完成
        6. 创建分享链接

        Args:
            file_path: 待上传的本地文件路径
            share_title: 分享标题

        Returns:
            TransferResult 包含分享链接、提取码等信息
        """
        # 检查前置条件
        if not self.cookie:
            return TransferResult(success=False, message="未配置夸克 Cookie")
        if not file_path.exists():
            return TransferResult(
                success=False, message=f"待上传文件不存在：{file_path}"
            )

        # 收集文件元信息
        file_size = file_path.stat().st_size
        file_name = safe_name(file_path.name, "activation.zip")
        content_type, _ = mimetypes.guess_type(file_name)
        content_type = content_type or "application/octet-stream"
        file_mtime = int(file_path.stat().st_mtime * 1000)
        file_ctime = int(file_path.stat().st_ctime * 1000)

        # 计算文件哈希
        try:
            md5_hash, sha1_hash = await self._compute_file_hash(file_path)
        except Exception as exc:
            return TransferResult(success=False, message=f"计算激活包哈希失败：{exc}")

        # 预上传：获取上传凭证
        try:
            pre_upload_response = await self._request_json(
                "POST",
                f"{QUARK_BASE_URL}/file/upload/pre",
                payload={
                    "ccp_hash_update": True,
                    "parallel_upload": True,
                    "pdir_fid": self.save_fid,
                    "dir_name": "",
                    "file_name": file_name,
                    "format_type": content_type,
                    "l_created_at": file_ctime,
                    "l_updated_at": file_mtime,
                    "size": file_size,
                },
            )
        except Exception as exc:
            return TransferResult(success=False, message=f"夸克预上传失败：{exc}")

        if not quark_response_ok(pre_upload_response):
            return TransferResult(
                success=False,
                message=quark_response_message(pre_upload_response, "夸克预上传失败"),
            )

        # 解析预上传返回的上传参数
        pre_data = pre_upload_response.get("data") or {}
        metadata = pre_upload_response.get("metadata") or {}
        task_id = as_text(pre_data.get("task_id"))
        upload_id = as_text(pre_data.get("upload_id"))
        obj_key = as_text(pre_data.get("obj_key"))
        fid = as_text(pre_data.get("fid"))
        bucket = as_text(pre_data.get("bucket"))
        region = as_text(pre_data.get("region")) or "oss-cn-zhangjiakou"
        self.callback = pre_data.get("callback") or {}
        part_size = safe_int(
            metadata.get("part_size"), 4 * 1024 * 1024, minimum=1024 * 1024
        )

        if not task_id or not upload_id or not obj_key or not bucket:
            return TransferResult(success=False, message="夸克预上传返回缺少必要字段")

        complete_oss_url = f"https://{bucket}.{region}.aliyuncs.com/{obj_key}"

        try:
            # 尝试秒传
            fast_upload_data = await self._check_fast_upload(
                task_id, md5_hash, sha1_hash
            )
            if fast_upload_data:
                # 秒传成功，确认上传完成
                finish_result = await self._finish_upload(task_id, obj_key)
                if not finish_result:
                    return TransferResult(success=False, message="夸克秒传确认失败")
                final_fid = as_text(finish_result.get("fid")) or fid
            else:
                # 秒传失败，执行分片上传
                total_parts = max(1, math.ceil(file_size / part_size))
                parts: list[dict[str, Any]] = []
                async with httpx.AsyncClient(
                    timeout=180, verify=self.verify_ssl
                ) as client:
                    with file_path.open("rb") as file_obj:
                        for part_number in range(1, total_parts + 1):
                            part_data = file_obj.read(part_size)
                            if not part_data:
                                break
                            # 计算分片 MD5（base64 编码，用于完整性校验）
                            part_md5_b64 = base64.b64encode(
                                hashlib.md5(part_data).digest()
                            ).decode("utf-8")
                            # 获取分片上传签名
                            auth_key, oss_date = await self._get_upload_auth(
                                task_id=task_id,
                                method="PUT",
                                content_type=content_type,
                                upload_url=complete_oss_url,
                                upload_id=upload_id,
                                part_number=part_number,
                                content_md5=part_md5_b64,
                            )
                            if not auth_key or not oss_date:
                                return TransferResult(
                                    success=False,
                                    message=f"夸克分片 {part_number} 授权失败",
                                )

                            headers = {
                                "Content-Type": content_type,
                                "Content-Length": str(len(part_data)),
                                "Content-MD5": part_md5_b64,
                                "Authorization": auth_key,
                                "x-oss-date": oss_date,
                                "x-oss-user-agent": QUARK_OSS_USER_AGENT,
                                "Host": f"{bucket}.{region}.aliyuncs.com",
                            }

                            # 上传分片到阿里云 OSS
                            response = await client.put(
                                f"{complete_oss_url}?partNumber={part_number}&uploadId={quote(upload_id)}",
                                headers=headers,
                                content=part_data,
                            )
                            response.raise_for_status()
                            etag = as_text(response.headers.get("ETag")).strip('"')
                            if not etag:
                                return TransferResult(
                                    success=False,
                                    message=f"夸克分片 {part_number} 缺少 ETag",
                                )
                            parts.append({"part_number": part_number, "etag": etag})

                # 合并所有分片
                completed = await self._complete_multipart_upload(
                    task_id, complete_oss_url, upload_id, parts
                )
                if not completed:
                    return TransferResult(success=False, message="夸克分片合并失败")

                # 确认上传完成
                finish_result = await self._finish_upload(task_id, obj_key)
                if not finish_result:
                    return TransferResult(success=False, message="夸克上传完成确认失败")
                final_fid = as_text(finish_result.get("fid")) or fid

            if not final_fid:
                return TransferResult(
                    success=False, message="夸克未返回上传后的文件 ID"
                )

            # 创建分享链接
            _, share_url, password = await self.create_share([final_fid], share_title)
            return TransferResult(
                success=True,
                title=share_title,
                share_url=share_url,
                password=password,
                cleanup_job={"provider": "quark", "payload": {"fids": [final_fid]}},
            )
        except RuntimeError as exc:
            return TransferResult(success=False, message=str(exc))
        except Exception as exc:
            logger.error(f"夸克上传失败：{exc}", exc_info=True)
            return TransferResult(success=False, message=f"夸克上传失败：{exc}")
