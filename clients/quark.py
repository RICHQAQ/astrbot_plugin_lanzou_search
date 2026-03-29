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
    def __init__(
        self,
        cookie: str,
        save_fid: str,
        timeout: int,
        verify_ssl: bool,
        share_expired_type: int,
    ):
        self.cookie = cookie.strip()
        self.save_fid = (save_fid or "").strip() or "0"
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.share_expired_type = share_expired_type
        self.headers = {**QUARK_HEADERS, "Cookie": self.cookie}
        self.callback: dict[str, Any] = {}

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
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
            if safe_int(task_data.get("status"), 0) == 2:
                return task_data
            await asyncio.sleep(0.5)
        raise RuntimeError(error_hint)

    async def create_share(
        self, fid_list: list[str], title: str
    ) -> tuple[str, str, str]:
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

        share_task = await self._poll_task(task_id, "夸克分享任务超时")
        share_id = as_text(share_task.get("share_id"))
        if not share_id:
            raise RuntimeError("夸克分享结果缺少 share_id")

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
        def run() -> tuple[str, str]:
            md5_hash = hashlib.md5()
            sha1_hash = hashlib.sha1()
            with file_path.open("rb") as file_obj:
                while True:
                    chunk = file_obj.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    md5_hash.update(chunk)
                    sha1_hash.update(chunk)
            return md5_hash.hexdigest(), sha1_hash.hexdigest()

        return await asyncio.to_thread(run)

    async def _check_fast_upload(
        self, task_id: str, md5: str, sha1: str
    ) -> dict[str, Any] | None:
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
        if not self.cookie:
            return TransferResult(success=False, message="未配置夸克 Cookie")
        if not file_path.exists():
            return TransferResult(
                success=False, message=f"待上传文件不存在：{file_path}"
            )

        file_size = file_path.stat().st_size
        file_name = safe_name(file_path.name, "activation.zip")
        content_type, _ = mimetypes.guess_type(file_name)
        content_type = content_type or "application/octet-stream"
        file_mtime = int(file_path.stat().st_mtime * 1000)
        file_ctime = int(file_path.stat().st_ctime * 1000)

        try:
            md5_hash, sha1_hash = await self._compute_file_hash(file_path)
        except Exception as exc:
            return TransferResult(success=False, message=f"计算激活包哈希失败：{exc}")

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
            fast_upload_data = await self._check_fast_upload(
                task_id, md5_hash, sha1_hash
            )
            if fast_upload_data:
                finish_result = await self._finish_upload(task_id, obj_key)
                if not finish_result:
                    return TransferResult(success=False, message="夸克秒传确认失败")
                final_fid = as_text(finish_result.get("fid")) or fid
            else:
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
                            part_md5_b64 = base64.b64encode(
                                hashlib.md5(part_data).digest()
                            ).decode("utf-8")
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

                completed = await self._complete_multipart_upload(
                    task_id, complete_oss_url, upload_id, parts
                )
                if not completed:
                    return TransferResult(success=False, message="夸克分片合并失败")

                finish_result = await self._finish_upload(task_id, obj_key)
                if not finish_result:
                    return TransferResult(success=False, message="夸克上传完成确认失败")
                final_fid = as_text(finish_result.get("fid")) or fid

            if not final_fid:
                return TransferResult(
                    success=False, message="夸克未返回上传后的文件 ID"
                )

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
