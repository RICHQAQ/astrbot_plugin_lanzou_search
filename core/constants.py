from __future__ import annotations

OPENLIST_SEARCH_API = "/api/fs/search"
OPENLIST_GET_API = "/api/fs/get"
OPENLIST_LIST_API = "/api/fs/list"
OPENLIST_LINK_API = "/api/fs/link"
OPENLIST_MKDIR_API = "/api/fs/mkdir"
OPENLIST_REMOVE_API = "/api/fs/remove"
OPENLIST_ADD_OFFLINE_DOWNLOAD_API = "/api/fs/add_offline_download"
OPENLIST_LOGIN_HASH_API = "/api/auth/login/hash"
OPENLIST_STATIC_HASH_SALT = "https://github.com/alist-org/alist"

COMMAND_NAME = "sh"
PAGE_SIZE = 10
MAX_SOURCES = 5
DEFAULT_SELECTION_DEDUP_WINDOW_SECONDS = 5
DOWNLOAD_DIR_RETENTION_SECONDS = 3600
DOWNLOAD_SEND_GRACE_SECONDS = 180
FILE_PREPARE_RETRY_COUNT = 3
FILE_PREPARE_RETRY_DELAY_SECONDS = 1.5
FILE_OPERATION_TIMEOUT_SECONDS = 180
FILE_SEND_TOTAL_TIMEOUT_SECONDS = 90
FILE_LINK_CACHE_TTL_SECONDS = 90
FILE_MISSING_CACHE_TTL_SECONDS = 15
OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS = 2.0

ACTIVATION_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ACTIVATION_COMMAND_ALIASES = ("激活", "jh")
ACTIVATION_STATE_FILE = "activation_state.json"
ACTIVATION_PENDING_STATE_FILE = "activation_pending_state.json"
ACTIVATION_USERS_FILE = "activation_users.json"
ACTIVATION_REQUESTS_FILE = "activation_requests.json"
ACTIVATION_ADMIN_ORIGINS_FILE = "activation_admin_origins.json"

QUARK_BASE_URL = "https://drive-pc.quark.cn/1/clouddrive"
QUARK_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json;charset=UTF-8",
    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "Referer": "https://pan.quark.cn/",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ),
}
QUARK_PARAMS = {"pr": "ucpro", "fr": "pc", "uc_param_str": ""}
QUARK_OSS_USER_AGENT = "aliyun-sdk-js/6.18.0 Chrome/134.0.0.0 on Windows 10 64-bit"
