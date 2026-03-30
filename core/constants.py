"""
常量定义模块

集中管理插件中使用的所有常量，包括 OpenList API 路径、
默认配置值、夸克网盘 API 参数、激活码相关配置等。
"""

from __future__ import annotations

# ==================== OpenList API 路径 ====================
# OpenList（基于 alist）的 RESTful API 端点路径

OPENLIST_SEARCH_API = "/api/fs/search"  # 搜索文件
OPENLIST_GET_API = "/api/fs/get"  # 获取文件信息（含 raw_url 直链）
OPENLIST_LIST_API = "/api/fs/list"  # 列出目录内容
OPENLIST_LINK_API = "/api/fs/link"  # 获取文件下载/访问链接
OPENLIST_MKDIR_API = "/api/fs/mkdir"  # 创建目录
OPENLIST_REMOVE_API = "/api/fs/remove"  # 删除文件/目录
OPENLIST_ADD_OFFLINE_DOWNLOAD_API = "/api/fs/add_offline_download"  # 添加离线下载任务
OPENLIST_LOGIN_HASH_API = "/api/auth/login/hash"  # 使用哈希密码登录

# OpenList 密码哈希的静态盐值，与 alist 源码中的默认盐值一致
OPENLIST_STATIC_HASH_SALT = "https://github.com/alist-org/alist"

# ==================== 搜索相关常量 ====================

COMMAND_NAME = "sh"  # 搜索命令的触发关键词
PAGE_SIZE = 10  # 每页显示的搜索结果数量
MAX_SOURCES = 5  # 最大支持的搜索来源数量

# 选择操作的防抖时间窗口（秒），同一用户在此时间内重复发送相同指令会被忽略
DEFAULT_SELECTION_DEDUP_WINDOW_SECONDS = 5

# ==================== 文件传输相关常量 ====================

# 下载临时目录的保留时间（秒），超过此时间的临时下载目录会被自动清理
DOWNLOAD_DIR_RETENTION_SECONDS = 3600

# 下载完成后延迟发送的宽限期（秒），防止文件刚下载就被清理
DOWNLOAD_SEND_GRACE_SECONDS = 180

# 文件准备操作（如获取下载链接）的重试次数
FILE_PREPARE_RETRY_COUNT = 3

# 文件准备操作重试之间的延迟（秒），每次重试按递增倍数延迟
FILE_PREPARE_RETRY_DELAY_SECONDS = 1.5

# 单次文件操作的超时时间（秒），如获取文件链接、下载等
FILE_OPERATION_TIMEOUT_SECONDS = 180

# 发送文件的总超时时间（秒），包含下载和发送的整个过程
FILE_SEND_TOTAL_TIMEOUT_SECONDS = 90

# 文件下载链接缓存的存活时间（秒），缓存期间复用已获取的链接
FILE_LINK_CACHE_TTL_SECONDS = 90

# 文件标记为"已失效"后的缓存存活时间（秒），短期内不重复查询失效文件
FILE_MISSING_CACHE_TTL_SECONDS = 15

# 离线下载回退方案的轮询间隔（秒），用于检查离线下载任务状态
OFFLINE_FALLBACK_POLL_INTERVAL_SECONDS = 2.0

# ==================== 激活码相关常量 ====================

# 激活码字符集，排除了容易混淆的字符（如 I/O/0/1）
ACTIVATION_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

# 激活命令的触发关键词别名，用户发送"激活"或"jh"均可触发
ACTIVATION_COMMAND_ALIASES = ("激活", "jh")

# 激活状态持久化文件名，存储当前有效的激活码和分享信息
ACTIVATION_STATE_FILE = "activation_state.json"

# 待生效的激活状态文件，用于预生成即将替换的下一批激活码
ACTIVATION_PENDING_STATE_FILE = "activation_pending_state.json"

# 已激活用户记录文件，记录每个用户对应的激活批次
ACTIVATION_USERS_FILE = "activation_users.json"

# 激活请求记录文件，记录在群聊中发起但尚未在私聊中完成的激活请求
ACTIVATION_REQUESTS_FILE = "activation_requests.json"

# 管理员通知来源记录文件，存储管理员的私信地址以便推送激活码更新通知
ACTIVATION_ADMIN_ORIGINS_FILE = "activation_admin_origins.json"

# ==================== 夸克网盘 API 常量 ====================

# 夸克网盘 API 基础 URL
QUARK_BASE_URL = "https://drive-pc.quark.cn/1/clouddrive"

# 夸克网盘 API 请求头，模拟 PC 端 Chrome 浏览器访问
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

# 夸克网盘 API 的通用查询参数
QUARK_PARAMS = {"pr": "ucpro", "fr": "pc", "uc_param_str": ""}

# 夸克 OSS 上传时使用的 User-Agent 标识
QUARK_OSS_USER_AGENT = "aliyun-sdk-js/6.18.0 Chrome/134.0.0.0 on Windows 10 64-bit"
