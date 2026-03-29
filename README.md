# astrbot_plugin_lanzou_search

基于 `OpenList` 的 AstrBot 检索插件。

当前版本保留了原来的蓝奏优享检索、翻页、序号调取、排队发文件逻辑，同时新增了“群聊激活”能力：
- 群成员未激活时，机器人会在群里 `@` 对方并发送激活包分享链接
- 激活包会先在本地生成 `zip`，再通过 OpenList 上传到你挂载的夸克目录，然后生成夸克原生分享链接
- 每次轮换新的激活码后，旧激活自动失效，需要重新激活

## 主要命令

- `sh 关键词`
  搜索资源
- `激活 激活码`
  提交当前批次的激活码
- `n / p / q / 序号`
  搜索结果会话内翻页、退出、调取文件

## 群聊行为

- 群里搜索结果仍然必须引用机器人发出的结果消息后，再发送 `序号 / n / p / q`
- 群里未激活用户触发本插件时，会被机器人 `@` 提醒并收到激活包分享链接
- 普通成员需要先在群里触发一次提示，再私聊机器人发送 `激活 激活码`
- 激活成功后，当前批次内可以继续在群里使用插件
- 管理员手动更新或自动轮换完成后，新的激活码和链接会主动发给管理员
- 如果配置了目标群 UID，每次生成新的激活链接后，机器人会自动往这些群各发一次新链接

## 激活功能说明

激活功能只影响“群里使用本插件”这一层，不影响原本的 OpenList 检索和发文件逻辑。

实现方式是：
1. 插件按配置的轮换周期生成一个新的激活码
2. 把激活码写进 `zip`
3. 通过 OpenList 上传到你挂载的夸克目录
4. 调用夸克原生分享接口生成新的分享链接
5. 群成员未激活时，机器人在群里发这个分享链接
6. 本地临时生成的 `zip` 会在上传完成后自动删除，不会长期占用服务器空间

注意：
- 这里发的是夸克原生分享链接，不是 OpenList 分享链接
- 激活码是“当前批次唯一的一组”，不是按人单独生成
- 自动轮换时，插件会在临近到期前提前后台预生成下一批，生成成功后再切换，尽量避免中间断档
- 普通成员不能直接私聊机器人完成激活，必须先在群里触发过一次未激活提示

## 关键配置

### 1. OpenList

```json
{
  "openlist": {
    "base_url": "http://host.docker.internal:5244",
    "download_base_url": "http://100.64.0.24:5244",
    "token": "",
    "username": "",
    "password": ""
  }
}
```

- `base_url`
  插件访问 OpenList API 用的地址
- `download_base_url`
  插件给用户生成下载链接时使用的地址

### 2. 检索目录

```json
{
  "sources": [
    {
      "enabled": true,
      "label": "主资源库",
      "mount_path": "/蓝奏云优享版",
      "search_path": "/"
    }
  ]
}
```

### 3. NapCat 群文件 Stream 配置

```json
{
  "napcat": {
    "group_file_stream_enabled": true,
    "ws_url": "ws://127.0.0.1:3001",
    "access_token": "",
    "stream_chunk_size_kb": 64,
    "stream_file_retention_seconds": 30
  }
}
```

- `group_file_stream_enabled`
  开启后，群里发送文件会优先走 NapCat 官方推荐的 `upload_file_stream`
- `ws_url`
  NapCat WebSocket 服务端地址，需要你在 NapCat 网络配置里额外开一个 WebSocket 服务端
- `access_token`
  如果你给这个 WebSocket 服务端设置了 token，就填这里

### 4. 激活配置

```json
{
  "activation": {
    "enabled": true,
    "auto_rotate_enabled": true,
    "rotate_hours": 24,
    "prewarm_minutes": 30,
    "code_length": 8,
    "quark_cookie": "你的夸克 Cookie",
    "quark_save_fid": "最终上传目录对应的夸克 FID",
    "openlist_mount_path": "/夸克",
    "openlist_upload_path": "/激活包",
    "notify_group_uid": "default:GroupMessage:111111111\ndefault:GroupMessage:222222222",
    "quark_verify_ssl": true,
    "quark_share_expired_type": 1
  }
}
```

- `enabled`
  是否开启群聊激活
- `rotate_hours`
  多少小时轮换一次新的激活码
- `auto_rotate_enabled`
  是否按轮换周期自动更新；关闭后只保留当前激活码，直到你手动更新
- `prewarm_minutes`
  距离轮换还有多久时，先开始后台生成并上传下一批激活包
- `code_length`
  激活码长度
- `quark_cookie`
  用来创建夸克原生分享
- `quark_save_fid`
  最终上传目录在夸克里的 FID，要和下面的 OpenList 上传目录对应到同一个实际目录
- `openlist_mount_path`
  OpenList 里挂载夸克的根路径
- `openlist_upload_path`
  相对挂载根路径的上传子目录，插件会自动建目录
- `notify_group_uid`
  接收“新激活链接”通知的目标群 UID 列表。建议直接在目标群发送 `/sid`，把 AstrBot 返回的那整串 `unified_msg_origin` 原样填进来。多个群可以用换行、英文逗号、中文逗号或分号分隔

## 手动更新激活码

管理员可以直接发送下面任一命令，立即生成并切换到新的激活码：

- `更新激活码`
- `刷新激活码`

如果当前已经有一轮激活包在后台生成，插件会等这一轮完成后再切换，不会在处理中途硬切。

## 默认行为

- 搜索结果每页显示 `10` 条
- 搜索会话默认 `10` 分钟无操作超时
- 文件调取默认全局排队，任务之间默认间隔 `2` 秒
- 群文件如果开启了 NapCat Stream，会优先走 `upload_file_stream -> upload_group_file`
- 搜索结果很多时，会先分批预取，不再等全量拉完才返回第一页

## 运行数据目录

插件运行时会在下面这个目录写入缓存和状态：

`data/plugin_data/astrbot_plugin_lanzou_search/`

主要文件有：
- `downloads/`
  临时下载目录
- `activation/`
  生成激活包时的临时目录
- `activation_state.json`
  当前激活批次状态
- `activation_users.json`
  当前批次已激活用户记录

## 打包内容

上传插件压缩包时，保留这些文件即可：
- `main.py`
- `metadata.yaml`
- `README.md`
- `_conf_schema.json`

不要把这些运行时文件打进包里：
- `__pycache__`
- `.gitignore`
- `data/`
