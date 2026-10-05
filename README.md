# 数字档案长期保存服务

仅使用 Python 3.11+ 标准库实现的独立档案保存项目，支持清单校验、真实 SHA-256 内容校验、多个离线副本、损坏检测与自动修复、格式迁移、保留期限和访问控制。

接收链：**移交批次（幂等验收）→ 档案版本 → 保存地点副本（容量排队/重试）→ 巡检批次（可续作）**。同一批移交会同时落到主馆与异地保存点。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

服务地址为 <http://127.0.0.1:8102>，默认数据库 `preservation.db`。测试：

```bash
python3 -m unittest -v
```

演示用户：`owner`、`archivist`、`auditor`、`outsider`。API 使用 `X-User-Id`。文件通过 Base64 提交，单文件上限 10 MiB；这是为了保持示例自包含，生产部署应换成对象存储和流式上传。

种子数据自带两个保存地点：`site-main`（主馆）与 `site-offsite`（异地保存点），各 64 MiB。

## 主要接口

- `POST /api/archives`：创建受限档案。
- `POST /api/archives/{id}/members`：所有者授予 read/write 权限。
- `POST /api/archives/{id}/versions`：提交文件清单，服务端重新计算哈希和大小。可带 `batch_key` 与 `locations`：
  - 同一 `batch_key` 重复提交只验收一次；后到者收到 `duplicate: true` 及先到接收人、接收时间，其填写内容保留为批次草稿（`GET /api/archives/{id}/status` 的 `transfers` 中可见）。
  - 携带批次时自动在指定地点（默认全部已登记地点）落地副本，两地同时接收。
- `POST /api/locations` / `GET /api/locations`：登记/调整保存地点容量；查看各地点 `used_bytes`/`available_bytes` 与副本状态分布。
- `GET /api/versions/{id}`：查看版本、文件清单和副本状态。
- `GET /api/versions/{id}/read?path=...`：校验读回，现场复核哈希后返回内容。
- `POST /api/versions/{id}/copies`：在指定保存地点创建副本；容量不足时副本进入 `pending` 排队。
- `POST /api/copies/{id}/retry`：重试排队（`pending`）或失败（`failed`）的副本，容量允许时从主档重新固化。
- `POST /api/copies/{id}/verify`：校验副本；发现损坏时从健康副本修复，无法修复则标记 `failed`。
- `POST /api/copies/{id}/simulate-corruption`：演示/测试介质损坏，仅 owner 或 archivist 可用。
- `POST /api/inspection-batches`：按档案创建巡检批次（可指定 `copy_ids`，默认全部副本）。
- `POST /api/inspection-batches/{id}/run`：运行巡检，只处理未通过项；失败副本留在批次中等待下一轮重试，全部通过后批次 `completed`。
- `GET /api/inspection-batches/{id}`：查看巡检批次与每项结果。
- `POST /api/versions/{id}/migrate`：生成格式迁移后的新版本并保留派生关系；副本链延续到新版本（同地点落地，容量不足则排队）。
- `GET /api/archives/{id}/status`：保留期限、版本状态、移交批次与草稿、审计记录。

## 语义约定

- 档案路径拒绝绝对路径和 `..`；同一版本副本位置唯一。
- 容量按副本行的 `reserved_bytes` 记录，落地成功才占用，重试/校验不重复扣减；排队副本占用为 0。
- 副本状态一变即重算版本可用性：存在健康副本为 `verified`，否则 `degraded`。
- 所有变更写入审计日志。
