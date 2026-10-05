# 数字档案长期保存服务

仅使用 Python 3.11+ 标准库实现的独立档案保存项目，支持清单校验、真实 SHA-256 内容校验、多个离线副本、损坏检测与自动修复、格式迁移、保留期限和访问控制。

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

## 主要接口

- `POST /api/archives`：创建受限档案。
- `POST /api/archives/{id}/members`：所有者授予 read/write 权限。
- `POST /api/archives/{id}/versions`：提交文件清单，服务端重新计算哈希和大小。
- `POST /api/archives/{id}/batches`：按 `batch_key` 成批移交，重复提交只验收一次；先到者建版本，后到者保留填写内容并在响应中通过 `received_by` 看到先接收人。
- `GET /api/versions/{id}`：查看版本、文件清单和副本状态。
- `POST /api/versions/{id}/copies`：创建独立副本内容；地点容量不足时进入排队（`state=queued`），不立即占用。
- `POST /api/copies/{id}/verify`：校验副本；发现损坏时从健康副本修复。
- `POST /api/copies/{id}/simulate-corruption`：演示/测试介质损坏，仅 owner 或 archivist 可用。
- `POST /api/versions/{id}/migrate`：生成格式迁移后的新版本并保留派生关系，同时继承源版本副本。
- `GET /api/archives/{id}/status`：保留期限、版本状态和审计记录。
- `POST /api/locations`：定义保存地点与容量（字节）。
- `GET /api/locations`：查看各地点容量与占用。
- `POST /api/inspections`：创建巡检批次，先激活排队副本再逐副本校验；失败副本留在批次。
- `POST /api/inspections/{id}/retry`：续作批次，只重试未达健康/已激活的条目。
- `GET /api/inspections/{id}`：查看巡检批次与条目。

档案路径拒绝绝对路径和 `..`；同一版本副本位置唯一；没有健康副本时版本标记为 `degraded`，副本状态一旦变化即重算版本可用性；所有变更写入审计日志。

## 可续作接收链

`ingest_batch` 以 `batch_key` 去重，配合 `BEGIN IMMEDIATE` 与唯一约束，保证并发移交只验收一次：先到者创建版本，后到者的文件清单与地点选择写入 `intake_submissions` 保留，响应 `received_by` 指向先接收人。副本占用由 `copy_files` 实际派生，成功写入只计一次，重试与修复不重复扣减；容量不足时副本进入 `copy_queue` 排队，巡检批次在容量恢复后续作激活。巡检批次（`inspections`）把版本、地点与副本状态串成可重试的接收链：失败条目留在批次，`retry_inspection` 只处理未完成条目。
