# 溢油应急响应与任务追踪

围控、回收、岸线保护和废弃物处置任务，按证据和监测结果闭环；船载、无人机、岸站多源上报经**可恢复收件批次**去重入库。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限、关闭不变量、快照/分片指纹与监测失效判定。
- `src/repository.py`：SQLite建表与旧库迁移、事务、版本控制、收件批次、快照、仲裁、草稿和审计链。
- `src/service.py`：权限检查、用例编排、并发控制、批次采用、复核和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败场景、收件批次与HTTP测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8320
```

默认端口为`8320`，首次启动自动建库；打开旧库时自动迁移并把**缺批次号的旧事件升级为历史基线**（批次号 `HISTORICAL-BASELINE`）。使用`X-Actor`和`X-Role`请求头传递身份。

允许角色：observer, response_commander, operations, viewer。来源：vessel（船载）、drone（无人机）、shore（岸站）、manual、legacy。

## 可恢复收件批次

船载/无人机/岸站重复上报、断网补传夹带旧版本，按批次分片收件：

1. `POST /api/batches`：`{batch_id, source, total_chunks}` 开启批次。`batch_id` 由上报端生成（如设备号+序列号），重试必须沿用原批次号。
2. `POST /api/batches/{batch_id}/chunks/{chunk_index}`：`{entries:[快照...]}` 上传分片。分片按规范化内容哈希存证；同序号重传幂等，内容与首次不一致直接 409。
3. `POST /api/batches/{batch_id}/complete`：分片到齐才提交；未到齐返回缺失序号。中断后按原批次号续传；批次完成结果一次性固化，**重试完成直接沿用首次结果**。
4. `GET /api/batches?batch_id=...`：查询批次状态、已收分片与首次结果。

快照字段：`external_ref`（事件编号）、`snapshot_at`（ISO-8601）、`title/description/severity/quantity/threshold`、可选 `records[]`。

采用规则：

- **同编号只采最早快照**：编号首次出现即建事件并采用该快照；其后的同内容上报记为 `duplicate`。
- **内容不同就留待裁**：任何字段不同（含补传夹带的更旧时间戳版本）都不覆盖，进入待裁队列 `pending_snapshots`，由 response_commander 裁决 accept/reject。
- **不能覆盖确认数量**：事件数量经 `POST /api/items/{id}/confirm-quantity` 确认后锁定；数量不同的快照以 `quantity_confirmed` 原因单列待裁，即使仲裁 accept 也只更新其他字段，确认数量永不被覆盖。

待裁接口：`GET /api/pending`、`POST /api/pending/{id}/adjudicate`（`{decision:"accept"|"reject"}`）。

## 监测失效与退回复核

`POST /api/items/{id}/monitoring` 提交监测证据（`severity`、`quantity`、`spill_active`、`external_ref`、`note`）。当观察导致**等级、期限参数或关闭结论失效**时：

- 事件退回 `review` 状态（`monitoring → review`、`closed → review`），原等级/期限/关闭结论标记 `conclusions_invalid`，并保留提议值；
- `review → assessing` 重新走响应流程；
- 数量未确认时可随监测更新；已确认则只记录分歧（原因 `quantity_confirmed`），数量不动。
- 也可 `POST /api/items/{id}/rewind` 由授权角色手动退回复核。

## 并发推进与草稿

两人同时推进同一事件时只接受当前版本：`POST /api/items/{id}/transition` 必须带 `expected_version`，版本过期的后到者不丢失操作，自动保存为草稿（409 响应带 `draft_id`）。

- `GET /api/drafts`、`GET /api/items/{id}/drafts`：查看草稿；
- `POST /api/drafts/{id}/apply`：以当前版本应用草稿（目标状态仍需合法、仍受关闭不变量约束）；
- `POST /api/drafts/{id}/discard`：放弃草稿。

## 其他接口

- `GET /health`
- `GET /api/items`（可带 `?status=`）、`POST /api/items`（直报，自动生成 `MANUAL-*` 批次）
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/items/{id}/records`
- `GET /api/audit`

估算油量、海况和未完成任务数影响响应等级；关闭前必须完成回收和岸线监测记录。所有采用、仲裁、确认、复核、草稿动作均写入 SHA-256 哈希链审计。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
