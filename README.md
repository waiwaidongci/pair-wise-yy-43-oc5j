# 溢油应急响应与任务追踪

围控、回收、岸线保护和废弃物处置任务，按证据和监测结果闭环。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8320
```

默认端口为`8320`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/items/{id}/drafts`
- `POST /api/items/{id}/drafts/{draft_id}/apply`
- `GET /api/audit`
- `GET /api/adjudication`
- `POST /api/admin/baseline`
- `POST /api/intake/batches`
- `GET /api/intake/batches/{batch_no}`
- `POST /api/intake/batches/{batch_no}/shards`
- `POST /api/intake/batches/{batch_no}/commit`

允许角色：observer, response_commander, operations, viewer。估算油量、海况和未完成任务数影响响应等级；关闭前必须完成回收和岸线监测记录。

## 收件批次与可恢复上报

船载、无人机、岸站重复上报或断网补传时，事件、记录和审计按`batch_no`接入同一收件批次：

- **分片入库，续传幂等**：批次按`shard_no`分片提交，中断后用原批次号续传；同一分片重试只返回首次结果，不重复落库。
- **同编号采最早快照**：`external_ref`相同且内容一致时保留最早一条，不重复创建。
- **内容不同留待裁**：同编号但等级、油量等内容不一致时，生成裁决案例（`/api/adjudication`），不覆盖原事件。
- **已确认数量不可覆盖**：事件进入`containing`后数量即确认，后续上报油量冲突时标记为`confirmed_quantity_conflict`。
- **监测变化退回复核**：监测记录油量远超阈值，导致等级、期限或关闭结论失效时，事件自动退回`assessing`并记录审计。
- **并发推进保留草稿**：两人同时推进只接受当前版本，版本不符的后到请求保留为草稿，可在草稿上继续。
- **历史基线**：缺少批次号的旧事件通过`/api/admin/baseline`升级到`BASELINE-LEGACY`基线批次，幂等执行。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
