from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ID_PREFIX, STATES


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    batch_no TEXT,
                    quantity_confirmed INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE INDEX IF NOT EXISTS ix_items_batch_no ON items(batch_no);
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    batch_no TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS intake_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','committed')),
                    shard_count INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    committed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS intake_shards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL REFERENCES intake_batches(batch_no),
                    shard_no INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL,
                    result TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'stored'
                        CHECK(status IN ('stored','applied')),
                    created_at TEXT NOT NULL,
                    UNIQUE(batch_no, shard_no)
                );
                CREATE INDEX IF NOT EXISTS ix_shards_batch
                    ON intake_shards(batch_no, shard_no);
                CREATE TABLE IF NOT EXISTS adjudication_cases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT,
                    shard_no INTEGER,
                    external_ref TEXT,
                    item_id INTEGER REFERENCES items(id),
                    reason TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','resolved')),
                    resolution TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by TEXT
                );
                CREATE INDEX IF NOT EXISTS ix_adjudication_status
                    ON adjudication_cases(status);
                CREATE TABLE IF NOT EXISTS drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    target TEXT NOT NULL,
                    expected_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft'
                        CHECK(status IN ('draft','applied','discarded')),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    applied_at TEXT
                );
                CREATE INDEX IF NOT EXISTS ix_drafts_item ON drafts(item_id, status);
            """)
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        """为旧库补齐批次相关列（幂等迁移）。"""
        additions = [
            ("items", "batch_no", "ALTER TABLE items ADD COLUMN batch_no TEXT"),
            ("items", "quantity_confirmed",
             "ALTER TABLE items ADD COLUMN quantity_confirmed INTEGER NOT NULL DEFAULT 0"),
            ("records", "batch_no", "ALTER TABLE records ADD COLUMN batch_no TEXT"),
        ]
        for table, column, ddl in additions:
            cols = [row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")]
            if column not in cols:
                with self.conn:
                    self.conn.execute(ddl)

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str, batch_no: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, batch_no, quantity_confirmed,
                       created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,0,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, batch_no, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def find_item_by_external_ref(self, external_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE external_ref=?", (external_ref,)
            ).fetchone()
        return self._item(row) if row is not None else None

    def set_quantity_confirmed(self, item_id: int) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE items SET quantity_confirmed=1 WHERE id=?", (item_id,)
            )

    def revert_item_to_review(self, item_id: int) -> bool:
        """监测变化导致等级/期限/关闭失效时，退回复核（assessing）。"""
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status='assessing', version=version+1, updated_at=?
                   WHERE id=? AND status!='assessing'""",
                (now, item_id),
            )
            return cur.rowcount > 0

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str,
                   batch_no: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       batch_no, created_by, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, batch_no, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    # ---- 收件批次（可恢复 intake batches） ----
    def create_intake_batch(self, batch_no: str, source: str, actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """INSERT INTO intake_batches(batch_no, source, status, shard_count,
                   created_by, created_at, updated_at) VALUES(?,?,?,0,?,?,?)""",
                (batch_no, source, "open", actor, now, now),
            )
        return self.get_intake_batch(batch_no)

    def get_intake_batch(self, batch_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM intake_batches WHERE batch_no=?", (batch_no,)
            ).fetchone()
        return dict(row) if row is not None else None

    def add_intake_shard(self, batch_no: str, shard_no: int, payload: Any,
                         result: Any) -> tuple:
        """分片入库；若幂等键已存在则沿用首次结果。返回 (shard, created)。"""
        now = utc_now()
        idem = f"{batch_no}#{shard_no}"
        try:
            with self._lock, self.conn:
                self.conn.execute(
                    """INSERT INTO intake_shards(batch_no, shard_no, idempotency_key,
                       payload, result, status, created_at)
                       VALUES(?,?,?,?,?, 'stored', ?)""",
                    (batch_no, shard_no, idem,
                     json.dumps(payload, ensure_ascii=False),
                     json.dumps(result, ensure_ascii=False), now),
                )
                self.conn.execute(
                    "UPDATE intake_batches SET shard_count=shard_count+1, updated_at=? WHERE batch_no=?",
                    (now, batch_no),
                )
        except sqlite3.IntegrityError:
            return self.get_intake_shard(idem), False
        return self.get_intake_shard(idem), True

    def get_intake_shard(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM intake_shards WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
        if row is None:
            return None
        shard = dict(row)
        shard["payload"] = json.loads(shard["payload"])
        shard["result"] = json.loads(shard["result"])
        return shard

    def list_intake_shards(self, batch_no: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM intake_shards WHERE batch_no=? ORDER BY shard_no",
                (batch_no,),
            ).fetchall()
        result = []
        for row in rows:
            shard = dict(row)
            shard["payload"] = json.loads(shard["payload"])
            shard["result"] = json.loads(shard["result"])
            result.append(shard)
        return result

    def commit_intake_batch(self, batch_no: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE intake_batches
                   SET status='committed', committed_at=?, updated_at=?
                   WHERE batch_no=? AND status='open'""",
                (now, now, batch_no),
            )
        return self.get_intake_batch(batch_no)

    # ---- 留待裁（adjudication） ----
    def create_adjudication_case(self, batch_no: Optional[str], shard_no: int,
                                 external_ref: Optional[str], item_id: Optional[int],
                                 reason: str, payload: Any,
                                 actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO adjudication_cases(batch_no, shard_no, external_ref, item_id,
                   reason, payload, status, created_by, created_at)
                   VALUES(?,?,?,?,?,?,'pending',?,?)""",
                (batch_no, shard_no, external_ref, item_id, reason,
                 json.dumps(payload, ensure_ascii=False), actor, now),
            )
            case_id = int(cur.lastrowid)
        return self.get_adjudication_case(case_id)

    def get_adjudication_case(self, case_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM adjudication_cases WHERE id=?", (case_id,)
            ).fetchone()
        if row is None:
            return None
        case = dict(row)
        case["payload"] = json.loads(case["payload"])
        return case

    def list_adjudication_cases(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM adjudication_cases"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            case = dict(row)
            case["payload"] = json.loads(case["payload"])
            result.append(case)
        return result

    def resolve_adjudication_case(self, case_id: int, resolution: str,
                                  actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE adjudication_cases
                   SET status='resolved', resolution=?, resolved_at=?, resolved_by=?
                   WHERE id=? AND status='pending'""",
                (resolution, now, actor, case_id),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute(
                    "SELECT 1 FROM adjudication_cases WHERE id=?", (case_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError("裁决案例不存在")
                raise ConflictError("裁决案例已处理")
        return self.get_adjudication_case(case_id)

    # ---- 草稿（drafts，后到者保留草稿） ----
    def create_draft(self, item_id: int, target: str, expected_version: int,
                     payload: Any, reason: str, actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO drafts(item_id, target, expected_version, payload, reason,
                   status, created_by, created_at)
                   VALUES(?,?,?,?,?,'draft',?,?)""",
                (item_id, target, expected_version,
                 json.dumps(payload, ensure_ascii=False), reason, actor, now),
            )
            draft_id = int(cur.lastrowid)
        return self.get_draft(draft_id)

    def get_draft(self, draft_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM drafts WHERE id=?", (draft_id,)
            ).fetchone()
        if row is None:
            return None
        draft = dict(row)
        draft["payload"] = json.loads(draft["payload"])
        return draft

    def list_drafts(self, item_id: int,
                    status: Optional[str] = "draft") -> List[Dict[str, Any]]:
        sql = "SELECT * FROM drafts WHERE item_id=?"
        params: list = [item_id]
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        result = []
        for row in rows:
            draft = dict(row)
            draft["payload"] = json.loads(draft["payload"])
            result.append(draft)
        return result

    def mark_draft_applied(self, draft_id: int) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE drafts SET status='applied', applied_at=? WHERE id=?",
                (now, draft_id),
            )

    # ---- 历史基线（缺批次号的旧事件升级） ----
    def backfill_baseline(self, batch_no: str, actor: str) -> int:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                "UPDATE items SET batch_no=? WHERE batch_no IS NULL", (batch_no,)
            )
            count = cur.rowcount
            if count > 0:
                self.conn.execute(
                    """INSERT OR IGNORE INTO intake_batches(batch_no, source, status,
                       shard_count, created_by, created_at, updated_at, committed_at)
                       VALUES(?, 'historical_baseline', 'committed', 0, ?, ?, ?, ?)""",
                    (batch_no, actor, now, now, now),
                )
        return count

    def close(self) -> None:
        with self._lock:
            self.conn.close()
