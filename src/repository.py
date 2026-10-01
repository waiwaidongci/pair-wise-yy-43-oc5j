from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ALL_STATES, HISTORICAL_BATCH_ID, STATES


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

    # ---------- schema ----------
    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in ALL_STATES)
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
                    batch_id TEXT,
                    source TEXT,
                    quantity_confirmed INTEGER NOT NULL DEFAULT 0,
                    review_reason TEXT,
                    closed_at TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    source TEXT,
                    batch_id TEXT,
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
                CREATE TABLE IF NOT EXISTS ingest_batches (
                    batch_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    total_chunks INTEGER NOT NULL CHECK(total_chunks >= 0),
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','completed')),
                    result TEXT,
                    historical INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS ingest_chunks (
                    batch_id TEXT NOT NULL REFERENCES ingest_batches(batch_id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(batch_id, chunk_index)
                );
                CREATE TABLE IF NOT EXISTS event_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER REFERENCES items(id) ON DELETE CASCADE,
                    external_ref TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    snapshot_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    adopted INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(batch_id, external_ref, content_hash)
                );
                CREATE INDEX IF NOT EXISTS ix_snapshots_ref
                    ON event_snapshots(external_ref, snapshot_at);
                CREATE TABLE IF NOT EXISTS pending_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    external_ref TEXT NOT NULL,
                    snapshot_id INTEGER NOT NULL REFERENCES event_snapshots(id) ON DELETE CASCADE,
                    reason TEXT NOT NULL
                        CHECK(reason IN ('content_differs','quantity_confirmed')),
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','accepted','rejected')),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by TEXT
                );
                CREATE TABLE IF NOT EXISTS transition_drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    target TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    current_version INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'stored'
                        CHECK(status IN ('stored','applied','discarded')),
                    created_at TEXT NOT NULL,
                    applied_at TEXT
                );
            """)
        self._migrate_items()

    def _migrate_items(self) -> None:
        """旧库缺少批次列/复核状态CHECK时，整表重建升级。"""
        with self._lock:
            cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(items)")}
            needed = {"batch_id", "source", "quantity_confirmed", "review_reason", "closed_at"}
            if not needed - cols:
                return
            old_check = self.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='items'"
            ).fetchone()["sql"]
            if "review" in old_check and not (needed - cols):
                return
            statuses = ",".join("'" + s + "'" for s in ALL_STATES)
            with self.conn:
                self.conn.execute("PRAGMA foreign_keys=OFF")
                self.conn.execute("ALTER TABLE items RENAME TO items_legacy")
                self.conn.execute(f"""
                    CREATE TABLE items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL, description TEXT NOT NULL,
                        severity TEXT NOT NULL, quantity REAL NOT NULL DEFAULT 0,
                        threshold REAL NOT NULL DEFAULT 1,
                        status TEXT NOT NULL CHECK(status IN ({statuses})),
                        version INTEGER NOT NULL DEFAULT 1,
                        external_ref TEXT, batch_id TEXT, source TEXT,
                        quantity_confirmed INTEGER NOT NULL DEFAULT 0,
                        review_reason TEXT, closed_at TEXT,
                        created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )""")
                common = [c for c in (
                    "id","title","description","severity","quantity","threshold",
                    "status","version","external_ref","created_by","created_at","updated_at"
                ) if c in cols]
                select = ",".join(common)
                self.conn.execute(
                    f"INSERT INTO items({select}) SELECT {select} FROM items_legacy")
                self.conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref "
                    "ON items(external_ref) WHERE external_ref IS NOT NULL")
                self.conn.execute("DROP TABLE items_legacy")
                self.conn.execute("PRAGMA foreign_keys=ON")
                self.conn.execute("PRAGMA foreign_key_check")

    def bootstrap_historical_baseline(self, actor: str = "system") -> Optional[int]:
        """旧事件缺批次号时升级为历史基线批次，返回本次升级的事件数。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM items WHERE batch_id IS NULL").fetchone()
            count = int(row["n"])
            if count == 0:
                return 0
            self.conn.execute(
                """INSERT INTO ingest_batches(batch_id, source, total_chunks, status,
                   historical, created_by, created_at, completed_at)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO NOTHING""",
                (HISTORICAL_BATCH_ID, "legacy", 0, "completed", 1, actor, now, now))
            self.conn.execute(
                """UPDATE items SET batch_id=?, source=COALESCE(source,'legacy')
                   WHERE batch_id IS NULL""", (HISTORICAL_BATCH_ID,))
        return count

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    # ---------- items ----------
    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str, batch_id: Optional[str] = None,
                    source: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, batch_id, source,
                       created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, batch_id, source, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def assign_batch(self, item_id: int, batch_id: str, source: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE items SET batch_id=?, source=? WHERE id=?",
                (batch_id, source, item_id))

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def get_item_by_ref(self, external_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE external_ref=?", (external_ref,)).fetchone()
        return self._item(row) if row else None

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
        closed_at = now if target == "closed" else None
        with self._lock, self.conn:
            if target == "closed":
                cur = self.conn.execute(
                    """UPDATE items SET status=?, version=version+1, updated_at=?,
                       closed_at=?, review_reason=NULL
                       WHERE id=? AND version=?""",
                    (target, now, closed_at, item_id, expected_version))
            else:
                cur = self.conn.execute(
                    """UPDATE items SET status=?, version=version+1, updated_at=?,
                       review_reason=CASE WHEN ?='assessing' THEN NULL ELSE review_reason END
                       WHERE id=? AND version=?""",
                    (target, now, target, item_id, expected_version))
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def rewind_item(self, item_id: int, reason_payload: dict,
                    observed_quantity: Optional[float], actor: str,
                    expected_version: Optional[int] = None) -> Dict[str, Any]:
        """退回复核：监测变化使等级/期限/关闭结论失效。数量未确认时才更新。"""
        now = utc_now()
        reason = json.dumps(reason_payload, ensure_ascii=False, sort_keys=True)
        with self._lock, self.conn:
            if observed_quantity is not None:
                sql = ("UPDATE items SET status='review', version=version+1, updated_at=?, "
                       "closed_at=NULL, review_reason=?, quantity=? WHERE id=?")
                params: tuple = (now, reason, observed_quantity, item_id)
            else:
                sql = ("UPDATE items SET status='review', version=version+1, updated_at=?, "
                       "closed_at=NULL, review_reason=? WHERE id=?")
                params = (now, reason, item_id)
            if expected_version is not None:
                sql += " AND version=?"
                params = params + (expected_version,)
            cur = self.conn.execute(sql, params)
            if cur.rowcount == 0 and expected_version is not None:
                if self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone() is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def apply_snapshot_to_item(self, item_id: int, entry: dict,
                               quantity_confirmed: bool) -> Dict[str, Any]:
        """仲裁通过后采用新快照内容；已确认数量永不覆盖。"""
        now = utc_now()
        with self._lock, self.conn:
            if quantity_confirmed:
                self.conn.execute(
                    """UPDATE items SET title=?, description=?, severity=?, threshold=?,
                       version=version+1, updated_at=? WHERE id=?""",
                    (entry["title"], entry["description"], entry["severity"],
                     entry["threshold"], now, item_id))
            else:
                self.conn.execute(
                    """UPDATE items SET title=?, description=?, severity=?, quantity=?,
                       threshold=?, version=version+1, updated_at=? WHERE id=?""",
                    (entry["title"], entry["description"], entry["severity"],
                     entry["quantity"], entry["threshold"], now, item_id))
        return self.get_item(item_id)

    def confirm_quantity(self, item_id: int, expected_version: int,
                         quantity: Optional[float], actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            if quantity is not None:
                cur = self.conn.execute(
                    """UPDATE items SET quantity=?, quantity_confirmed=1,
                       version=version+1, updated_at=? WHERE id=? AND version=?""",
                    (quantity, now, item_id, expected_version))
            else:
                cur = self.conn.execute(
                    """UPDATE items SET quantity_confirmed=1, version=version+1, updated_at=?
                       WHERE id=? AND version=?""",
                    (now, item_id, expected_version))
            if cur.rowcount == 0:
                if self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone() is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    # ---------- records ----------
    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str,
                   source: Optional[str] = None,
                   batch_id: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       source, batch_id, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, source, batch_id,
                     actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def has_record_ref(self, item_id: int, external_ref: Optional[str]) -> bool:
        if external_ref is None:
            return False
        with self._lock:
            row = self.conn.execute(
                "SELECT 1 FROM records WHERE item_id=? AND external_ref=?",
                (item_id, external_ref)).fetchone()
        return row is not None

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

    # ---------- ingest batches ----------
    def open_batch(self, batch_id: str, source: str, total_chunks: int,
                   actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO ingest_batches(batch_id, source, total_chunks, status,
                   created_by, created_at) VALUES(?,?,?,'open',?,?)
                   ON CONFLICT(batch_id) DO NOTHING""",
                (batch_id, source, total_chunks, actor, now))
            if cur.rowcount == 0:
                row = self.conn.execute(
                    "SELECT * FROM ingest_batches WHERE batch_id=?", (batch_id,)).fetchone()
                if row["total_chunks"] != total_chunks or row["source"] != source:
                    raise ConflictError("批次已存在且分片数或来源不一致")
        batch = self.get_batch(batch_id)
        batch["created"] = cur.rowcount > 0
        return batch

    def get_batch(self, batch_id: str) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM ingest_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        batch = dict(row)
        with self._lock:
            chunk_rows = self.conn.execute(
                "SELECT chunk_index, content_hash FROM ingest_chunks WHERE batch_id=? ORDER BY chunk_index",
                (batch_id,)).fetchall()
        batch["received_chunks"] = [r["chunk_index"] for r in chunk_rows]
        batch["chunk_hashes"] = {r["chunk_index"]: r["content_hash"] for r in chunk_rows}
        if batch["result"]:
            batch["result"] = json.loads(batch["result"])
        return batch

    def put_chunk(self, batch_id: str, chunk_index: int, payload: List[dict],
                  content_hash: str, actor: str) -> Dict[str, Any]:
        now = utc_now()
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._lock, self.conn:
            batch = self.conn.execute(
                "SELECT * FROM ingest_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if batch is None:
                raise NotFoundError("批次不存在，请先开启批次")
            if batch["status"] == "completed":
                raise ConflictError("批次已完成，不能再写入分片")
            if not 0 <= chunk_index < batch["total_chunks"]:
                raise ConflictError("分片序号超出批次范围")
            existing = self.conn.execute(
                "SELECT content_hash FROM ingest_chunks WHERE batch_id=? AND chunk_index=?",
                (batch_id, chunk_index)).fetchone()
            if existing is not None:
                if existing["content_hash"] != content_hash:
                    raise ConflictError("分片内容与首次上传不一致")
                return {"batch_id": batch_id, "chunk_index": chunk_index,
                        "content_hash": content_hash, "resubmitted": True}
            self.conn.execute(
                """INSERT INTO ingest_chunks(batch_id, chunk_index, payload, content_hash,
                   created_by, created_at) VALUES(?,?,?,?,?,?)""",
                (batch_id, chunk_index, raw, content_hash, actor, now))
        return {"batch_id": batch_id, "chunk_index": chunk_index,
                "content_hash": content_hash, "resubmitted": False}

    def get_chunk_payload(self, batch_id: str, chunk_index: int) -> List[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT payload FROM ingest_chunks WHERE batch_id=? AND chunk_index=?",
                (batch_id, chunk_index)).fetchone()
        if row is None:
            raise NotFoundError("分片不存在")
        return json.loads(row["payload"])

    def all_chunks_present(self, batch_id: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                """SELECT b.total_chunks AS total, COUNT(c.chunk_index) AS got
                   FROM ingest_batches b LEFT JOIN ingest_chunks c
                   ON c.batch_id=b.batch_id WHERE b.batch_id=?""",
                (batch_id,)).fetchone()
        return int(row["got"]) >= int(row["total"])

    def complete_batch(self, batch_id: str, result: dict) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT status, result FROM ingest_batches WHERE batch_id=?",
                (batch_id,)).fetchone()
            if row is None:
                raise NotFoundError("批次不存在")
            if row["status"] == "completed":
                return json.loads(row["result"])
            self.conn.execute(
                "UPDATE ingest_batches SET status='completed', result=?, completed_at=? WHERE batch_id=?",
                (json.dumps(result, ensure_ascii=False, sort_keys=True), now, batch_id))
        return result

    # ---------- snapshots ----------
    def insert_snapshot(self, entry: dict, batch_id: str, source: str,
                        content_hash: str, adopted: bool,
                        outcome: str, item_id: Optional[int]) -> int:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO event_snapshots(item_id, external_ref, batch_id, source,
                   snapshot_at, content_hash, payload, adopted, outcome, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id, external_ref, content_hash) DO NOTHING""",
                (item_id, entry["external_ref"], batch_id, source, entry["snapshot_at"],
                 content_hash, json.dumps(entry, ensure_ascii=False, sort_keys=True),
                 1 if adopted else 0, outcome, now))
            if cur.lastrowid:
                return int(cur.lastrowid)
            row = self.conn.execute(
                "SELECT id FROM event_snapshots WHERE batch_id=? AND external_ref=? AND content_hash=?",
                (batch_id, entry["external_ref"], content_hash)).fetchone()
        return int(row["id"])

    def find_adopted_snapshot(self, external_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                """SELECT * FROM event_snapshots WHERE external_ref=? AND adopted=1
                   ORDER BY snapshot_at ASC, id ASC LIMIT 1""",
                (external_ref,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["payload_obj"] = json.loads(row["payload"])
        return result

    def find_identical_snapshot(self, external_ref: str,
                                content_hash: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM event_snapshots WHERE external_ref=? AND content_hash=? ORDER BY id LIMIT 1",
                (external_ref, content_hash)).fetchone()
        return dict(row) if row else None

    def open_pending_for_snapshot(self, snapshot_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM pending_snapshots WHERE snapshot_id=? AND status='pending'",
                (snapshot_id,)).fetchone()
        return dict(row) if row else None

    def add_pending(self, item_id: int, external_ref: str, snapshot_id: int,
                    reason: str, actor: str) -> int:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO pending_snapshots(item_id, external_ref, snapshot_id,
                   reason, created_by, created_at) VALUES(?,?,?,?,?,?)""",
                (item_id, external_ref, snapshot_id, reason, actor, now))
            return int(cur.lastrowid)

    def list_pending(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM pending_snapshots WHERE status='pending' ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def get_pending(self, pending_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM pending_snapshots WHERE id=?", (pending_id,)).fetchone()
        if row is None:
            raise NotFoundError("待裁快照不存在")
        return dict(row)

    def get_snapshot(self, snapshot_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM event_snapshots WHERE id=?", (snapshot_id,)).fetchone()
        if row is None:
            raise NotFoundError("快照不存在")
        result = dict(row)
        result["payload_obj"] = json.loads(row["payload"])
        return result

    def resolve_pending(self, pending_id: int, decision: str, actor: str) -> None:
        now = utc_now()
        status = "accepted" if decision == "accept" else "rejected"
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE pending_snapshots SET status=?, resolved_at=?, resolved_by=?
                   WHERE id=? AND status='pending'""",
                (status, now, actor, pending_id))
            if cur.rowcount == 0:
                if self.conn.execute("SELECT 1 FROM pending_snapshots WHERE id=?", (pending_id,)).fetchone() is None:
                    raise NotFoundError("待裁快照不存在")
                raise ConflictError("待裁快照已有结论")

    def mark_snapshot_adopted(self, snapshot_id: int) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE event_snapshots SET adopted=1, outcome='accepted' WHERE id=?",
                (snapshot_id,))

    # ---------- drafts ----------
    def save_draft(self, item_id: int, target: str, base_version: int,
                   current_version: int, actor: str) -> int:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO transition_drafts(item_id, target, base_version,
                   current_version, actor, created_at) VALUES(?,?,?,?,?,?)""",
                (item_id, target, base_version, current_version, actor, now))
            return int(cur.lastrowid)

    def get_draft(self, draft_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM transition_drafts WHERE id=?", (draft_id,)).fetchone()
        if row is None:
            raise NotFoundError("草稿不存在")
        return dict(row)

    def list_drafts(self, item_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM transition_drafts WHERE status='stored'"
        params: tuple = ()
        if item_id is not None:
            sql += " AND item_id=?"
            params = (item_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def mark_draft(self, draft_id: int, status: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            if status == "applied":
                cur = self.conn.execute(
                    "UPDATE transition_drafts SET status='applied', applied_at=? WHERE id=? AND status='stored'",
                    (now, draft_id))
            else:
                cur = self.conn.execute(
                    "UPDATE transition_drafts SET status=? WHERE id=? AND status='stored'",
                    (status, draft_id))
            if cur.rowcount == 0:
                if self.conn.execute("SELECT 1 FROM transition_drafts WHERE id=?", (draft_id,)).fetchone() is None:
                    raise NotFoundError("草稿不存在")
                raise ConflictError("草稿已处理")

    # ---------- audit ----------
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

    def close(self) -> None:
        with self._lock:
            self.conn.close()
