from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from .domain import (ConflictError, ValidationError, ensure_role,
                     normalize_severity, require_number, require_text)
from .repository import Repository
from .rules import (ADJUDICATE_ROLES, ALL_STATES, AUDIT_ROLES, CREATE_ROLES,
                    DRAFT_ROLES, ENTITY, HISTORICAL_BATCH_ID, INGEST_ROLES,
                    MONITOR_ROLES, RECORD_ROLES, SNAPSHOT_FIELDS, SOURCES,
                    VIEW_ROLES, chunk_hash, completion_blockers,
                    escalation_required, monitoring_invalidation, parse_utc,
                    priority_score, response_deadline_hours,
                    role_for_transition, snapshot_canonical,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository
        self.bootstrap()

    def bootstrap(self) -> None:
        upgraded = self.repository.bootstrap_historical_baseline()
        if upgraded:
            self.repository.append_audit(
                "baseline_upgrade", ENTITY, 0, "system",
                {"batch_id": HISTORICAL_BATCH_ID, "items": upgraded})

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    # ---------- direct create (kept for compatibility) ----------
    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor,
                                           source="manual")
        # 直报事件自带单条批次，避免重启时被误归入历史基线
        batch_id = f"MANUAL-{uuid.uuid4().hex[:12].upper()}"
        self.repository.assign_batch(item["id"], batch_id, "manual")
        item = self.repository.get_item(item["id"])
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
            "source": "manual", "batch_id": batch_id,
        })
        return self.enrich(item)

    # ---------- ingest batches ----------
    def _validate_source(self, source: Any) -> str:
        source = require_text(source, "source", 40)
        if source not in SOURCES:
            raise ValidationError(f"source必须是{sorted(SOURCES)}之一")
        return source

    def open_batch(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, INGEST_ROLES)
        actor = require_text(actor, "actor", 100)
        batch_id = require_text(payload.get("batch_id"), "batch_id", 120)
        source = self._validate_source(payload.get("source"))
        total = payload.get("total_chunks")
        if isinstance(total, bool) or not isinstance(total, int) or total < 1:
            raise ValidationError("total_chunks必须是正整数")
        batch = self.repository.open_batch(batch_id, source, total, actor)
        if batch["created"]:
            self.repository.append_audit("batch_open", "ingest_batch", 0, actor, {
                "batch_id": batch_id, "source": source, "total_chunks": total})
        return batch

    def _validate_snapshot(self, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValidationError("分片条目必须是对象")
        entry: Dict[str, Any] = {}
        entry["external_ref"] = require_text(raw.get("external_ref"), "external_ref", 100)
        entry["snapshot_at"] = parse_utc(raw.get("snapshot_at"))
        entry["title"] = require_text(raw.get("title"), "title", 200)
        entry["description"] = require_text(raw.get("description"), "description")
        entry["severity"] = normalize_severity(raw.get("severity"))
        entry["quantity"] = require_number(raw.get("quantity", 0), "quantity")
        entry["threshold"] = require_number(raw.get("threshold", 1), "threshold", 0.000001)
        records = raw.get("records", [])
        if not isinstance(records, list):
            raise ValidationError("records必须是数组")
        clean_records: List[Dict[str, Any]] = []
        for rec in records:
            if not isinstance(rec, dict):
                raise ValidationError("记录条目必须是对象")
            status = rec.get("status", "open")
            if status not in ("open", "closed"):
                raise ValidationError("记录status必须是open或closed")
            clean = {
                "kind": require_text(rec.get("kind"), "record.kind", 100),
                "detail": require_text(rec.get("detail"), "record.detail"),
                "status": status,
                "external_ref": None,
            }
            ref = rec.get("external_ref")
            if ref is not None:
                clean["external_ref"] = require_text(ref, "record.external_ref", 100)
            clean_records.append(clean)
        entry["records"] = clean_records
        return entry

    def upload_chunk(self, batch_id: str, chunk_index: int,
                     payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, INGEST_ROLES)
        actor = require_text(actor, "actor", 100)
        if isinstance(chunk_index, bool) or not isinstance(chunk_index, int) or chunk_index < 0:
            raise ValidationError("chunk_index必须是非负整数")
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list) or not raw_entries:
            raise ValidationError("entries必须是非空数组")
        entries = [self._validate_snapshot(e) for e in raw_entries]
        content_hash = chunk_hash(entries)
        result = self.repository.put_chunk(batch_id, chunk_index, entries,
                                           content_hash, actor)
        if not result["resubmitted"]:
            self.repository.append_audit("chunk_upload", "ingest_chunk", chunk_index,
                                         actor, {"batch_id": batch_id,
                                                 "chunk_index": chunk_index,
                                                 "content_hash": content_hash,
                                                 "entries": len(entries)})
        result["received_chunks"] = self.repository.get_batch(batch_id)["received_chunks"]
        return result

    def _apply_snapshot(self, entry: Dict[str, Any], batch_id: str, source: str,
                        actor: str, content_hash: str) -> Dict[str, Any]:
        """同编号只采最早快照：已有采用快照则不动；内容不同留待裁；确认数量不覆盖。"""
        external_ref = entry["external_ref"]
        identical = self.repository.find_identical_snapshot(external_ref, content_hash)
        if identical is not None:
            self.repository.insert_snapshot(entry, batch_id, source, content_hash,
                                            adopted=False, outcome="duplicate",
                                            item_id=identical["item_id"])
            return {"external_ref": external_ref, "result": "duplicate",
                    "snapshot_id": identical["id"]}

        adopted = self.repository.find_adopted_snapshot(external_ref)
        if adopted is None:
            item = self.repository.get_item_by_ref(external_ref)
            if item is None:
                item = self.repository.create_item(
                    entry["title"], entry["description"], entry["severity"],
                    entry["quantity"], entry["threshold"], external_ref, actor,
                    batch_id=batch_id, source=source)
                snapshot_id = self.repository.insert_snapshot(
                    entry, batch_id, source, content_hash, adopted=True,
                    outcome="adopted", item_id=item["id"])
                self._ingest_records(item["id"], entry, batch_id, source, actor)
                self.repository.append_audit("snapshot_adopt", ENTITY, item["id"], actor, {
                    "batch_id": batch_id, "external_ref": external_ref,
                    "snapshot_id": snapshot_id, "snapshot_at": entry["snapshot_at"],
                    "quantity_confirmed": False})
                return {"external_ref": external_ref, "result": "adopted",
                        "item_id": item["id"], "snapshot_id": snapshot_id}
            # 编号已被直报占用，且内容与现有快照不同 -> 留待裁
            reason = ("quantity_confirmed" if item["quantity_confirmed"]
                      and float(entry["quantity"]) != float(item["quantity"])
                      else "content_differs")
            snapshot_id = self.repository.insert_snapshot(
                entry, batch_id, source, content_hash, adopted=False,
                outcome="pending", item_id=item["id"])
            pending_id = self._queue_pending(item["id"], entry, snapshot_id, reason, actor)
            return {"external_ref": external_ref, "result": "pending",
                    "reason": reason, "pending_id": pending_id,
                    "item_id": item["id"], "snapshot_id": snapshot_id}

        # 已有最早采用快照：晚到者永远不替换；补传的时间戳更早也留待人工裁
        item = self.repository.get_item(adopted["item_id"])
        reason = ("quantity_confirmed" if item["quantity_confirmed"]
                  and float(entry["quantity"]) != float(item["quantity"])
                  else "content_differs")
        snapshot_id = self.repository.insert_snapshot(
            entry, batch_id, source, content_hash, adopted=False,
            outcome="pending", item_id=item["id"])
        pending_id = self._queue_pending(item["id"], entry, snapshot_id, reason, actor)
        note = ("earlier_than_adopted"
                if entry["snapshot_at"] < adopted["snapshot_at"] else reason)
        return {"external_ref": external_ref, "result": "pending", "reason": note,
                "pending_id": pending_id, "item_id": item["id"],
                "snapshot_id": snapshot_id}

    def _queue_pending(self, item_id: int, entry: Dict[str, Any], snapshot_id: int,
                       reason: str, actor: str) -> int:
        existing = self.repository.open_pending_for_snapshot(snapshot_id)
        if existing is not None:
            return existing["id"]
        pending_id = self.repository.add_pending(
            item_id, entry["external_ref"], snapshot_id, reason, actor)
        self.repository.append_audit("snapshot_pending", ENTITY, item_id, actor, {
            "pending_id": pending_id, "snapshot_id": snapshot_id,
            "external_ref": entry["external_ref"], "reason": reason,
            "snapshot_at": entry["snapshot_at"]})
        return pending_id

    def _ingest_records(self, item_id: int, entry: Dict[str, Any], batch_id: str,
                        source: str, actor: str) -> int:
        inserted = 0
        for rec in entry["records"]:
            if rec["external_ref"] is not None and \
                    self.repository.has_record_ref(item_id, rec["external_ref"]):
                continue
            try:
                self.repository.add_record(
                    item_id, rec["kind"], rec["detail"], rec["status"],
                    rec["external_ref"], actor, source=source, batch_id=batch_id)
                inserted += 1
            except ConflictError:
                continue
        return inserted

    def complete_batch(self, batch_id: str, actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, INGEST_ROLES)
        actor = require_text(actor, "actor", 100)
        batch = self.repository.get_batch(batch_id)
        if batch["status"] == "completed":
            # 重试沿用首次结果
            return {"batch_id": batch_id, "resumed": True,
                    "reused_first_result": True, "result": batch["result"]}
        if not self.repository.all_chunks_present(batch_id):
            missing = sorted(set(range(batch["total_chunks"]))
                             - set(batch["received_chunks"]))
            raise ConflictError(f"分片未到齐，缺少{missing}，请按原批次号续传")
        # 按分片顺序、分片内顺序处理；同编号在批内按 snapshot_at 取最早
        all_entries: List[Dict[str, Any]] = []
        for index in range(batch["total_chunks"]):
            all_entries.extend(self.repository.get_chunk_payload(batch_id, index))
        all_entries.sort(key=lambda e: (e["snapshot_at"], e["external_ref"]))
        outcomes = [self._apply_snapshot(entry, batch_id, batch["source"], actor,
                                         snapshot_canonical(entry))
                    for entry in all_entries]
        summary = {"adopted": sum(1 for o in outcomes if o["result"] == "adopted"),
                   "pending": sum(1 for o in outcomes if o["result"] == "pending"),
                   "duplicate": sum(1 for o in outcomes if o["result"] == "duplicate"),
                   "total": len(outcomes)}
        result = self.repository.complete_batch(batch_id, summary)
        self.repository.append_audit("batch_complete", "ingest_batch", 0, actor, {
            "batch_id": batch_id, "summary": summary})
        return {"batch_id": batch_id, "resumed": False,
                "reused_first_result": False, "result": result, "outcomes": outcomes}

    def get_batch(self, batch_id: str, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.repository.get_batch(batch_id)

    # ---------- adjudication ----------
    def list_pending(self, role: str) -> List[Dict[str, Any]]:
        ensure_role(role, ADJUDICATE_ROLES)
        pending = self.repository.list_pending()
        for item in pending:
            snapshot = self.repository.get_snapshot(item["snapshot_id"])
            item["snapshot"] = {k: snapshot["payload_obj"][k]
                                for k in SNAPSHOT_FIELDS
                                if k in snapshot["payload_obj"]}
            item["snapshot"]["snapshot_at"] = snapshot["payload_obj"]["snapshot_at"]
        return pending

    def adjudicate(self, pending_id: int, payload: Dict[str, Any],
                   actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, ADJUDICATE_ROLES)
        actor = require_text(actor, "actor", 100)
        decision = payload.get("decision")
        if decision not in ("accept", "reject"):
            raise ValidationError("decision必须是accept或reject")
        pending = self.repository.get_pending(pending_id)
        snapshot = self.repository.get_snapshot(pending["snapshot_id"])
        entry = snapshot["payload_obj"]
        self.repository.resolve_pending(pending_id, decision, actor)
        if decision == "reject":
            self.repository.append_audit("snapshot_reject", ENTITY, pending["item_id"],
                                         actor, {"pending_id": pending_id,
                                                 "snapshot_id": pending["snapshot_id"],
                                                 "external_ref": pending["external_ref"]})
            return {"pending_id": pending_id, "decision": "reject"}
        item = self.repository.get_item(pending["item_id"])
        # 仲裁采用新内容；确认数量仍然不能覆盖
        updated = self.repository.apply_snapshot_to_item(
            item["id"], entry, bool(item["quantity_confirmed"]))
        records = self._ingest_records(item["id"], entry, snapshot["batch_id"],
                                       snapshot["source"], actor)
        self.repository.mark_snapshot_adopted(pending["snapshot_id"])
        self.repository.append_audit("snapshot_accept", ENTITY, item["id"], actor, {
            "pending_id": pending_id, "snapshot_id": pending["snapshot_id"],
            "external_ref": pending["external_ref"],
            "quantity_kept": bool(item["quantity_confirmed"]),
            "records_added": records})
        return {"pending_id": pending_id, "decision": "accept",
                "item": self.enrich(updated)}

    def confirm_quantity(self, item_id: int, payload: Dict[str, Any],
                         actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, ADJUDICATE_ROLES)
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        expected = payload.get("expected_version")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
            raise ValidationError("expected_version必须是正整数")
        quantity = payload.get("quantity")
        if quantity is not None:
            quantity = require_number(quantity, "quantity")
        updated = self.repository.confirm_quantity(item_id, expected, quantity, actor)
        self.repository.append_audit("quantity_confirm", ENTITY, item_id, actor, {
            "previous_quantity": item["quantity"],
            "confirmed_quantity": updated["quantity"],
            "overwritten": quantity is not None})
        return self.enrich(updated)

    # ---------- monitoring & rewind ----------
    def monitoring_observation(self, item_id: int, payload: Dict[str, Any],
                               actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, MONITOR_ROLES)
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        severity = payload.get("severity")
        if severity is not None:
            severity = normalize_severity(severity)
        quantity = payload.get("quantity")
        if quantity is not None:
            quantity = require_number(quantity, "quantity")
        spill_active = payload.get("spill_active")
        if spill_active is not None and not isinstance(spill_active, bool):
            raise ValidationError("spill_active必须是布尔值")
        note = payload.get("note")
        if note is not None:
            note = require_text(note, "note")
        invalid, reasons, update_quantity = monitoring_invalidation(
            item, severity, quantity, spill_active)
        record = self.repository.add_record(
            item_id, "monitoring", note or "监测变化上报", "open",
            payload.get("external_ref"), actor, source="monitoring")
        if not invalid:
            self.repository.append_audit("monitoring_observe", ENTITY, item_id, actor, {
                "record_id": record["id"], "invalidated": False})
            return {"invalidated": False, "item": self.enrich(item),
                    "record_id": record["id"]}
        observed_quantity = quantity if update_quantity else None
        reason_payload = {"reasons": reasons,
                          "observed_severity": severity,
                          "observed_quantity": quantity,
                          "spill_active": spill_active,
                          "from_status": item["status"],
                          "record_id": record["id"]}
        updated = self.repository.rewind_item(item_id, reason_payload,
                                              observed_quantity, actor)
        self.repository.append_audit("monitoring_rewind", ENTITY, item_id, actor, {
            "record_id": record["id"], "invalidated": True, "reasons": reasons,
            "quantity_updated": observed_quantity is not None,
            "from_status": item["status"], "to_status": "review",
            "proposed_severity": severity, "proposed_quantity": quantity})
        return {"invalidated": True, "reasons": reasons,
                "rewound_to": "review", "item": self.enrich(updated),
                "record_id": record["id"]}

    def rewind(self, item_id: int, payload: Dict[str, Any],
               actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, role_for_transition("review"))
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        reason = require_text(payload.get("reason"), "reason")
        expected = payload.get("expected_version")
        if expected is not None and (isinstance(expected, bool)
                                     or not isinstance(expected, int) or expected < 1):
            raise ValidationError("expected_version必须是正整数")
        updated = self.repository.rewind_item(
            item_id, {"reasons": ["manual"], "reason": reason,
                      "from_status": item["status"]}, None, actor,
            expected_version=expected)
        self.repository.append_audit("rewind", ENTITY, item_id, actor, {
            "from_status": item["status"], "reason": reason})
        return self.enrich(updated)

    # ---------- records / transitions / drafts ----------
    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValidationError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor, source="manual")
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def _store_stale_draft(self, item_id: int, target: str,
                           expected_version: int, current_version: int,
                           actor: str) -> None:
        draft_id = self.repository.save_draft(
            item_id, target, expected_version, current_version, actor)
        self.repository.append_audit("transition_draft", ENTITY, item_id, actor, {
            "draft_id": draft_id, "target": target,
            "base_version": expected_version,
            "current_version": current_version})
        raise ConflictError(
            f"版本已过期，推进意图已保留为草稿#{draft_id}；草稿{draft_id}")

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        if target not in ALL_STATES:
            raise ValidationError("未知状态")
        ensure_role(role, role_for_transition(target))
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) \
                or expected_version < 1:
            raise ValidationError("expected_version必须是正整数")
        # 两人同时推进：版本过期者先保留草稿，只接受当前版本
        if item["version"] != expected_version:
            self._store_stale_draft(item_id, target, expected_version,
                                    item["version"], actor)
        validate_transition(item["status"], target)
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        try:
            updated = self.repository.transition_item(item_id, target, expected_version, actor)
        except ConflictError:
            current = self.repository.get_item(item_id)
            self._store_stale_draft(item_id, target, expected_version,
                                    current["version"], actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def list_drafts(self, item_id: Optional[int], actor: str, role: str) -> list:
        ensure_role(role, DRAFT_ROLES)
        del actor
        drafts = self.repository.list_drafts(item_id)
        for draft in drafts:
            draft["current"] = self.enrich(self.repository.get_item(draft["item_id"]))
        return drafts

    def apply_draft(self, draft_id: int, payload: Dict[str, Any],
                    actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, DRAFT_ROLES)
        actor = require_text(actor, "actor", 100)
        draft = self.repository.get_draft(draft_id)
        if draft["status"] != "stored":
            raise ConflictError("草稿已处理")
        item = self.repository.get_item(draft["item_id"])
        validate_transition(item["status"], draft["target"])
        ensure_role(role, role_for_transition(draft["target"]))
        expected = payload.get("expected_version", item["version"])
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
            raise ValidationError("expected_version必须是正整数")
        blockers = completion_blockers(draft["target"],
                                      self.repository.open_record_count(item["id"]))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(
            item["id"], draft["target"], expected, actor)
        self.repository.mark_draft(draft_id, "applied")
        self.repository.append_audit("draft_apply", ENTITY, item["id"], actor, {
            "draft_id": draft_id, "target": draft["target"],
            "from_version": expected})
        return self.enrich(updated)

    def discard_draft(self, draft_id: int, actor: str, role: str) -> dict:
        ensure_role(role, DRAFT_ROLES)
        actor = require_text(actor, "actor", 100)
        draft = self.repository.get_draft(draft_id)
        self.repository.mark_draft(draft_id, "discarded")
        self.repository.append_audit("draft_discard", ENTITY, draft["item_id"],
                                     actor, {"draft_id": draft_id})
        return {"draft_id": draft_id, "status": "discarded"}

    # ---------- reads ----------
    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        severity = item["severity"]
        # 复核中：原等级/期限结论已失效，按观察提议预演新期限并标记
        proposed = None
        if item.get("review_reason"):
            try:
                import json as _json
                reason = _json.loads(item["review_reason"])
                proposed = reason.get("observed_severity")
                if reason.get("observed_quantity") is not None:
                    result["proposed_quantity"] = reason["observed_quantity"]
                result["review_reasons"] = reason.get("reasons", [])
            except (ValueError, TypeError):
                pass
        result["priority"] = priority_score(
            severity, item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            severity, item["quantity"], item["threshold"])
        if proposed:
            result["proposed_severity"] = proposed
            result["proposed_deadline_hours"] = response_deadline_hours(
                proposed, item["quantity"], item["threshold"])
            result["conclusions_invalid"] = True
        else:
            result["conclusions_invalid"] = item["status"] == "review"
        result["escalation_required"] = escalation_required(
            severity, item["quantity"], item["threshold"])
        result["quantity_confirmed"] = bool(item.get("quantity_confirmed"))
        return result


