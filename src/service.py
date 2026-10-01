from __future__ import annotations

from typing import Any, Dict, List, Optional

from .domain import (ConflictError, NotFoundError, ValidationError, ensure_role,
                     normalize_severity, require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, BASELINE_BATCH_NO, CREATE_ROLES, ENTITY,
                    INTAKE_SOURCES, RECORD_ROLES, TITLE, VIEW_ROLES,
                    completion_blockers, escalation_required, priority_score,
                    response_deadline_hours, role_for_transition, validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

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
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        batch_no = payload.get("batch_no")
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor, batch_no=batch_no)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        # 监测变化让等级、期限或关闭结论失效时，事件退回复核
        item = self.repository.get_item(item_id)
        measured = payload.get("quantity")
        if kind == "monitoring" and measured is not None:
            measured = require_number(measured, "quantity")
            if escalation_required(item["severity"], measured, item["threshold"]):
                if item["status"] in ("containing", "recovering", "monitoring", "closed"):
                    self.repository.revert_item_to_review(item_id)
                    self.repository.append_audit("revert_to_review", ENTITY, item_id, actor, {
                        "reason": "monitoring_escalation",
                        "measured_quantity": measured,
                        "from_status": item["status"],
                    })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        if item["version"] != expected_version:
            # 两人同时推进：只接受当前版本，后到者保留草稿
            draft = self.repository.create_draft(
                item_id, target, expected_version,
                {"target": target, "expected_version": expected_version},
                "version_conflict", actor)
            self.repository.append_audit("draft_saved", ENTITY, item_id, actor, {
                "draft_id": draft["id"], "target": target,
                "expected_version": expected_version,
            })
            raise ConflictError(f"版本冲突，已保留草稿#{draft['id']}，请刷新后在草稿上继续")
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        if target == "containing":
            # 进入围控即确认估算数量，后续上报不得覆盖
            self.repository.set_quantity_confirmed(item_id)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

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

    # ---- 收件批次（船载/无人机/岸站重复上报、断网补传） ----
    def open_intake_batch(self, payload: Dict[str, Any], actor: str,
                          role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        batch_no = require_text(payload.get("batch_no"), "batch_no", 100)
        source = require_text(payload.get("source"), "source", 50)
        if source not in INTAKE_SOURCES:
            raise ValidationError("source必须是ship/drone/shore_station")
        batch = self.repository.create_intake_batch(batch_no, source, actor)
        self.repository.append_audit("batch_open", "intake_batch", batch["id"], actor, {
            "batch_no": batch_no, "source": source,
        })
        return batch

    def add_intake_shard(self, batch_no: str, payload: Dict[str, Any], actor: str,
                         role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        batch = self.repository.get_intake_batch(batch_no)
        if batch is None:
            raise NotFoundError("批次不存在")
        if batch["status"] != "open":
            raise ConflictError("批次已提交，不能继续分片")
        shard_no = payload.get("shard_no")
        if not isinstance(shard_no, int) or shard_no < 0:
            raise ValidationError("shard_no必须是非负整数")
        events = payload.get("events", [])
        if not isinstance(events, list):
            raise ValidationError("events必须是数组")
        # 重试沿用首次结果：幂等分片直接回放
        existing = self.repository.get_intake_shard(f"{batch_no}#{shard_no}")
        if existing is not None:
            return {"shard": existing, "idempotent_replay": True}
        result = self._apply_shard(batch_no, shard_no, events, actor)
        shard, _created = self.repository.add_intake_shard(batch_no, shard_no, payload, result)
        self.repository.append_audit("shard_received", "intake_batch", batch["id"], actor, {
            "batch_no": batch_no, "shard_no": shard_no, "events": len(events),
            "created": len(result["created"]), "duplicates": len(result["duplicates"]),
            "adjudications": len(result["adjudications"]),
        })
        return {"shard": shard, "idempotent_replay": False}

    def _apply_shard(self, batch_no: str, shard_no: int, events: List[Dict[str, Any]],
                     actor: str) -> Dict[str, Any]:
        created: List[Dict[str, Any]] = []
        duplicates: List[Dict[str, Any]] = []
        adjudications: List[Dict[str, Any]] = []
        for ev in events:
            external_ref = ev.get("external_ref")
            if external_ref is not None:
                external_ref = require_text(external_ref, "external_ref", 100)
            title = require_text(ev.get("title"), "title", 200)
            description = require_text(ev.get("description"), "description")
            severity = normalize_severity(ev.get("severity"))
            quantity = require_number(ev.get("quantity", 0), "quantity")
            threshold = require_number(ev.get("threshold", 1), "threshold", 0.000001)
            records = ev.get("records", [])
            if not isinstance(records, list):
                raise ValidationError("records必须是数组")
            existing = None
            if external_ref is not None:
                existing = self.repository.find_item_by_external_ref(external_ref)
            if existing is not None:
                if self._same_snapshot(existing, title, description, severity,
                                      quantity, threshold):
                    # 同编号只采最早快照
                    duplicates.append({
                        "external_ref": external_ref, "item_id": existing["id"],
                        "result": "earliest_snapshot_kept",
                    })
                    continue
                # 内容不同留待裁；已确认数量不得覆盖
                if existing["quantity_confirmed"] and \
                        abs(existing["quantity"] - quantity) > 1e-9:
                    reason = "confirmed_quantity_conflict"
                else:
                    reason = "content_conflict"
                case = self.repository.create_adjudication_case(
                    batch_no, shard_no, external_ref, existing["id"], reason, ev, actor)
                adjudications.append({
                    "external_ref": external_ref, "case_id": case["id"], "reason": reason,
                })
                continue
            item = self.repository.create_item(
                title, description, severity, quantity, threshold, external_ref, actor,
                batch_no=batch_no)
            for rec in records:
                kind = require_text(rec.get("kind"), "kind", 100)
                detail = require_text(rec.get("detail"), "detail")
                status = rec.get("status", "open")
                rec_ref = rec.get("external_ref")
                if rec_ref is not None:
                    rec_ref = require_text(rec_ref, "external_ref", 100)
                self.repository.add_record(item["id"], kind, detail, status,
                                           rec_ref, actor, batch_no=batch_no)
            self.repository.append_audit("intake", ENTITY, item["id"], actor, {
                "batch_no": batch_no, "shard_no": shard_no,
                "external_ref": external_ref, "severity": severity,
                "quantity": quantity,
            })
            created.append({"external_ref": external_ref, "item_id": item["id"]})
        return {"created": created, "duplicates": duplicates,
                "adjudications": adjudications}

    @staticmethod
    def _same_snapshot(existing: Dict[str, Any], title: str, description: str,
                       severity: str, quantity: float, threshold: float) -> bool:
        return (existing["title"] == title
                and existing["description"] == description
                and existing["severity"] == severity
                and abs(existing["quantity"] - quantity) < 1e-9
                and abs(existing["threshold"] - threshold) < 1e-9)

    def commit_intake_batch(self, batch_no: str, actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        batch = self.repository.get_intake_batch(batch_no)
        if batch is None:
            raise NotFoundError("批次不存在")
        was_open = batch["status"] == "open"
        committed = self.repository.commit_intake_batch(batch_no)
        if was_open:
            self.repository.append_audit("batch_committed", "intake_batch",
                                         batch["id"], actor, {
                "batch_no": batch_no, "shards": committed["shard_count"],
            })
        return committed

    def get_intake_batch(self, batch_no: str, role: str) -> Dict[str, Any]:
        self._view(role)
        batch = self.repository.get_intake_batch(batch_no)
        if batch is None:
            raise NotFoundError("批次不存在")
        return batch

    def backfill_baseline(self, actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, set(["response_commander"]))
        actor = require_text(actor, "actor", 100)
        count = self.repository.backfill_baseline(BASELINE_BATCH_NO, actor)
        if count > 0:
            self.repository.append_audit("baseline_upgraded", "intake_batch", 0, actor, {
                "batch_no": BASELINE_BATCH_NO, "items": count,
            })
        return {"batch_no": BASELINE_BATCH_NO, "upgraded": count}

    def list_adjudication(self, role: str, status: Optional[str] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_adjudication_cases(status)

    def list_drafts(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_drafts(item_id, status="draft")

    def apply_draft(self, item_id: int, draft_id: int, actor: str,
                    role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        draft = self.repository.get_draft(draft_id)
        if draft is None or draft["item_id"] != item_id:
            raise NotFoundError("草稿不存在")
        if draft["status"] != "draft":
            raise ConflictError("草稿已处理")
        item = self.repository.get_item(item_id)
        target = draft["target"]
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, item["version"], actor)
        self.repository.mark_draft_applied(draft_id)
        if target == "containing":
            self.repository.set_quantity_confirmed(item_id)
        self.repository.append_audit("draft_applied", ENTITY, item_id, actor, {
            "draft_id": draft_id, "to": target,
        })
        return self.enrich(updated)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
