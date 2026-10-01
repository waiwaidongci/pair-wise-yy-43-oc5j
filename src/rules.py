from __future__ import annotations
import hashlib
import json
from datetime import datetime, timezone
from .domain import ConflictError, ValidationError
TITLE='溢油应急响应与任务追踪'; ENTITY='溢油事件'; ID_PREFIX='OS'
SEVERITIES=['minor', 'moderate', 'major', 'catastrophic']; STATES=['reported', 'assessing', 'containing', 'recovering', 'monitoring', 'closed']; REVIEW_STATE='review'; ALL_STATES=STATES+[REVIEW_STATE]; TRANSITIONS={'reported': ['assessing'], 'assessing': ['containing'], 'containing': ['recovering'], 'recovering': ['monitoring'], 'monitoring': ['closed', 'review'], 'review': ['assessing'], 'closed': ['review']}; TRANSITION_ROLES={'assessing': ['response_commander'], 'containing': ['response_commander'], 'recovering': ['operations'], 'monitoring': ['operations'], 'closed': ['response_commander'], 'review': ['response_commander', 'operations']}
CREATE_ROLES=set(['observer', 'response_commander']); RECORD_ROLES=set(['response_commander', 'operations']); AUDIT_ROLES=set(['response_commander', 'viewer']); VIEW_ROLES=set(['observer', 'response_commander', 'operations', 'viewer'])
INGEST_ROLES=set(['observer', 'response_commander']); ADJUDICATE_ROLES=set(['response_commander']); MONITOR_ROLES=set(['response_commander', 'operations']); DRAFT_ROLES=set(['response_commander', 'operations'])
SEVERITY_WEIGHT={'minor': 1.0, 'moderate': 3.0, 'major': 6.0, 'catastrophic': 9.0}; DEADLINE_HOURS={'minor': 72, 'moderate': 24, 'major': 8, 'catastrophic': 4}; TERMINAL_STATES=set(['closed']); REVIEW_STATES=set(['review'])
HISTORICAL_BATCH_ID='HISTORICAL-BASELINE'; SOURCES=set(['vessel', 'drone', 'shore', 'manual', 'legacy']); SNAPSHOT_FIELDS=('title', 'description', 'severity', 'quantity', 'threshold')
def priority_score(severity,quantity=0.0,threshold=1.0,open_records=0):
    if severity not in SEVERITY_WEIGHT: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(0,min(10,int(round(SEVERITY_WEIGHT[severity]+min(4.0,ratio*4.0)+min(3.0,float(open_records))))))
def response_deadline_hours(severity,quantity=0.0,threshold=1.0):
    if severity not in DEADLINE_HOURS: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(1,int(DEADLINE_HOURS[severity]/max(1.0,ratio)))
def escalation_required(severity,quantity=0.0,threshold=1.0):
    return severity==SEVERITIES[-1] or (threshold>0 and quantity>=threshold)
def can_transition(current,target): return target in TRANSITIONS.get(current,[])
def validate_transition(current,target):
    if current not in ALL_STATES or target not in ALL_STATES: raise ValidationError("未知状态")
    if not can_transition(current,target): raise ConflictError(f"不能从{current}转换到{target}")
def completion_blockers(target,open_records): return ["仍有未关闭事项"] if target in TERMINAL_STATES and open_records>0 else []
def role_for_transition(target): return set(TRANSITION_ROLES.get(target,[]))
def parse_utc(value):
    if not isinstance(value,str) or not value.strip(): raise ValidationError("时间戳不能为空")
    text=value.strip().replace('Z','+00:00')
    try: parsed=datetime.fromisoformat(text)
    except ValueError as exc: raise ValidationError("时间戳必须是ISO-8601 UTC格式") from exc
    if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()
def snapshot_canonical(entry):
    """快照业务指纹：相同指纹视为同一份快照的重复上报，与来源/批次无关。"""
    payload={
        "external_ref": entry["external_ref"],
        "title": entry["title"], "description": entry["description"],
        "severity": entry["severity"],
        "quantity": round(float(entry["quantity"]),6),
        "threshold": round(float(entry["threshold"]),6),
        "records": [
            {"kind": str(r.get("kind","")).strip(),
             "detail": str(r.get("detail","")).strip(),
             "status": r.get("status","open"),
             "external_ref": (r.get("external_ref") or None)}
            for r in entry.get("records",[])
        ],
    }
    raw=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
def chunk_hash(entries):
    """分片内容指纹：规范化条目后哈希，重传校验与首次结果保持一致。"""
    raw=json.dumps(entries,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
def monitoring_invalidation(item,observed_severity=None,observed_quantity=None,spill_active=None):
    """监测结论：等级/期限参数或关闭结论发生变化时，事件退回复核。返回(是否失效, 原因, 是否允许更新数量)。"""
    reasons=[]; update_quantity=False
    if observed_severity is not None and observed_severity!=item["severity"]:
        reasons.append("severity")
    if observed_quantity is not None and float(observed_quantity)!=float(item["quantity"]):
        if item.get("quantity_confirmed"):
            reasons.append("quantity_confirmed")
        else:
            update_quantity=True
            reasons.append("quantity")
    if item["status"]=="closed" and spill_active is True:
        reasons.append("closure")
    return (bool(reasons),reasons,update_quantity)
