import tempfile, unittest
from pathlib import Path
from src.domain import ConflictError
from src.repository import Repository
from src.service import Service
from src.rules import STATES, TRANSITION_ROLES


class IntakeBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    # ---- 批次分片入库，事件/记录/审计按批次号关联 ----
    def test_shard_creates_linked_items_records_audit(self):
        self.service.open_intake_batch(
            {"batch_no": "B-001", "source": "ship"}, "captain", "observer")
        shard = self.service.add_intake_shard("B-001", {
            "shard_no": 0,
            "events": [
                {"external_ref": "OS-1", "title": "溢油A", "description": "近岸",
                 "severity": "major", "quantity": 5, "threshold": 10,
                 "records": [{"kind": "evidence", "detail": "现场照片",
                              "status": "closed", "external_ref": "EV-1"}]},
                {"external_ref": "OS-2", "title": "溢油B", "description": "远岸",
                 "severity": "minor", "quantity": 1, "threshold": 10},
            ],
        }, "captain", "observer")
        self.assertFalse(shard["idempotent_replay"])
        self.assertEqual(len(shard["shard"]["result"]["created"]), 2)
        self.service.commit_intake_batch("B-001", "captain", "observer")
        batch = self.service.get_intake_batch("B-001", "viewer")
        self.assertEqual(batch["status"], "committed")
        self.assertEqual(batch["shard_count"], 1)
        # 事件带批次号
        item1 = self.repo.find_item_by_external_ref("OS-1")
        self.assertEqual(item1["batch_no"], "B-001")
        # 记录带批次号
        recs = self.service.list_records(item1["id"], "viewer")
        self.assertEqual(recs[0]["batch_no"], "B-001")
        # 审计链完整且含批次
        self.assertTrue(self.repo.verify_audit_chain())
        actions = [e["action"] for e in self.service.audit("viewer")]
        self.assertIn("intake", actions)
        self.assertIn("batch_committed", actions)

    # ---- 重试沿用首次结果 ----
    def test_shard_retry_returns_first_result(self):
        self.service.open_intake_batch(
            {"batch_no": "B-002", "source": "drone"}, "pilot", "observer")
        body = {"shard_no": 0, "events": [
            {"external_ref": "OS-3", "title": "重复上报", "description": "首报",
             "severity": "moderate", "quantity": 2, "threshold": 10}]}
        first = self.service.add_intake_shard("B-002", body, "pilot", "observer")
        retry = self.service.add_intake_shard("B-002", body, "pilot", "observer")
        self.assertTrue(retry["idempotent_replay"])
        self.assertEqual(retry["shard"]["result"], first["shard"]["result"])
        # 只创建一条事件
        item = self.repo.find_item_by_external_ref("OS-3")
        self.assertIsNotNone(item)
        self.assertEqual(item["title"], "重复上报")

    # ---- 中断后按原批次号续传 ----
    def test_resume_interrupted_batch(self):
        self.service.open_intake_batch(
            {"batch_no": "B-003", "source": "shore_station"}, "station", "observer")
        self.service.add_intake_shard("B-003", {"shard_no": 0, "events": [
            {"external_ref": "OS-4", "title": "分片0", "description": "d",
             "severity": "minor", "quantity": 1, "threshold": 10}]}, "station", "observer")
        # 模拟中断：不提交，直接用原批次号续传分片1
        self.service.add_intake_shard("B-003", {"shard_no": 1, "events": [
            {"external_ref": "OS-5", "title": "分片1", "description": "d",
             "severity": "minor", "quantity": 1, "threshold": 10}]}, "station", "observer")
        shards = self.repo.list_intake_shards("B-003")
        self.assertEqual([s["shard_no"] for s in shards], [0, 1])
        self.service.commit_intake_batch("B-003", "station", "observer")
        # 重复提交幂等
        again = self.service.commit_intake_batch("B-003", "station", "observer")
        self.assertEqual(again["status"], "committed")

    # ---- 同编号只采最早快照 ----
    def test_duplicate_same_number_keeps_earliest_snapshot(self):
        self.service.open_intake_batch(
            {"batch_no": "B-004", "source": "ship"}, "captain", "observer")
        ev = {"external_ref": "OS-6", "title": "最早快照", "description": "d",
              "severity": "minor", "quantity": 1, "threshold": 10}
        self.service.add_intake_shard("B-004", {"shard_no": 0, "events": [ev]},
                                      "captain", "observer")
        # 断网补传夹带同编号旧版本，内容一致
        res = self.service.add_intake_shard("B-004", {"shard_no": 1, "events": [
            dict(ev, title="最早快照")]}, "captain", "observer")
        self.assertEqual(len(res["shard"]["result"]["duplicates"]), 1)
        self.assertEqual(len(res["shard"]["result"]["adjudications"]), 0)
        # 仍只有一条事件，且标题不变
        item = self.repo.find_item_by_external_ref("OS-6")
        self.assertEqual(item["title"], "最早快照")

    # ---- 内容不同留待裁 ----
    def test_content_differs_held_for_adjudication(self):
        self.service.open_intake_batch(
            {"batch_no": "B-005", "source": "drone"}, "pilot", "observer")
        self.service.add_intake_shard("B-005", {"shard_no": 0, "events": [
            {"external_ref": "OS-7", "title": "初报", "description": "d",
             "severity": "minor", "quantity": 1, "threshold": 10}]}, "pilot", "observer")
        res = self.service.add_intake_shard("B-005", {"shard_no": 1, "events": [
            {"external_ref": "OS-7", "title": "改级", "description": "d",
             "severity": "major", "quantity": 1, "threshold": 10}]}, "pilot", "observer")
        self.assertEqual(len(res["shard"]["result"]["adjudications"]), 1)
        case = res["shard"]["result"]["adjudications"][0]
        self.assertEqual(case["reason"], "content_conflict")
        # 事件未被覆盖
        item = self.repo.find_item_by_external_ref("OS-7")
        self.assertEqual(item["severity"], "minor")
        self.assertEqual(item["title"], "初报")
        # 裁决案例可查
        cases = self.service.list_adjudication("viewer", status="pending")
        self.assertEqual(len(cases), 1)

    # ---- 不能覆盖确认数量 ----
    def test_confirmed_quantity_not_overwritten(self):
        self.service.open_intake_batch(
            {"batch_no": "B-006", "source": "ship"}, "captain", "observer")
        self.service.add_intake_shard("B-006", {"shard_no": 0, "events": [
            {"external_ref": "OS-8", "title": "油量确认", "description": "d",
             "severity": "major", "quantity": 5, "threshold": 10}]}, "captain", "observer")
        item = self.repo.find_item_by_external_ref("OS-8")
        # 推进到 containing，确认数量
        item = self._drive_to(item, "containing")
        self.assertEqual(item["status"], "containing")
        # 另一批次同编号报不同油量
        self.service.open_intake_batch(
            {"batch_no": "B-007", "source": "drone"}, "pilot", "observer")
        res = self.service.add_intake_shard("B-007", {"shard_no": 0, "events": [
            {"external_ref": "OS-8", "title": "油量确认", "description": "d",
             "severity": "major", "quantity": 9, "threshold": 10}]}, "pilot", "observer")
        case = res["shard"]["result"]["adjudications"][0]
        self.assertEqual(case["reason"], "confirmed_quantity_conflict")
        item = self.repo.find_item_by_external_ref("OS-8")
        self.assertEqual(item["quantity"], 5)

    # ---- 旧事件缺批次号升级为历史基线 ----
    def test_backfill_baseline(self):
        # 直接创建的事件没有批次号
        item = self.service.create_item(
            {"title": "旧事件", "description": "legacy", "severity": "minor",
             "quantity": 1, "threshold": 10}, "old", "observer")
        self.assertIsNone(item["batch_no"])
        out = self.service.backfill_baseline("commander", "response_commander")
        self.assertEqual(out["upgraded"], 1)
        refreshed = self.service.get_item(item["id"], "viewer")
        self.assertEqual(refreshed["batch_no"], "BASELINE-LEGACY")
        # 幂等：再次执行不再升级
        again = self.service.backfill_baseline("commander", "response_commander")
        self.assertEqual(again["upgraded"], 0)
        self.assertTrue(self.repo.verify_audit_chain())

    # ---- 监测变化让等级/期限/关闭失效，事件退回复核 ----
    def test_monitoring_escalation_reverts_to_review(self):
        self.service.open_intake_batch(
            {"batch_no": "B-008", "source": "ship"}, "captain", "observer")
        self.service.add_intake_shard("B-008", {"shard_no": 0, "events": [
            {"external_ref": "OS-9", "title": "监测升级", "description": "d",
             "severity": "minor", "quantity": 1, "threshold": 10}]}, "captain", "observer")
        item = self.repo.find_item_by_external_ref("OS-9")
        item = self._drive_to(item, "monitoring")
        self.assertEqual(item["status"], "monitoring")
        version_before = item["version"]
        # 新监测数据：油量远超阈值，等级/期限/关闭结论失效
        self.service.add_record(item["id"], {
            "kind": "monitoring", "detail": "复测油量", "status": "closed",
            "quantity": 60, "external_ref": "MON-1"}, "recorder", "operations")
        reverted = self.service.get_item(item["id"], "viewer")
        self.assertEqual(reverted["status"], "assessing")
        self.assertGreater(reverted["version"], version_before)
        actions = [e["action"] for e in self.service.audit("viewer", item["id"])]
        self.assertIn("revert_to_review", actions)

    # ---- 两人同时推进：只接受当前版本，后到者保留草稿 ----
    def test_concurrent_transition_keeps_draft(self):
        item = self.service.create_item(
            {"title": "并发", "description": "d", "severity": "minor",
             "quantity": 1, "threshold": 10}, "creator", "observer")
        # A 先推进到 assessing（版本 1 -> 2）
        self.service.transition(item["id"], "assessing", item["version"],
                                "reviewer", TRANSITION_ROLES["assessing"][0])
        # B 停留在旧视图，用版本 1 直接推进到 containing -> 版本冲突，保留草稿
        with self.assertRaises(ConflictError):
            self.service.transition(item["id"], "containing", 1,
                                     "reviewer", TRANSITION_ROLES["containing"][0])
        drafts = self.service.list_drafts(item["id"], "viewer")
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["target"], "containing")
        self.assertEqual(drafts[0]["reason"], "version_conflict")
        # B 在草稿上继续：当前状态 assessing -> containing 合法，草稿生效
        applied = self.service.apply_draft(
            item["id"], drafts[0]["id"], "reviewer",
            TRANSITION_ROLES["containing"][0])
        self.assertEqual(applied["status"], "containing")
        # 草稿已处理，不再挂起
        self.assertEqual(self.service.list_drafts(item["id"], "viewer"), [])
        self.assertTrue(self.repo.verify_audit_chain())

    # ---- 提交后不能再分片 ----
    def test_committed_batch_rejects_more_shards(self):
        self.service.open_intake_batch(
            {"batch_no": "B-009", "source": "ship"}, "captain", "observer")
        self.service.add_intake_shard("B-009", {"shard_no": 0, "events": [
            {"external_ref": "OS-10", "title": "t", "description": "d",
             "severity": "minor", "quantity": 1, "threshold": 10}]}, "captain", "observer")
        self.service.commit_intake_batch("B-009", "captain", "observer")
        with self.assertRaises(ConflictError):
            self.service.add_intake_shard("B-009", {"shard_no": 1, "events": []},
                                          "captain", "observer")

    def _drive_to(self, item, target):
        current = item
        start = STATES.index(current["status"])
        end = STATES.index(target)
        for t in STATES[start + 1:end + 1]:
            if t == "closed":
                recs = self.service.list_records(current["id"], "viewer")
                if not any(r["status"] == "closed" for r in recs):
                    self.service.add_record(
                        current["id"],
                        {"kind": "evidence", "detail": "收尾", "status": "closed",
                         "external_ref": f"EV-{current['id']}"},
                        "recorder", "response_commander")
            current = self.service.transition(
                current["id"], t, current["version"], "reviewer",
                TRANSITION_ROLES[t][0])
        return current


if __name__ == "__main__":
    unittest.main()
