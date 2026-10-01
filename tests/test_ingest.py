import json
import tempfile
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from src.domain import ConflictError
from src.http_api import make_handler
from src.repository import Repository
from src.rules import (HISTORICAL_BATCH_ID, REVIEW_STATE, STATES,
                       TRANSITION_ROLES)
from src.service import Service


def entry(ref, snapshot_at, **overrides):
    data = {
        "external_ref": ref,
        "snapshot_at": snapshot_at,
        "title": f"spill {ref}",
        "description": "reported slick",
        "severity": "moderate",
        "quantity": 5.0,
        "threshold": 10.0,
        "records": [],
    }
    data.update(overrides)
    return data


def open_upload_complete(service, batch_id, source, chunks, actor="vessel-1",
                         role="observer"):
    service.open_batch({"batch_id": batch_id, "source": source,
                        "total_chunks": len(chunks)}, actor, role)
    for index, entries in enumerate(chunks):
        service.upload_chunk(batch_id, index, {"entries": entries}, actor, role)
    return service.complete_batch(batch_id, actor, role)


class IngestTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def test_chunked_resume_and_idempotent_completion(self):
        self.service.open_batch({"batch_id": "B-1", "source": "vessel",
                                 "total_chunks": 2}, "vessel-1", "observer")
        r0 = self.service.upload_chunk(
            "B-1", 0, {"entries": [entry("OS-1", "2026-09-30T10:00:00Z")]},
            "vessel-1", "observer")
        self.assertFalse(r0["resubmitted"])
        self.assertEqual(r0["received_chunks"], [0])
        # 中断后续传：同批次号、同分片号重发 -> 幂等
        r0_again = self.service.upload_chunk(
            "B-1", 0, {"entries": [entry("OS-1", "2026-09-30T10:00:00Z")]},
            "vessel-1", "observer")
        self.assertTrue(r0_again["resubmitted"])
        self.service.upload_chunk(
            "B-1", 1, {"entries": [entry("OS-2", "2026-09-30T10:05:00Z",
                                          severity="minor")]},
            "vessel-1", "observer")
        first = self.service.complete_batch("B-1", "vessel-1", "observer")
        self.assertEqual(first["result"], {"adopted": 2, "pending": 0,
                                           "duplicate": 0, "total": 2})
        # 重试完成沿用首次结果
        second = self.service.complete_batch("B-1", "vessel-1", "observer")
        self.assertTrue(second["reused_first_result"])
        self.assertEqual(second["result"], first["result"])
        batch = self.service.get_batch("B-1", "viewer")
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(len(batch["received_chunks"]), 2)

    def test_resume_blocked_until_all_chunks_and_hash_mismatch_rejected(self):
        self.service.open_batch({"batch_id": "B-X", "source": "drone",
                                 "total_chunks": 2}, "drone-1", "observer")
        self.service.upload_chunk(
            "B-X", 0, {"entries": [entry("OS-9", "2026-09-30T10:00:00Z")]},
            "drone-1", "observer")
        with self.assertRaises(ConflictError):
            self.service.complete_batch("B-X", "drone-1", "observer")
        with self.assertRaises(ConflictError):
            self.service.upload_chunk(
                "B-X", 0, {"entries": [entry("OS-9", "2026-09-30T10:00:00Z",
                                              quantity=9.0)]},
                "drone-1", "observer")

    def test_duplicate_reports_and_earliest_snapshot_wins(self):
        # 船载、无人机、岸站对同一编号重复上报：同内容=重复；晚到旧版本不覆盖
        open_upload_complete(self.service, "B-A", "vessel",
                             [[entry("OS-100", "2026-09-30T11:00:00Z",
                                     title="vessel title")]])
        open_upload_complete(self.service, "B-B", "drone",
                             [[entry("OS-100", "2026-09-30T12:00:00Z",
                                     title="vessel title")]])
        open_upload_complete(self.service, "B-C", "shore",
                             [[entry("OS-100", "2026-09-30T12:30:00Z",
                                     title="shore title", quantity=7.0)]])
        item = self.repo.get_item_by_ref("OS-100")
        self.assertEqual(item["title"], "vessel title")
        pending = self.service.list_pending("response_commander")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["external_ref"], "OS-100")
        self.assertEqual(pending[0]["reason"], "content_differs")
        # 断网补传夹带更旧版本：时间戳更早也不能直接覆盖，留待裁
        open_upload_complete(self.service, "B-D", "vessel",
                             [[entry("OS-100", "2026-09-30T08:00:00Z",
                                     title="older offline title")]])
        self.assertEqual(len(self.service.list_pending("response_commander")), 2)

    def test_adjudication_accept_and_reject(self):
        open_upload_complete(self.service, "B-E", "vessel",
                             [[entry("OS-200", "2026-09-30T11:00:00Z")]])
        open_upload_complete(self.service, "B-F", "shore",
                             [[entry("OS-200", "2026-09-30T13:00:00Z",
                                     title="new shore title", severity="major")]])
        pending = self.service.list_pending("response_commander")[0]
        rejected = self.service.adjudicate(
            pending["id"], {"decision": "reject"}, "cmd-1", "response_commander")
        self.assertEqual(rejected["decision"], "reject")
        with self.assertRaises(ConflictError):
            self.service.adjudicate(
                pending["id"], {"decision": "accept"}, "cmd-1",
                "response_commander")
        open_upload_complete(self.service, "B-G", "shore",
                             [[entry("OS-200", "2026-09-30T14:00:00Z",
                                     title="accepted title", severity="major")]])
        pending2 = self.service.list_pending("response_commander")[0]
        accepted = self.service.adjudicate(
            pending2["id"], {"decision": "accept"}, "cmd-1",
            "response_commander")
        self.assertEqual(accepted["item"]["title"], "accepted title")
        self.assertEqual(accepted["item"]["severity"], "major")

    def test_confirmed_quantity_never_overwritten(self):
        open_upload_complete(self.service, "B-H", "vessel",
                             [[entry("OS-300", "2026-09-30T11:00:00Z",
                                     quantity=5.0)]])
        item = self.repo.get_item_by_ref("OS-300")
        self.service.confirm_quantity(
            item["id"], {"expected_version": item["version"], "quantity": 42.0},
            "cmd-1", "response_commander")
        # 新快照数量不同 -> 单独标记 quantity_confirmed 留待裁
        open_upload_complete(self.service, "B-I", "drone",
                             [[entry("OS-300", "2026-09-30T15:00:00Z",
                                     title="same title", quantity=99.0)]])
        item = self.repo.get_item_by_ref("OS-300")
        self.assertEqual(item["quantity"], 42.0)
        pending = self.service.list_pending("response_commander")
        self.assertEqual(pending[0]["reason"], "quantity_confirmed")
        # 仲裁接受仍不得覆盖已确认数量
        accepted = self.service.adjudicate(
            pending[0]["id"], {"decision": "accept"}, "cmd-1",
            "response_commander")
        self.assertEqual(accepted["item"]["quantity"], 42.0)
        self.assertTrue(accepted["item"]["quantity_confirmed"])
        # 再次确认数量需匹配当前版本，防止并发覆盖
        with self.assertRaises(ConflictError):
            self.service.confirm_quantity(
                item["id"], {"expected_version": 1, "quantity": 1.0},
                "cmd-1", "response_commander")

    def test_monitoring_rewind_invalidates_level_deadline_and_closure(self):
        result = open_upload_complete(self.service, "B-J", "vessel",
                                      [[entry("OS-400", "2026-09-30T11:00:00Z",
                                              severity="minor", quantity=1.0)]])
        item_id = [o["item_id"] for o in result["outcomes"] if "item_id" in o][0]
        # 走完主链并关闭（每步推进前关闭阻断记录）
        current = self.service.get_item(item_id, "viewer")
        for target in STATES[1:]:
            self.repo.conn.execute(
                "UPDATE records SET status='closed' WHERE item_id=? AND status='open'",
                (item_id,))
            self.repo.conn.commit()
            current = self.service.transition(
                current["id"], target, current["version"], "r",
                TRANSITION_ROLES[target][0])
        self.assertEqual(current["status"], "closed")
        # 关闭后监测仍有溢油活动 -> 关闭结论失效，退回复核
        outcome = self.service.monitoring_observation(
            item_id, {"spill_active": True, "severity": "major",
                      "external_ref": "MON-1"},
            "ops-1", "operations")
        self.assertTrue(outcome["invalidated"])
        self.assertEqual(outcome["rewound_to"], REVIEW_STATE)
        self.assertIn("closure", outcome["reasons"])
        item = self.service.get_item(item_id, "viewer")
        self.assertEqual(item["status"], REVIEW_STATE)
        self.assertTrue(item["conclusions_invalid"])
        self.assertEqual(item["proposed_severity"], "major")
        self.assertTrue(self.repo.verify_audit_chain())

    def test_monitoring_quantity_change_rewinds_but_confirmed_blocks(self):
        result = open_upload_complete(self.service, "B-K", "drone",
                                      [[entry("OS-500", "2026-09-30T11:00:00Z",
                                              quantity=3.0)]])
        item_id = result["outcomes"][0]["item_id"]
        outcome = self.service.monitoring_observation(
            item_id, {"quantity": 8.0, "external_ref": "MON-2",
                      "note": "slick expanded"},
            "ops-1", "operations")
        self.assertTrue(outcome["invalidated"])
        self.assertEqual(self.repo.get_item(item_id)["quantity"], 8.0)
        # 复核返回 assessing 重新走流程
        item = self.service.get_item(item_id, "viewer")
        moved = self.service.transition(
            item_id, "assessing", item["version"], "cmd-1",
            "response_commander")
        self.assertEqual(moved["status"], "assessing")
        # 数量已确认后，监测数量不能再改，只记录分歧并退回复核
        self.service.confirm_quantity(
            item_id, {"expected_version": moved["version"], "quantity": 8.0},
            "cmd-1", "response_commander")
        outcome2 = self.service.monitoring_observation(
            item_id, {"quantity": 55.0, "external_ref": "MON-3"},
            "ops-1", "operations")
        self.assertTrue(outcome2["invalidated"])
        self.assertIn("quantity_confirmed", outcome2["reasons"])
        self.assertEqual(self.repo.get_item(item_id)["quantity"], 8.0)

    def test_concurrent_transition_keeps_loser_as_draft(self):
        result = open_upload_complete(self.service, "B-L", "vessel",
                                      [[entry("OS-600", "2026-09-30T11:00:00Z")]])
        item_id = result["outcomes"][0]["item_id"]
        first = self.service.transition(
            item_id, "assessing", 1, "cmd-A", "response_commander")
        self.assertEqual(first["version"], 2)
        with self.assertRaises(ConflictError):
            self.service.transition(
                item_id, "assessing", 1, "cmd-B", "response_commander")
        # 错误信息携带草稿号；草稿可查
        drafts = self.service.list_drafts(item_id, "cmd-B", "response_commander")
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["target"], "assessing")
        self.assertEqual(drafts[0]["base_version"], 1)
        # 草稿针对的推进已被他人完成 -> 可放弃
        self.service.discard_draft(drafts[0]["id"], "cmd-B", "response_commander")
        self.assertEqual(self.service.list_drafts(item_id, "cmd-B",
                                                  "response_commander"), [])

    def test_draft_can_be_applied_against_current_version(self):
        # cmd-A 推进 reported->assessing；cmd-B 基于旧版想推进 -> 草稿
        result = open_upload_complete(self.service, "B-M", "vessel",
                                      [[entry("OS-700", "2026-09-30T11:00:00Z")]])
        item_id = result["outcomes"][0]["item_id"]
        self.service.transition(item_id, "assessing", 1, "cmd-A",
                                "response_commander")
        with self.assertRaises(ConflictError):
            self.service.transition(item_id, "containing", 1, "cmd-B",
                                    "response_commander")
        draft = self.service.list_drafts(item_id, "cmd-B",
                                         "response_commander")[0]
        self.assertEqual(draft["target"], "containing")
        # 按当前版本应用草稿
        applied = self.service.apply_draft(
            draft["id"], {"expected_version": draft["current_version"]},
            "cmd-B", "response_commander")
        self.assertEqual(applied["status"], "containing")
        with self.assertRaises(ConflictError):
            self.service.apply_draft(draft["id"], {}, "cmd-B",
                                     "response_commander")


class HistoricalBaselineTest(unittest.TestCase):
    def test_legacy_items_upgraded_to_historical_baseline(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = str(Path(tmp.name) / "legacy.db")
        # 用旧版仓储（无批次列）造一条旧数据
        legacy = Repository(db_path)
        legacy.conn.execute(
            "INSERT INTO items(title,description,severity,quantity,threshold,"
            "status,version,created_by,created_at,updated_at) "
            "VALUES('legacy','old','minor',1,1,'reported',1,'old','t','t')")
        legacy.conn.commit()
        legacy.close()
        # 新仓储打开：迁移列 + 引导历史基线
        repo = Repository(db_path)
        service = Service(repo)
        item = repo.list_items()[0]
        self.assertEqual(item["batch_id"], HISTORICAL_BATCH_ID)
        self.assertEqual(item["source"], "legacy")
        batch = service.get_batch(HISTORICAL_BATCH_ID, "viewer")
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(batch["historical"], 1)
        # 再次初始化不重复升级、不重复审计
        before = len(repo.list_audit())
        service.bootstrap()
        self.assertEqual(len(repo.list_audit()), before)
        self.assertTrue(repo.verify_audit_chain())
        repo.close()


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.repo = Repository(str(Path(cls.tmp.name) / "http.db"))
        cls.service = Service(cls.repo)
        cls.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(cls.service,
                                           str(Path("static").resolve())))
        cls.port = cls.server.server_address[1]
        import threading
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.repo.close()
        cls.tmp.cleanup()

    def _request(self, method, path, body=None, actor="a", role="observer"):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"X-Actor": actor, "X-Role": role}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers,
            method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_batch_lifecycle_over_http_and_conflict_draft_id(self):
        status, batch = self._request("POST", "/api/batches", {
            "batch_id": "HTTP-1", "source": "shore", "total_chunks": 1})
        self.assertEqual(status, 201)
        status, _ = self._request("POST", "/api/batches/HTTP-1/chunks/0", {
            "entries": [entry("OS-HTTP", "2026-09-30T20:00:00Z")]})
        self.assertEqual(status, 201)
        status, done = self._request("POST", "/api/batches/HTTP-1/complete")
        self.assertEqual(status, 200)
        self.assertEqual(done["result"]["adopted"], 1)
        # 版本冲突响应直接给出 draft_id
        status, ok = self._request(
            "POST", "/api/items/%d/transition" % done["outcomes"][0]["item_id"],
            {"target": "assessing", "expected_version": 1},
            actor="c1", role="response_commander")
        self.assertEqual(status, 200)
        status, conflict = self._request(
            "POST", "/api/items/%d/transition" % done["outcomes"][0]["item_id"],
            {"target": "assessing", "expected_version": 1},
            actor="c2", role="response_commander")
        self.assertEqual(status, 409)
        self.assertIn("draft_id", conflict)
        status, pending = self._request("GET", "/api/pending",
                                        actor="c1", role="response_commander")
        self.assertEqual(status, 200)
        self.assertIsInstance(pending["pending"], list)
        # viewer 不能开批次
        status, denied = self._request("POST", "/api/batches", {
            "batch_id": "HTTP-DENY", "source": "vessel", "total_chunks": 1},
            role="viewer")
        self.assertEqual(status, 403)




class ConcurrencyStressTest(unittest.TestCase):
    def test_parallel_chunk_upload_and_transition(self):
        import threading
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        repo = Repository(str(Path(tmp.name) / "c.db"))
        service = Service(repo)
        service.open_batch({"batch_id": "C-1", "source": "vessel",
                            "total_chunks": 20}, "v", "observer")
        errors = []

        def upload(i):
            try:
                service.upload_chunk(
                    "C-1", i,
                    {"entries": [entry(f"OS-P{i}", "2026-09-30T10:00:00Z")]},
                    "v", "observer")
            except Exception as exc:  # pragma: no cover - 调试用
                errors.append(exc)

        threads = [threading.Thread(target=upload, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        done = service.complete_batch("C-1", "v", "observer")
        self.assertEqual(done["result"]["adopted"], 20)
        # 同一分片被两个线程重复上传也必须幂等
        service.open_batch({"batch_id": "C-2", "source": "drone",
                            "total_chunks": 1}, "d", "observer")

        def same():
            service.upload_chunk(
                "C-2", 0,
                {"entries": [entry("OS-Q1", "2026-09-30T10:00:00Z")]},
                "d", "observer")

        t1 = threading.Thread(target=same)
        t2 = threading.Thread(target=same)
        t1.start(); t2.start(); t1.join(); t2.join()
        done2 = service.complete_batch("C-2", "d", "observer")
        self.assertEqual(done2["result"]["adopted"], 1)
        self.assertTrue(repo.verify_audit_chain())
        repo.close()


if __name__ == "__main__":
    unittest.main()
