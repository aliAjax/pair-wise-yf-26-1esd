import base64
import hashlib
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

from app import BusinessError, PreservationStore


class PreservationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = PreservationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.archive = self.store.create_archive("owner", "城市测绘档案", (date.today() + timedelta(days=3650)).isoformat())
        self.raw = b"<record><id>1</id></record>"
        self.version = self.store.ingest_version("owner", self.archive["id"], [
            {"path": "records/one.xml", "content_b64": base64.b64encode(self.raw).decode()},
            {"path": "README.txt", "content_b64": base64.b64encode(b"archive readme").decode()},
        ])
        self.copy1 = self.store.add_copy("owner", self.version["id"], "offline-disk-a")["id"]
        self.copy2 = self.store.add_copy("owner", self.version["id"], "offline-disk-b")["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_integrity_repair_and_format_migration(self):
        self.store.simulate_corruption("owner", self.copy1, "records/one.xml")
        result = self.store.verify_copy("owner", self.copy1)
        self.assertEqual(result["state"], "healthy")
        self.assertTrue(result["repaired"])
        self.assertEqual(result["corrupt_paths"], ["records/one.xml"])
        migrated = self.store.migrate(
            "owner", self.version["id"], "records/one.xml", "records/one.html", "html",
            base64.b64encode(b"<html><body><p>1</p></body></html>").decode(),
        )
        detail = self.store.get_version("owner", migrated["id"])
        self.assertEqual(detail["version"]["version"], 2)
        self.assertTrue(any(f["path"] == "records/one.html" for f in detail["files"]))
        status = self.store.archive_status("owner", self.archive["id"])
        self.assertGreater(status["days_remaining"], 3000)

    def test_restricted_access_and_invalid_manifest_are_rejected(self):
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_version("outsider", self.version["id"])
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(BusinessError) as ctx:
            self.store.ingest_version("owner", self.archive["id"], [{"path": "../escape.txt", "content_b64": "eA=="}])
        self.assertEqual(ctx.exception.code, "unsafe_path")
        with self.assertRaises(BusinessError) as ctx:
            self.store.add_copy("owner", self.version["id"], "offline-disk-a")
        self.assertEqual(ctx.exception.code, "copy_exists")

    def test_batch_ingest_idempotent_and_late_draft_kept(self):
        self.store.grant("owner", self.archive["id"], "archivist", "write")
        files = [{"path": "doc.txt", "content_b64": base64.b64encode(b"transfer one").decode()}]
        first = self.store.ingest_version("owner", self.archive["id"], files, batch_key="T-001")
        self.assertFalse(first["duplicate"])
        # 同一批移交同时落到两地（默认全部已登记保存点，含 setUp 自动登记的离线盘）
        self.assertGreaterEqual({c["location"] for c in first["copies"]}, {"site-main", "site-offsite"})
        self.assertTrue(all(c["state"] == "healthy" for c in first["copies"]))
        # 重复提交只验收一次，后到者看到谁先接收
        again = self.store.ingest_version("archivist", self.archive["id"], files, batch_key="T-001")
        self.assertTrue(again["duplicate"])
        self.assertEqual(again["version_id"], first["id"])
        self.assertEqual(again["received_by"], "owner")
        self.assertTrue(again["manifest_match"])
        # 内容不一致的后到提交同样保留填写内容
        changed = self.store.ingest_version(
            "archivist", self.archive["id"],
            [{"path": "doc.txt", "content_b64": base64.b64encode(b"changed").decode()}],
            batch_key="T-001",
        )
        self.assertTrue(changed["duplicate"])
        self.assertFalse(changed["manifest_match"])
        status = self.store.archive_status("owner", self.archive["id"])
        transfers = [t for t in status["transfers"] if t["batch_key"] == "T-001"]
        self.assertEqual(len(transfers), 1)
        self.assertEqual(len(transfers[0]["drafts"]), 2)
        self.assertEqual(transfers[0]["drafts"][0]["submitted_by"], "archivist")
        self.assertEqual(transfers[0]["drafts"][0]["files"][0]["path"], "doc.txt")
        # 版本只建了一次：setUp 一个 + 批次验收一个
        self.assertEqual(len(status["versions"]), 2)

    def test_concurrent_batch_ingest_first_writer_wins(self):
        self.store.grant("owner", self.archive["id"], "archivist", "write")
        files = [{"path": "a.bin", "content_b64": base64.b64encode(b"concurrent").decode()}]
        results, errors = {}, {}
        barrier = threading.Barrier(2)

        def submit(user):
            barrier.wait()
            try:
                results[user] = self.store.ingest_version(user, self.archive["id"], files, batch_key="T-race")
            except Exception as exc:  # pragma: no cover - 失败时输出诊断
                errors[user] = exc

        threads = [threading.Thread(target=submit, args=(u,)) for u in ("owner", "archivist")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, {})
        winners = [u for u, r in results.items() if not r.get("duplicate")]
        losers = [u for u, r in results.items() if r.get("duplicate")]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        self.assertEqual(results[losers[0]]["received_by"], winners[0])
        self.assertEqual(results[losers[0]]["version_id"], results[winners[0]]["id"])
        status = self.store.archive_status("owner", self.archive["id"])
        transfers = [t for t in status["transfers"] if t["batch_key"] == "T-race"]
        self.assertEqual(len(transfers), 1)
        self.assertEqual(transfers[0]["received_by"], winners[0])
        self.assertEqual(len(transfers[0]["drafts"]), 1)
        self.assertEqual(transfers[0]["drafts"][0]["submitted_by"], losers[0])

    def test_capacity_queue_retry_and_no_double_charge(self):
        payload = b"z" * (1024 * 1024)
        version = self.store.ingest_version(
            "owner", self.archive["id"],
            [{"path": "big.bin", "content_b64": base64.b64encode(payload).decode()}],
        )
        self.store.upsert_location("owner", "site-tight", 512 * 1024)
        copy = self.store.add_copy("owner", version["id"], "site-tight")
        self.assertEqual(copy["state"], "pending")
        self.assertEqual(copy["reserved_bytes"], 0)

        def location():
            return [l for l in self.store.list_locations("owner")["locations"] if l["name"] == "site-tight"][0]

        self.assertEqual(location()["used_bytes"], 0)
        # 排队中的副本不能校验
        with self.assertRaises(BusinessError) as ctx:
            self.store.verify_copy("owner", copy["id"])
        self.assertEqual(ctx.exception.code, "copy_queued")
        # 容量不足时重试仍然排队
        still = self.store.retry_copy("owner", copy["id"])
        self.assertEqual(still["state"], "pending")
        # 扩容后重试成功，占用恰好一次
        self.store.upsert_location("owner", "site-tight", 2 * 1024 * 1024)
        ok = self.store.retry_copy("owner", copy["id"])
        self.assertEqual(ok["state"], "healthy")
        self.assertEqual(location()["used_bytes"], 1024 * 1024)
        # 校验不重复扣容量；健康副本不能再走重试
        self.store.verify_copy("owner", copy["id"])
        self.assertEqual(location()["used_bytes"], 1024 * 1024)
        with self.assertRaises(BusinessError) as ctx:
            self.store.retry_copy("owner", copy["id"])
        self.assertEqual(ctx.exception.code, "invalid_state")

    def test_inspection_batch_keeps_failed_copies_for_retry(self):
        # 两个副本都损坏，没有健康副本可修复
        self.store.simulate_corruption("owner", self.copy1, "records/one.xml")
        self.store.simulate_corruption("owner", self.copy2, "records/one.xml")
        batch = self.store.create_inspection_batch("owner", self.archive["id"])
        self.assertEqual(batch["status"], "open")
        run1 = self.store.run_inspection_batch("owner", batch["id"])
        self.assertEqual(run1["status"], "open")
        self.assertTrue(all(i["result"] == "failed" for i in run1["items"]))
        # 副本状态变化已联动版本可用性
        self.assertEqual(self.store.get_version("owner", self.version["id"])["version"]["state"], "degraded")
        # 修复 copy1 后重跑同一批次：失败项留在批次中重试，已通过项不重复处理
        self.assertEqual(self.store.retry_copy("owner", self.copy1)["state"], "healthy")
        run2 = self.store.run_inspection_batch("owner", batch["id"])
        self.assertEqual(run2["status"], "completed")
        items = {i["copy_id"]: i for i in run2["items"]}
        self.assertEqual(items[self.copy1]["result"], "passed")
        self.assertEqual(items[self.copy2]["result"], "passed")
        self.assertTrue(items[self.copy2]["detail"]["repaired"])
        self.assertEqual(self.store.get_version("owner", self.version["id"])["version"]["state"], "verified")

    def test_migrated_version_keeps_replica_chain_and_readback(self):
        files = [{"path": "doc.txt", "content_b64": base64.b64encode(b"original").decode()}]
        version = self.store.ingest_version(
            "owner", self.archive["id"], files, batch_key="T-migrate", locations=["site-main", "site-offsite"],
        )
        html = b"<html><body>original</body></html>"
        migrated = self.store.migrate(
            "owner", version["id"], "doc.txt", "doc.html", "html",
            base64.b64encode(html).decode(),
        )
        # 迁移版本在两地都有副本
        self.assertEqual({c["location"] for c in migrated["copies"]}, {"site-main", "site-offsite"})
        detail = self.store.get_version("owner", migrated["id"])
        self.assertEqual({c["location"] for c in detail["copies"]}, {"site-main", "site-offsite"})
        # 迁移后的版本照旧能校验
        for copy in detail["copies"]:
            self.assertEqual(self.store.verify_copy("owner", copy["id"])["state"], "healthy")
        # 照旧能校验读回
        read = self.store.read_file("owner", migrated["id"], "doc.html")
        self.assertTrue(read["verified"])
        self.assertEqual(base64.b64decode(read["content_b64"]), html)
        self.assertEqual(read["sha256"], hashlib.sha256(html).hexdigest())


if __name__ == "__main__":
    unittest.main()
