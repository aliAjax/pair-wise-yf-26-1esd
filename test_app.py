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


if __name__ == "__main__":
    unittest.main()


class IntakeChainTests(unittest.TestCase):
    """档案版本 × 保存地点 × 巡检批次的可续作接收链。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = PreservationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.archive = self.store.create_archive("owner", "移交链测试档案", (date.today() + timedelta(days=3650)).isoformat())
        self.store.grant("owner", self.archive["id"], "archivist", "write")
        self.store.create_location("owner", "sh", "上海异地库", 100)
        self.store.create_location("owner", "bj", "北京异地库", 100000)
        self.store.create_location("owner", "gz", "广州异地库", 100000)
        self.store.create_location("owner", "cd", "成都异地库", 100000)

    def tearDown(self):
        self.tmp.cleanup()

    def _files(self, content=b"<record/>"):
        return [{"path": "records/one.xml", "content_b64": base64.b64encode(content).decode()}]

    def _state(self, version_id):
        return self.store.get_version("owner", version_id)["version"]["state"]

    def test_batch_duplicate_accepted_once(self):
        first = self.store.ingest_batch("owner", self.archive["id"], "batch-1", self._files(), ["bj"])
        second = self.store.ingest_batch("owner", self.archive["id"], "batch-1", self._files(), ["bj"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["version"]["version"], first["version"]["version"])
        self.assertEqual(second["received_by"], "owner")

    def test_concurrent_submission_first_wins(self):
        results = {}
        barrier = threading.Barrier(2)

        def submit(admin):
            barrier.wait()
            results[admin] = self.store.ingest_batch(admin, self.archive["id"], "race", self._files(), ["bj"])

        t1 = threading.Thread(target=submit, args=("owner",))
        t2 = threading.Thread(target=submit, args=("archivist",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(results["owner"]["version"]["version"], results["archivist"]["version"]["version"])
        self.assertEqual(results["owner"]["received_by"], results["archivist"]["received_by"])
        self.assertNotEqual(results["owner"]["duplicate"], results["archivist"]["duplicate"])

    def test_capacity_queue_then_inspection_activates(self):
        q1 = self.store.ingest_batch("owner", self.archive["id"], "q1", self._files(b"q" * 60), ["sh"])
        q2 = self.store.ingest_batch("owner", self.archive["id"], "q2", self._files(b"q" * 50), ["sh"])
        self.assertTrue(any(q["status"] == "pending" for q in q2["queue"]))
        self.assertEqual(self._state(q2["version"]["id"]), "degraded")
        sh_copy = [c for c in q1["copies"] if c["location"] == "sh"][0]["id"]
        self.store.simulate_corruption("owner", sh_copy, "records/one.xml")
        insp = self.store.create_inspection("owner", "sh")
        self.assertTrue(any(i["status"] == "activated" for i in insp["items"] if i["version_id"] == q2["version"]["id"]))
        self.assertEqual(self._state(q2["version"]["id"]), "verified")

    def test_failed_copy_stays_for_retry(self):
        f1 = self.store.ingest_batch("owner", self.archive["id"], "f1", self._files(b"f" * 5000), ["bj"])
        copy_id = f1["copies"][0]["id"]
        self.store.simulate_corruption("owner", copy_id, "records/one.xml")
        self.assertEqual(self._state(f1["version"]["id"]), "degraded")
        insp = self.store.create_inspection("owner", "bj")
        self.assertTrue(any(i["status"] == "failed" for i in insp["items"] if i["copy_id"] == copy_id))
        # 在 gz 补一个健康副本作为供体，续作批次
        self.store.add_copy("owner", f1["version"]["id"], "gz")
        retry = self.store.retry_inspection("owner", insp["inspection_id"])
        self.assertTrue(all(r["status"] == "repaired" for r in retry["retried"] if r["copy_id"] == copy_id))
        self.assertEqual(self._state(f1["version"]["id"]), "verified")

    def test_successful_occupancy_not_deduplicated_twice(self):
        f1 = self.store.ingest_batch("owner", self.archive["id"], "f1", self._files(b"f" * 5000), ["bj", "gz"])
        before = {l["code"]: l for l in self.store.list_locations("owner")}["bj"]["occupied_bytes"]
        bj_copy = [c for c in f1["copies"] if c["location"] == "bj"][0]["id"]
        gz_copy = [c for c in f1["copies"] if c["location"] == "gz"][0]["id"]
        self.store.simulate_corruption("owner", bj_copy, "records/one.xml")
        self.store.simulate_corruption("owner", gz_copy, "records/one.xml")
        insp = self.store.create_inspection("owner", None)
        self.store.add_copy("owner", f1["version"]["id"], "cd")  # 供体
        self.store.retry_inspection("owner", insp["inspection_id"])
        after = {l["code"]: l for l in self.store.list_locations("owner")}["bj"]["occupied_bytes"]
        self.assertEqual(after, before)

    def test_version_state_recomputed_on_copy_change(self):
        v = self.store.ingest_batch("owner", self.archive["id"], "v1", self._files(), ["bj"])
        vid = v["version"]["id"]
        self.assertEqual(self._state(vid), "verified")
        copy_id = v["copies"][0]["id"]
        self.store.simulate_corruption("owner", copy_id, "records/one.xml")
        self.assertEqual(self._state(vid), "degraded")
        self.store.add_copy("owner", vid, "gz")
        self.store.create_inspection("owner", "bj")
        self.assertEqual(self._state(vid), "verified")

    def test_migrated_version_verifiable(self):
        m1 = self.store.ingest_batch("owner", self.archive["id"], "m1", self._files(b"m" * 100), ["bj"])
        migrated = self.store.migrate(
            "owner", m1["version"]["id"], "records/one.xml", "records/one.html", "html",
            base64.b64encode(b"<html/>").decode(),
        )
        detail = self.store.get_version("owner", migrated["id"])
        self.assertEqual(detail["version"]["version"], 2)
        self.assertEqual(detail["version"]["state"], "verified")
        self.assertTrue(any(f["path"] == "records/one.html" for f in detail["files"]))
        copy_id = detail["copies"][0]["id"]
        self.assertEqual(self.store.verify_copy("owner", copy_id)["state"], "healthy")

