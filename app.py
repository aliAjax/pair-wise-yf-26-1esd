"""数字档案长期保存服务：SQLite 多副本、哈希校验、修复与迁移。

接收链：移交批次（幂等验收）→ 档案版本 → 保存地点副本（容量排队/重试）→ 巡检批次（可续作）。
"""
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import sqlite3
import threading
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "preservation.db"
MAX_FILE_SIZE = 10 * 1024 * 1024
DEFAULT_LOCATION_CAPACITY = 1 << 40  # 1 TiB，未登记地点的兜底容量
SEED_LOCATION_CAPACITY = 64 * 1024 * 1024

COPIES_COLUMNS = """(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
    location TEXT NOT NULL,
    location_id INTEGER REFERENCES storage_locations(id),
    state TEXT NOT NULL DEFAULT 'healthy' CHECK(state IN ('pending','healthy','corrupt','degraded','failed')),
    reserved_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_verified_at TEXT,
    UNIQUE(version_id,location)
)"""


class BusinessError(Exception):
    def __init__(self, message: str, status: int = 400, code: str = "bad_request"):
        super().__init__(message)
        self.message, self.status, self.code = message, status, code


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def verify_manifest(files: object) -> list[dict]:
    if not isinstance(files, list) or not files:
        raise BusinessError("files 必须是非空数组", 422, "invalid_manifest")
    result, seen = [], set()
    for item in files:
        if not isinstance(item, dict):
            raise BusinessError("文件条目必须是对象", 422, "invalid_manifest")
        raw_path = str(item.get("path", "")).strip().replace("\\", "/")
        pure = PurePosixPath(raw_path)
        if not raw_path or pure.is_absolute() or ".." in pure.parts or pure.name in {"", ".", ".."}:
            raise BusinessError(f"档案路径不安全: {raw_path}", 422, "unsafe_path")
        if raw_path in seen:
            raise BusinessError(f"档案路径重复: {raw_path}", 409, "duplicate_path")
        seen.add(raw_path)
        encoded = item.get("content_b64")
        if not isinstance(encoded, str):
            raise BusinessError(f"{raw_path} 缺少 content_b64", 422, "content_required")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            raise BusinessError(f"{raw_path} 不是合法 Base64", 422, "invalid_base64")
        if len(content) > MAX_FILE_SIZE:
            raise BusinessError(f"{raw_path} 超过单文件大小限制", 413, "file_too_large")
        result.append(
            {"path": raw_path, "content": content, "sha256": hashlib.sha256(content).hexdigest(), "size": len(content)}
        )
    return result


def manifest_hash(manifest: list[dict]) -> str:
    entries = [[x["path"], x["sha256"], x["size"]] for x in sorted(manifest, key=lambda x: x["path"])]
    return hashlib.sha256(json.dumps(entries, separators=(",", ":")).encode()).hexdigest()


class PreservationStore:
    def __init__(self, db_path: str | Path = DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def init_schema(self) -> None:
        with self._lock, self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('owner','archivist','auditor'))
                );
                CREATE TABLE IF NOT EXISTS archives(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    owner_id TEXT NOT NULL REFERENCES users(id),
                    retention_until TEXT NOT NULL,
                    restricted INTEGER NOT NULL DEFAULT 1 CHECK(restricted IN (0,1)),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS archive_members(
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    user_id TEXT NOT NULL REFERENCES users(id),
                    permission TEXT NOT NULL CHECK(permission IN ('read','write')),
                    PRIMARY KEY(archive_id,user_id)
                );
                CREATE TABLE IF NOT EXISTS archive_versions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    version INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'verified' CHECK(state IN ('verified','degraded')),
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL,
                    UNIQUE(archive_id,version)
                );
                CREATE TABLE IF NOT EXISTS archive_files(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    UNIQUE(version_id,path)
                );
                CREATE TABLE IF NOT EXISTS storage_locations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    capacity_bytes INTEGER NOT NULL CHECK(capacity_bytes >= 0),
                    created_at TEXT NOT NULL
                );
                """
                + f"CREATE TABLE IF NOT EXISTS copies{COPIES_COLUMNS};"
                + """
                CREATE TABLE IF NOT EXISTS copy_files(
                    copy_id INTEGER NOT NULL REFERENCES copies(id),
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    PRIMARY KEY(copy_id,path)
                );
                CREATE TABLE IF NOT EXISTS migrations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    target_version_id INTEGER NOT NULL UNIQUE REFERENCES archive_versions(id),
                    source_path TEXT NOT NULL,
                    target_path TEXT NOT NULL,
                    target_format TEXT NOT NULL,
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transfer_batches(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_key TEXT NOT NULL UNIQUE,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    manifest_hash TEXT NOT NULL,
                    received_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transfer_drafts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES transfer_batches(id),
                    submitted_by TEXT NOT NULL REFERENCES users(id),
                    files_json TEXT NOT NULL,
                    manifest_match INTEGER NOT NULL DEFAULT 1 CHECK(manifest_match IN (0,1)),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inspection_batches(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','completed')),
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS inspection_items(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES inspection_batches(id),
                    copy_id INTEGER NOT NULL REFERENCES copies(id),
                    result TEXT NOT NULL DEFAULT 'pending' CHECK(result IN ('pending','passed','failed')),
                    detail TEXT NOT NULL DEFAULT '',
                    checked_at TEXT,
                    UNIQUE(batch_id,copy_id)
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._migrate_copies(conn)
            self._backfill_locations(conn)

    def _migrate_copies(self, conn: sqlite3.Connection) -> None:
        """旧库的 copies 表缺少新状态/容量列时重建，保留原有数据。"""
        row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='copies'").fetchone()
        if not row or "pending" in row["sql"]:
            return
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(
            f"""
            ALTER TABLE copies RENAME TO copies_legacy;
            CREATE TABLE copies{COPIES_COLUMNS};
            INSERT INTO copies(id,version_id,location,state,created_at,last_verified_at)
                SELECT id,version_id,location,state,created_at,last_verified_at FROM copies_legacy;
            DROP TABLE copies_legacy;
            """
        )
        conn.execute("PRAGMA foreign_keys=ON")

    def _backfill_locations(self, conn: sqlite3.Connection) -> None:
        """为历史副本补齐保存地点与容量占用记录，幂等。"""
        for row in conn.execute("SELECT DISTINCT location FROM copies WHERE location_id IS NULL").fetchall():
            conn.execute(
                "INSERT OR IGNORE INTO storage_locations(name,capacity_bytes,created_at) VALUES(?,?,?)",
                (row["location"], DEFAULT_LOCATION_CAPACITY, now()),
            )
        conn.execute(
            "UPDATE copies SET location_id=(SELECT id FROM storage_locations WHERE name=copies.location) "
            "WHERE location_id IS NULL"
        )
        conn.execute(
            """UPDATE copies SET reserved_bytes=(
                   SELECT COALESCE(SUM(size),0) FROM archive_files WHERE version_id=copies.version_id)
               WHERE state='healthy' AND reserved_bytes=0"""
        )

    def seed(self) -> None:
        self.init_schema()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO users(id,name,role) VALUES(?,?,?)",
                [
                    ("owner", "机构档案负责人", "owner"),
                    ("archivist", "档案管理员", "archivist"),
                    ("auditor", "独立审计员", "auditor"),
                    ("outsider", "未授权访客", "auditor"),
                ],
            )
            conn.executemany(
                "INSERT OR IGNORE INTO storage_locations(name,capacity_bytes,created_at) VALUES(?,?,?)",
                [
                    ("site-main", SEED_LOCATION_CAPACITY, now()),
                    ("site-offsite", SEED_LOCATION_CAPACITY, now()),
                ],
            )

    def _user(self, conn, user_id: str | None, roles: set[str] | None = None) -> sqlite3.Row:
        if not user_id:
            raise BusinessError("缺少 X-User-Id", 401, "authentication_required")
        user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not user:
            raise BusinessError("用户不存在", 401, "unknown_user")
        if roles and user["role"] not in roles:
            raise BusinessError("当前角色无权执行此操作", 403, "forbidden")
        return user

    def _access(self, conn, archive_id: int, user: sqlite3.Row, require_write: bool = False) -> None:
        archive = conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone()
        if not archive:
            raise BusinessError("档案不存在", 404, "not_found")
        if archive["owner_id"] == user["id"]:
            return
        row = conn.execute(
            "SELECT permission FROM archive_members WHERE archive_id=? AND user_id=?", (archive_id, user["id"])
        ).fetchone()
        if not row or (require_write and row["permission"] != "write"):
            raise BusinessError("没有该受限档案的访问权限", 403, "forbidden")

    def _audit(self, conn, archive_id: int, actor: str, action: str, detail: dict) -> None:
        conn.execute(
            "INSERT INTO audit_log(archive_id,actor_id,action,detail,created_at) VALUES(?,?,?,?,?)",
            (archive_id, actor, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), now()),
        )

    def _refresh_version_state(self, conn, version_id: int) -> None:
        """副本状态变化后重算版本可用性：存在健康副本为 verified，否则 degraded。"""
        healthy = conn.execute(
            "SELECT COUNT(*) FROM copies WHERE version_id=? AND state='healthy'", (version_id,)
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM copies WHERE version_id=?", (version_id,)).fetchone()[0]
        state = "verified" if healthy > 0 or total == 0 else "degraded"
        conn.execute("UPDATE archive_versions SET state=? WHERE id=?", (state, version_id))

    def create_archive(self, user_id: str, name: str, retention_until: str, restricted: bool = True) -> dict:
        name = name.strip()
        if len(name) < 2:
            raise BusinessError("档案名称至少 2 字", 422, "invalid_name")
        try:
            deadline = date.fromisoformat(retention_until)
        except ValueError:
            raise BusinessError("retention_until 必须是 YYYY-MM-DD", 422, "invalid_retention")
        if deadline < date.today():
            raise BusinessError("保留期限不能早于今天", 422, "retention_in_past")
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist"})
            try:
                cur = conn.execute(
                    "INSERT INTO archives(name,owner_id,retention_until,restricted,created_at) VALUES(?,?,?,?,?)",
                    (name, user_id, retention_until, int(bool(restricted)), now()),
                )
            except sqlite3.IntegrityError:
                raise BusinessError("档案名称已存在", 409, "archive_exists")
            archive_id = cur.lastrowid
            conn.execute(
                "INSERT INTO archive_members(archive_id,user_id,permission) VALUES(?,?,'write')", (archive_id, user_id)
            )
            self._audit(conn, archive_id, user_id, "archive.create", {"retention_until": retention_until, "restricted": restricted})
            return {"id": archive_id, "name": name, "retention_until": retention_until, "restricted": restricted}

    def grant(self, actor_id: str, archive_id: int, user_id: str, permission: str) -> dict:
        if permission not in {"read", "write"}:
            raise BusinessError("permission 必须是 read 或 write", 422, "invalid_permission")
        with self.connect() as conn:
            actor = self._user(conn, actor_id)
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone()
            if not archive:
                raise BusinessError("档案不存在", 404, "not_found")
            if archive["owner_id"] != actor_id:
                raise BusinessError("只有档案所有者可以授权", 403, "forbidden")
            self._user(conn, user_id)
            conn.execute(
                """INSERT INTO archive_members(archive_id,user_id,permission) VALUES(?,?,?)
                   ON CONFLICT(archive_id,user_id) DO UPDATE SET permission=excluded.permission""",
                (archive_id, user_id, permission),
            )
            self._audit(conn, archive_id, actor_id, "access.grant", {"user_id": user_id, "permission": permission})
            return {"archive_id": archive_id, "user_id": user_id, "permission": permission}

    def upsert_location(self, actor_id: str, name: str, capacity_bytes: object) -> dict:
        name = str(name).strip()
        if len(name) < 2:
            raise BusinessError("保存地点名称至少 2 字", 422, "invalid_location")
        try:
            capacity = int(capacity_bytes)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise BusinessError("capacity_bytes 必须是非负整数", 422, "invalid_capacity")
        if capacity < 0:
            raise BusinessError("capacity_bytes 必须是非负整数", 422, "invalid_capacity")
        with self.connect() as conn:
            self._user(conn, actor_id, {"owner", "archivist"})
            conn.execute(
                """INSERT INTO storage_locations(name,capacity_bytes,created_at) VALUES(?,?,?)
                   ON CONFLICT(name) DO UPDATE SET capacity_bytes=excluded.capacity_bytes""",
                (name, capacity, now()),
            )
            return {"name": name, "capacity_bytes": capacity}

    def list_locations(self, user_id: str) -> dict:
        with self.connect() as conn:
            self._user(conn, user_id)
            locations = []
            for loc in conn.execute("SELECT * FROM storage_locations ORDER BY id").fetchall():
                used = conn.execute(
                    "SELECT COALESCE(SUM(reserved_bytes),0) FROM copies WHERE location_id=?", (loc["id"],)
                ).fetchone()[0]
                states = {
                    r["state"]: r["n"]
                    for r in conn.execute(
                        "SELECT state,COUNT(*) AS n FROM copies WHERE location_id=? GROUP BY state", (loc["id"],)
                    ).fetchall()
                }
                locations.append(
                    {"id": loc["id"], "name": loc["name"], "capacity_bytes": loc["capacity_bytes"],
                     "used_bytes": used, "available_bytes": loc["capacity_bytes"] - used,
                     "copies_by_state": states}
                )
            return {"locations": locations}

    def ingest_version(self, actor_id: str, archive_id: int, files: object,
                       batch_key: str | None = None, locations: object = None) -> dict:
        manifest = verify_manifest(files)
        batch_key = (batch_key or "").strip() or None
        if batch_key and len(batch_key) > 128:
            raise BusinessError("batch_key 过长", 422, "invalid_batch")
        if locations is not None:
            if not isinstance(locations, list) or not all(isinstance(x, str) and x.strip() for x in locations):
                raise BusinessError("locations 必须是非空字符串数组", 422, "invalid_locations")
            locations = list(dict.fromkeys(x.strip() for x in locations))
        digest = manifest_hash(manifest)
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            self._access(conn, archive_id, actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                if batch_key:
                    existing = conn.execute(
                        "SELECT * FROM transfer_batches WHERE batch_key=?", (batch_key,)
                    ).fetchone()
                    if existing:
                        # 同一批移交只验收一次：后到者保留填写内容，并看到谁先接收
                        if existing["archive_id"] != archive_id:
                            raise BusinessError("移交批次号已被其他档案使用", 409, "batch_archive_mismatch")
                        match = existing["manifest_hash"] == digest
                        draft_files = [
                            {"path": x["path"], "sha256": x["sha256"], "size": x["size"],
                             "content_b64": base64.b64encode(x["content"]).decode()}
                            for x in manifest
                        ]
                        cur = conn.execute(
                            "INSERT INTO transfer_drafts(batch_id,submitted_by,files_json,manifest_match,created_at) "
                            "VALUES(?,?,?,?,?)",
                            (existing["id"], actor_id, json.dumps(draft_files, ensure_ascii=False), int(match), now()),
                        )
                        self._audit(
                            conn, archive_id, actor_id, "transfer.duplicate",
                            {"batch_id": existing["id"], "batch_key": batch_key,
                             "manifest_match": match, "draft_id": cur.lastrowid},
                        )
                        return {"duplicate": True, "batch_id": existing["id"], "batch_key": batch_key,
                                "version_id": existing["version_id"], "received_by": existing["received_by"],
                                "received_at": existing["created_at"], "manifest_match": match,
                                "draft_id": cur.lastrowid}
                ts = now()
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM archive_versions WHERE archive_id=?", (archive_id,)
                ).fetchone()[0]
                cur = conn.execute(
                    "INSERT INTO archive_versions(archive_id,version,created_by,created_at) VALUES(?,?,?,?)",
                    (archive_id, version_no, actor_id, ts),
                )
                version_id = cur.lastrowid
                for item in manifest:
                    conn.execute(
                        "INSERT INTO archive_files(version_id,path,sha256,size,content) VALUES(?,?,?,?,?)",
                        (version_id, item["path"], item["sha256"], item["size"], item["content"]),
                    )
                self._audit(
                    conn, archive_id, actor_id, "version.ingest",
                    {"version_id": version_id, "version": version_no, "files": len(manifest),
                     "manifest": [{"path": x["path"], "sha256": x["sha256"], "size": x["size"]} for x in manifest]},
                )
                result = {"id": version_id, "archive_id": archive_id, "version": version_no,
                          "file_count": len(manifest), "duplicate": False}
                if batch_key:
                    cur = conn.execute(
                        "INSERT INTO transfer_batches(batch_key,archive_id,version_id,manifest_hash,received_by,created_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (batch_key, archive_id, version_id, digest, actor_id, ts),
                    )
                    batch_id = cur.lastrowid
                    if locations is None:
                        locations = [r["name"] for r in conn.execute(
                            "SELECT name FROM storage_locations ORDER BY id").fetchall()]
                    version_row = {"id": version_id, "archive_id": archive_id}
                    copies = [self._place_copy(conn, actor_id, version_row, name) for name in locations]
                    self._refresh_version_state(conn, version_id)
                    self._audit(
                        conn, archive_id, actor_id, "transfer.accept",
                        {"batch_id": batch_id, "batch_key": batch_key, "version_id": version_id,
                         "copies": [{"id": c["id"], "location": c["location"], "state": c["state"]} for c in copies]},
                    )
                    result["batch"] = {"id": batch_id, "batch_key": batch_key,
                                       "received_by": actor_id, "received_at": ts}
                    result["copies"] = copies
                return result
            except Exception:
                conn.rollback()
                raise

    def _place_copy(self, conn, actor_id: str, version: dict, location_name: str) -> dict:
        """在指定保存地点落地副本：容量足够则健康接收，不足则排队（pending）。"""
        location = conn.execute("SELECT * FROM storage_locations WHERE name=?", (location_name,)).fetchone()
        if not location:
            cur = conn.execute(
                "INSERT INTO storage_locations(name,capacity_bytes,created_at) VALUES(?,?,?)",
                (location_name, DEFAULT_LOCATION_CAPACITY, now()),
            )
            location = conn.execute("SELECT * FROM storage_locations WHERE id=?", (cur.lastrowid,)).fetchone()
        if conn.execute(
            "SELECT 1 FROM copies WHERE version_id=? AND location=?", (version["id"], location_name)
        ).fetchone():
            raise BusinessError("该版本的副本位置已存在", 409, "copy_exists")
        size = conn.execute(
            "SELECT COALESCE(SUM(size),0) FROM archive_files WHERE version_id=?", (version["id"],)
        ).fetchone()[0]
        used = conn.execute(
            "SELECT COALESCE(SUM(reserved_bytes),0) FROM copies WHERE location_id=?", (location["id"],)
        ).fetchone()[0]
        if used + size <= location["capacity_bytes"]:
            state, reserved = "healthy", size
        else:
            state, reserved = "pending", 0
        cur = conn.execute(
            "INSERT INTO copies(version_id,location,location_id,state,reserved_bytes,created_at,last_verified_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (version["id"], location_name, location["id"], state, reserved, now(),
             now() if state == "healthy" else None),
        )
        copy_id = cur.lastrowid
        if state == "healthy":
            conn.execute(
                """INSERT INTO copy_files(copy_id,path,sha256,size,content)
                   SELECT ?,path,sha256,size,content FROM archive_files WHERE version_id=?""",
                (copy_id, version["id"]),
            )
        self._audit(
            conn, version["archive_id"], actor_id, "copy.create",
            {"copy_id": copy_id, "version_id": version["id"], "location": location_name,
             "state": state, "reserved_bytes": reserved},
        )
        return {"id": copy_id, "version_id": version["id"], "location": location_name,
                "state": state, "reserved_bytes": reserved}

    def add_copy(self, actor_id: str, version_id: int, location: str) -> dict:
        location = location.strip()
        if len(location) < 2:
            raise BusinessError("副本位置不能为空", 422, "invalid_location")
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not version:
                raise BusinessError("档案版本不存在", 404, "not_found")
            self._access(conn, version["archive_id"], actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = self._place_copy(conn, actor_id, dict(version), location)
                self._refresh_version_state(conn, version_id)
                return result
            except Exception:
                conn.rollback()
                raise

    def retry_copy(self, actor_id: str, copy_id: int) -> dict:
        """重试排队或失败的副本：容量允许时从主档重新固化，占用只记一次。"""
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
                if not copy:
                    raise BusinessError("副本不存在", 404, "not_found")
                version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
                self._access(conn, version["archive_id"], actor, require_write=True)
                if copy["state"] not in ("pending", "failed"):
                    raise BusinessError("仅排队或失败的副本可以重试", 409, "invalid_state")
                location = conn.execute(
                    "SELECT * FROM storage_locations WHERE id=?", (copy["location_id"],)
                ).fetchone()
                if not location:
                    raise BusinessError("副本未关联保存地点", 409, "location_missing")
                size = conn.execute(
                    "SELECT COALESCE(SUM(size),0) FROM archive_files WHERE version_id=?", (copy["version_id"],)
                ).fetchone()[0]
                used = conn.execute(
                    "SELECT COALESCE(SUM(reserved_bytes),0) FROM copies WHERE location_id=? AND id<>?",
                    (location["id"], copy_id),
                ).fetchone()[0]
                if used + size > location["capacity_bytes"]:
                    self._audit(
                        conn, version["archive_id"], actor_id, "copy.retry",
                        {"copy_id": copy_id, "state": copy["state"], "reason": "capacity_insufficient"},
                    )
                    return {"id": copy_id, "state": copy["state"], "queued": copy["state"] == "pending",
                            "message": "保存地点容量不足，副本保持排队/重试待办"}
                conn.execute("DELETE FROM copy_files WHERE copy_id=?", (copy_id,))
                conn.execute(
                    """INSERT INTO copy_files(copy_id,path,sha256,size,content)
                       SELECT ?,path,sha256,size,content FROM archive_files WHERE version_id=?""",
                    (copy_id, copy["version_id"]),
                )
                conn.execute(
                    "UPDATE copies SET state='healthy',reserved_bytes=?,last_verified_at=? WHERE id=?",
                    (size, now(), copy_id),
                )
                self._refresh_version_state(conn, copy["version_id"])
                self._audit(
                    conn, version["archive_id"], actor_id, "copy.retry",
                    {"copy_id": copy_id, "state": "healthy", "reserved_bytes": size},
                )
                return {"id": copy_id, "state": "healthy", "queued": False, "reserved_bytes": size}
            except Exception:
                conn.rollback()
                raise

    def get_version(self, user_id: str, version_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not version:
                raise BusinessError("档案版本不存在", 404, "not_found")
            self._access(conn, version["archive_id"], user)
            files = conn.execute(
                "SELECT path,sha256,size FROM archive_files WHERE version_id=? ORDER BY path", (version_id,)
            ).fetchall()
            copies = conn.execute(
                "SELECT id,location,state,reserved_bytes,last_verified_at FROM copies WHERE version_id=? ORDER BY id",
                (version_id,),
            ).fetchall()
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (version["archive_id"],)).fetchone()
            return {"version": dict(version), "archive": dict(archive), "files": [dict(x) for x in files], "copies": [dict(x) for x in copies]}

    def read_file(self, user_id: str, version_id: int, path: str) -> dict:
        """校验读回：返回文件内容并现场复核哈希。"""
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not version:
                raise BusinessError("档案版本不存在", 404, "not_found")
            self._access(conn, version["archive_id"], user)
            row = conn.execute(
                "SELECT * FROM archive_files WHERE version_id=? AND path=?", (version_id, path)
            ).fetchone()
            if not row:
                raise BusinessError("文件不存在", 404, "not_found")
            digest = hashlib.sha256(row["content"]).hexdigest()
            verified = digest == row["sha256"] and len(row["content"]) == row["size"]
            return {"version_id": version_id, "path": row["path"], "sha256": row["sha256"],
                    "size": row["size"], "verified": verified,
                    "content_b64": base64.b64encode(row["content"]).decode()}

    def _verify_copy_locked(self, conn, copy: sqlite3.Row) -> dict:
        stored = conn.execute(
            "SELECT path,sha256,size,content FROM copy_files WHERE copy_id=? ORDER BY path", (copy["id"],)
        ).fetchall()
        corrupt_paths = [r["path"] for r in stored if hashlib.sha256(r["content"]).hexdigest() != r["sha256"] or len(r["content"]) != r["size"]]
        repaired = False
        if not corrupt_paths:
            conn.execute("UPDATE copies SET state='healthy',last_verified_at=? WHERE id=?", (now(), copy["id"]))
            result_state = "healthy"
        else:
            conn.execute("UPDATE copies SET state='corrupt',last_verified_at=? WHERE id=?", (now(), copy["id"]))
            healthy = conn.execute(
                "SELECT id FROM copies WHERE version_id=? AND id<>? AND state='healthy' ORDER BY last_verified_at DESC LIMIT 1",
                (copy["version_id"], copy["id"]),
            ).fetchone()
            result_state = "failed"
            if healthy:
                donor = conn.execute(
                    "SELECT path,sha256,size,content FROM copy_files WHERE copy_id=? ORDER BY path", (healthy["id"],)
                ).fetchall()
                donor_by_path = {r["path"]: r for r in donor}
                expected = {r["path"]: r for r in conn.execute(
                    "SELECT path,sha256,size FROM archive_files WHERE version_id=?", (copy["version_id"],)
                ).fetchall()}
                if set(donor_by_path) == set(expected) and all(
                    hashlib.sha256(donor_by_path[p]["content"]).hexdigest() == expected[p]["sha256"] for p in expected
                ):
                    conn.execute("DELETE FROM copy_files WHERE copy_id=?", (copy["id"],))
                    conn.execute(
                        """INSERT INTO copy_files(copy_id,path,sha256,size,content)
                           SELECT ?,path,sha256,size,content FROM copy_files WHERE copy_id=?""",
                        (copy["id"], healthy["id"]),
                    )
                    conn.execute("UPDATE copies SET state='healthy',last_verified_at=? WHERE id=?", (now(), copy["id"]))
                    repaired, result_state = True, "healthy"
            if result_state == "failed":
                conn.execute("UPDATE copies SET state='failed' WHERE id=?", (copy["id"],))
        self._refresh_version_state(conn, copy["version_id"])
        return {"state": result_state, "corrupt_paths": corrupt_paths, "repaired": repaired}

    def verify_copy(self, user_id: str, copy_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
                if not copy:
                    raise BusinessError("副本不存在", 404, "not_found")
                version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
                self._access(conn, version["archive_id"], user)
                if copy["state"] == "pending":
                    raise BusinessError("副本容量排队中，请先重试接收", 409, "copy_queued")
                outcome = self._verify_copy_locked(conn, copy)
                self._audit(conn, version["archive_id"], user_id, "copy.verify", {"copy_id": copy_id, **outcome})
                return {"copy_id": copy_id, **outcome}
            except Exception:
                conn.rollback()
                raise

    def simulate_corruption(self, user_id: str, copy_id: int, path: str) -> dict:
        """仅用于演示和测试，在受控环境中模拟底层介质损坏。"""
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist"})
            copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
            if not copy:
                raise BusinessError("副本不存在", 404, "not_found")
            version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
            self._access(conn, version["archive_id"], user, require_write=True)
            row = conn.execute("SELECT content FROM copy_files WHERE copy_id=? AND path=?", (copy_id, path)).fetchone()
            if not row:
                raise BusinessError("副本文件不存在", 404, "not_found")
            damaged = bytes([row["content"][0] ^ 0xFF]) + row["content"][1:] if row["content"] else b"corrupt"
            conn.execute("UPDATE copy_files SET content=? WHERE copy_id=? AND path=?", (damaged, copy_id, path))
            conn.execute("UPDATE copies SET state='corrupt' WHERE id=?", (copy_id,))
            self._refresh_version_state(conn, copy["version_id"])
            self._audit(conn, version["archive_id"], user_id, "copy.simulate_corruption", {"copy_id": copy_id, "path": path})
            return {"copy_id": copy_id, "path": path, "state": "corrupt"}

    def migrate(self, actor_id: str, version_id: int, source_path: str, target_path: str, target_format: str, content_b64: str) -> dict:
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            source_version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (version_id,)).fetchone()
            if not source_version:
                raise BusinessError("源档案版本不存在", 404, "not_found")
            self._access(conn, source_version["archive_id"], actor, require_write=True)
            source = conn.execute(
                "SELECT * FROM archive_files WHERE version_id=? AND path=?", (version_id, source_path)
            ).fetchone()
            if not source:
                raise BusinessError("源文件不存在", 404, "source_not_found")
            converted = verify_manifest([{"path": target_path, "content_b64": content_b64}])[0]
            try:
                conn.execute("BEGIN IMMEDIATE")
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM archive_versions WHERE archive_id=?", (source_version["archive_id"],)
                ).fetchone()[0]
                cur = conn.execute(
                    "INSERT INTO archive_versions(archive_id,version,created_by,created_at) VALUES(?,?,?,?)",
                    (source_version["archive_id"], version_no, actor_id, now()),
                )
                target_version_id = cur.lastrowid
                conn.execute(
                    """INSERT INTO archive_files(version_id,path,sha256,size,content)
                       SELECT ?,path,sha256,size,content FROM archive_files
                       WHERE version_id=? AND path<>?""",
                    (target_version_id, version_id, source_path),
                )
                conn.execute(
                    "INSERT INTO archive_files(version_id,path,sha256,size,content) VALUES(?,?,?,?,?)",
                    (target_version_id, converted["path"], converted["sha256"], converted["size"], converted["content"]),
                )
                conn.execute(
                    "INSERT INTO migrations(source_version_id,target_version_id,source_path,target_path,target_format,actor_id,created_at) VALUES(?,?,?,?,?,?,?)",
                    (version_id, target_version_id, source_path, converted["path"], target_format.strip(), actor_id, now()),
                )
                # 副本链延续到新版本：同一批保存地点照常落地，容量不足则排队
                target_version = {"id": target_version_id, "archive_id": source_version["archive_id"]}
                copies = [
                    self._place_copy(conn, actor_id, target_version, r["location"])
                    for r in conn.execute(
                        "SELECT DISTINCT location FROM copies WHERE version_id=? ORDER BY id", (version_id,)
                    ).fetchall()
                ]
                self._refresh_version_state(conn, target_version_id)
                self._audit(
                    conn, source_version["archive_id"], actor_id, "format.migrate",
                    {"source_version_id": version_id, "target_version_id": target_version_id,
                     "source_path": source_path, "target_path": converted["path"], "target_format": target_format.strip()},
                )
                return {"id": target_version_id, "version": version_no, "source_version_id": version_id,
                        "target_path": converted["path"], "copies": copies}
            except Exception:
                conn.rollback()
                raise

    def create_inspection_batch(self, actor_id: str, archive_id: int, copy_ids: object = None) -> dict:
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            self._access(conn, archive_id, actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                if copy_ids is None:
                    ids = [r["id"] for r in conn.execute(
                        """SELECT c.id FROM copies c JOIN archive_versions v ON c.version_id=v.id
                           WHERE v.archive_id=? ORDER BY c.id""", (archive_id,),
                    ).fetchall()]
                else:
                    if not isinstance(copy_ids, list) or not all(isinstance(x, int) for x in copy_ids):
                        raise BusinessError("copy_ids 必须是整数数组", 422, "invalid_copies")
                    ids = list(dict.fromkeys(copy_ids))
                    for cid in ids:
                        if not conn.execute(
                            """SELECT 1 FROM copies c JOIN archive_versions v ON c.version_id=v.id
                               WHERE c.id=? AND v.archive_id=?""", (cid, archive_id),
                        ).fetchone():
                            raise BusinessError(f"副本 {cid} 不属于该档案", 422, "invalid_copies")
                if not ids:
                    raise BusinessError("档案下没有可巡检的副本", 422, "no_copies")
                cur = conn.execute(
                    "INSERT INTO inspection_batches(archive_id,created_by,created_at) VALUES(?,?,?)",
                    (archive_id, actor_id, now()),
                )
                batch_id = cur.lastrowid
                for cid in ids:
                    conn.execute("INSERT INTO inspection_items(batch_id,copy_id) VALUES(?,?)", (batch_id, cid))
                self._audit(conn, archive_id, actor_id, "inspection.create", {"batch_id": batch_id, "copies": ids})
                return self._inspection_detail(conn, batch_id)
            except Exception:
                conn.rollback()
                raise

    def _inspection_detail(self, conn, batch_id: int) -> dict:
        batch = conn.execute("SELECT * FROM inspection_batches WHERE id=?", (batch_id,)).fetchone()
        items = conn.execute(
            """SELECT i.id,i.copy_id,i.result,i.detail,i.checked_at,
                      c.location,c.state AS copy_state,c.version_id
               FROM inspection_items i JOIN copies c ON i.copy_id=c.id
               WHERE i.batch_id=? ORDER BY i.id""",
            (batch_id,),
        ).fetchall()
        return {
            "id": batch["id"], "archive_id": batch["archive_id"], "status": batch["status"],
            "created_by": batch["created_by"], "created_at": batch["created_at"],
            "finished_at": batch["finished_at"],
            "items": [dict(i) | {"detail": json.loads(i["detail"]) if i["detail"] else None} for i in items],
        }

    def get_inspection_batch(self, user_id: str, batch_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            batch = conn.execute("SELECT * FROM inspection_batches WHERE id=?", (batch_id,)).fetchone()
            if not batch:
                raise BusinessError("巡检批次不存在", 404, "not_found")
            self._access(conn, batch["archive_id"], user)
            return self._inspection_detail(conn, batch_id)

    def run_inspection_batch(self, actor_id: str, batch_id: int) -> dict:
        """可续作巡检：只处理未通过项，失败副本留在批次中等待下一轮重试。"""
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            batch = conn.execute("SELECT * FROM inspection_batches WHERE id=?", (batch_id,)).fetchone()
            if not batch:
                raise BusinessError("巡检批次不存在", 404, "not_found")
            self._access(conn, batch["archive_id"], actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                pending_items = conn.execute(
                    "SELECT * FROM inspection_items WHERE batch_id=? AND result<>'passed' ORDER BY id", (batch_id,)
                ).fetchall()
                processed = 0
                for item in pending_items:
                    copy = conn.execute("SELECT * FROM copies WHERE id=?", (item["copy_id"],)).fetchone()
                    if not copy:
                        outcome, result = {"error": "副本不存在"}, "failed"
                    elif copy["state"] == "pending":
                        outcome, result = {"state": "pending", "note": "副本容量排队中"}, "failed"
                    else:
                        outcome = self._verify_copy_locked(conn, copy)
                        result = "passed" if outcome["state"] == "healthy" else "failed"
                    conn.execute(
                        "UPDATE inspection_items SET result=?,detail=?,checked_at=? WHERE id=?",
                        (result, json.dumps(outcome, ensure_ascii=False, sort_keys=True), now(), item["id"]),
                    )
                    self._audit(
                        conn, batch["archive_id"], actor_id, "inspection.verify",
                        {"batch_id": batch_id, "copy_id": item["copy_id"], "result": result, **outcome},
                    )
                    processed += 1
                remaining = conn.execute(
                    "SELECT COUNT(*) FROM inspection_items WHERE batch_id=? AND result<>'passed'", (batch_id,)
                ).fetchone()[0]
                if remaining == 0:
                    conn.execute(
                        "UPDATE inspection_batches SET status='completed',finished_at=? WHERE id=?",
                        (now(), batch_id),
                    )
                    status = "completed"
                else:
                    conn.execute("UPDATE inspection_batches SET status='open' WHERE id=?", (batch_id,))
                    status = "open"
                self._audit(
                    conn, batch["archive_id"], actor_id, "inspection.run",
                    {"batch_id": batch_id, "processed": processed, "status": status},
                )
                return self._inspection_detail(conn, batch_id)
            except Exception:
                conn.rollback()
                raise

    def archive_status(self, user_id: str, archive_id: int) -> dict:
        with self.connect() as conn:
            user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
            self._access(conn, archive_id, user)
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone()
            versions = conn.execute("SELECT id,version,state,created_at FROM archive_versions WHERE archive_id=? ORDER BY version", (archive_id,)).fetchall()
            deadline = date.fromisoformat(archive["retention_until"])
            transfers = []
            for b in conn.execute(
                "SELECT * FROM transfer_batches WHERE archive_id=? ORDER BY id", (archive_id,)
            ).fetchall():
                drafts = [
                    {"id": d["id"], "submitted_by": d["submitted_by"],
                     "manifest_match": bool(d["manifest_match"]), "created_at": d["created_at"],
                     "files": [{k: v for k, v in f.items() if k != "content_b64"}
                               for f in json.loads(d["files_json"])]}
                    for d in conn.execute(
                        "SELECT * FROM transfer_drafts WHERE batch_id=? ORDER BY id", (b["id"],)
                    ).fetchall()
                ]
                transfers.append(
                    {"id": b["id"], "batch_key": b["batch_key"], "version_id": b["version_id"],
                     "received_by": b["received_by"], "received_at": b["created_at"],
                     "manifest_hash": b["manifest_hash"], "drafts": drafts}
                )
            return {
                "archive": dict(archive),
                "days_remaining": (deadline - date.today()).days,
                "versions": [dict(v) | {"file_count": conn.execute("SELECT COUNT(*) FROM archive_files WHERE version_id=?", (v["id"],)).fetchone()[0],
                                         "copy_count": conn.execute("SELECT COUNT(*) FROM copies WHERE version_id=?", (v["id"],)).fetchone()[0]}
                             for v in versions],
                "transfers": transfers,
                "audit": [dict(r) | {"detail": json.loads(r["detail"])} for r in conn.execute("SELECT * FROM audit_log WHERE archive_id=? ORDER BY id", (archive_id,)).fetchall()],
            }


class Handler(BaseHTTPRequestHandler):
    server_version = "Preservation/1.0"

    def _store(self):
        return self.server.store  # type: ignore[attr-defined]

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        try:
            data = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise BusinessError("请求体必须是合法 JSON", 400, "invalid_json")
        if not isinstance(data, dict):
            raise BusinessError("JSON 顶层必须是对象", 422, "invalid_json")
        return data

    def _send(self, status: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        parts = [p for p in path.split("/") if p]
        user = self.headers.get("X-User-Id", "")
        if method == "GET" and path == "/":
            body = (BASE_DIR / "web" / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if method == "GET" and path == "/health":
            return self._send(200, {"ok": True})
        store = self._store()
        if parts == ["api", "archives"] and method == "POST":
            d = self._body()
            return self._send(201, store.create_archive(user, d.get("name", ""), d.get("retention_until", ""), bool(d.get("restricted", True))))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and method == "POST":
            archive_id = int(parts[2])
            if parts[3] == "versions":
                d = self._body()
                result = store.ingest_version(user, archive_id, d.get("files"), d.get("batch_key"), d.get("locations"))
                return self._send(200 if result.get("duplicate") else 201, result)
            if parts[3] == "members":
                d = self._body()
                return self._send(201, store.grant(user, archive_id, d.get("user_id", ""), d.get("permission", "")))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and parts[3] == "status" and method == "GET":
            return self._send(200, store.archive_status(user, int(parts[2])))
        if parts == ["api", "locations"] and method == "POST":
            d = self._body()
            return self._send(201, store.upsert_location(user, d.get("name", ""), d.get("capacity_bytes", 0)))
        if parts == ["api", "locations"] and method == "GET":
            return self._send(200, store.list_locations(user))
        if parts == ["api", "inspection-batches"] and method == "POST":
            d = self._body()
            return self._send(201, store.create_inspection_batch(user, int(d.get("archive_id", 0)), d.get("copy_ids")))
        if len(parts) == 3 and parts[:2] == ["api", "inspection-batches"] and method == "GET":
            return self._send(200, store.get_inspection_batch(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "inspection-batches"] and parts[3] == "run" and method == "POST":
            return self._send(200, store.run_inspection_batch(user, int(parts[2])))
        if len(parts) == 3 and parts[:2] == ["api", "versions"] and method == "GET":
            return self._send(200, store.get_version(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "read" and method == "GET":
            query = parse_qs(parsed.query)
            return self._send(200, store.read_file(user, int(parts[2]), query.get("path", [""])[0]))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "copies" and method == "POST":
            d = self._body()
            return self._send(201, store.add_copy(user, int(parts[2]), d.get("location", "")))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "migrate" and method == "POST":
            d = self._body()
            return self._send(201, store.migrate(user, int(parts[2]), d.get("source_path", ""), d.get("target_path", ""), d.get("target_format", ""), d.get("content_b64", "")))
        if len(parts) == 4 and parts[:2] == ["api", "copies"] and parts[3] == "verify" and method == "POST":
            return self._send(200, store.verify_copy(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "copies"] and parts[3] == "retry" and method == "POST":
            return self._send(200, store.retry_copy(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "copies"] and parts[3] == "simulate-corruption" and method == "POST":
            d = self._body()
            return self._send(200, store.simulate_corruption(user, int(parts[2]), d.get("path", "")))
        raise BusinessError("接口不存在", 404, "not_found")

    def _handle(self, method: str) -> None:
        try:
            self._dispatch(method)
        except BusinessError as exc:
            self._send(exc.status, {"error": {"code": exc.code, "message": exc.message}})
        except (ValueError, TypeError):
            self._send(400, {"error": {"code": "invalid_path", "message": "路径参数格式错误"}})
        except Exception as exc:
            self._send(500, {"error": {"code": "internal_error", "message": str(exc)}})

    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def do_DELETE(self): self._handle("DELETE")
    def log_message(self, fmt, *args): print(f"{self.address_string()} - {fmt % args}")


class PreservationServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, store):
        self.store = store
        super().__init__(address, Handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="数字档案长期保存服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--port", type=int, default=8102)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    store = PreservationStore(args.db)
    store.init_schema()
    if args.seed:
        store.seed()
    if args.init or args.seed:
        print(f"数据库已初始化: {args.db}")
        return
    print(f"数字档案服务运行于 http://127.0.0.1:{args.port}")
    server = PreservationServer(("127.0.0.1", args.port), store)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
