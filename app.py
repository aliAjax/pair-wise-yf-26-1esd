"""数字档案长期保存服务：SQLite 多副本、哈希校验、修复与迁移。"""
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
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "preservation.db"
MAX_FILE_SIZE = 10 * 1024 * 1024


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
                CREATE TABLE IF NOT EXISTS copies(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    location TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'healthy' CHECK(state IN ('healthy','corrupt','degraded')),
                    created_at TEXT NOT NULL,
                    last_verified_at TEXT,
                    UNIQUE(version_id,location)
                );
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
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER REFERENCES archives(id),
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS locations(
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    capacity_bytes INTEGER NOT NULL CHECK(capacity_bytes > 0),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS intake_batches(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER NOT NULL REFERENCES archives(id),
                    batch_key TEXT NOT NULL UNIQUE,
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    locations TEXT NOT NULL,
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS intake_submissions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES intake_batches(id),
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    duplicate INTEGER NOT NULL DEFAULT 0 CHECK(duplicate IN (0,1)),
                    files TEXT NOT NULL,
                    locations TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS copy_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    location TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','activated','failed','cancelled')),
                    copy_id INTEGER REFERENCES copies(id),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(version_id,location)
                );
                CREATE TABLE IF NOT EXISTS inspections(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    location TEXT,
                    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','closed')),
                    note TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inspection_items(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inspection_id INTEGER NOT NULL REFERENCES inspections(id),
                    copy_id INTEGER REFERENCES copies(id),
                    version_id INTEGER NOT NULL REFERENCES archive_versions(id),
                    location TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('healthy','repaired','corrupt','failed','queued','activated')),
                    detail TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._migrate_audit_log(conn)

    def _migrate_audit_log(self, conn: sqlite3.Connection) -> None:
        """旧版本 audit_log.archive_id 为 NOT NULL，重建以支持地点/巡检等无档案维度的审计。"""
        cols = conn.execute("PRAGMA table_info(audit_log)").fetchall()
        if not cols:
            return
        archive_col = next((c for c in cols if c["name"] == "archive_id"), None)
        if archive_col and archive_col["notnull"] == 1:
            conn.executescript(
                """
                CREATE TABLE audit_log_new(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_id INTEGER REFERENCES archives(id),
                    actor_id TEXT NOT NULL REFERENCES users(id),
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                INSERT INTO audit_log_new(id,archive_id,actor_id,action,detail,created_at)
                    SELECT id,archive_id,actor_id,action,detail,created_at FROM audit_log;
                DROP TABLE audit_log;
                ALTER TABLE audit_log_new RENAME TO audit_log;
                """
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

    def ingest_version(self, actor_id: str, archive_id: int, files: object) -> dict:
        manifest = verify_manifest(files)
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            self._access(conn, archive_id, actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM archive_versions WHERE archive_id=?", (archive_id,)
                ).fetchone()[0]
                cur = conn.execute(
                    "INSERT INTO archive_versions(archive_id,version,created_by,created_at) VALUES(?,?,?,?)",
                    (archive_id, version_no, actor_id, now()),
                )
                version_id = cur.lastrowid
                for item in manifest:
                    conn.execute(
                        "INSERT INTO archive_files(version_id,path,sha256,size,content) VALUES(?,?,?,?,?)",
                        (version_id, item["path"], item["sha256"], item["size"], item["content"]),
                    )
                self._recompute_version_state(conn, version_id)
                self._audit(
                    conn, archive_id, actor_id, "version.ingest",
                    {"version_id": version_id, "version": version_no, "files": len(manifest),
                     "manifest": [{"path": x["path"], "sha256": x["sha256"], "size": x["size"]} for x in manifest]},
                )
                return {"id": version_id, "archive_id": archive_id, "version": version_no, "file_count": len(manifest)}
            except Exception:
                conn.rollback()
                raise

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
                return self._request_copy_locked(conn, version_id, location, actor_id, allow_existing=False)
            except Exception:
                conn.rollback()
                raise

    def create_location(self, actor_id: str, code: str, name: str, capacity_bytes: int) -> dict:
        code, name = code.strip(), name.strip()
        if len(code) < 2:
            raise BusinessError("地点代码至少 2 字", 422, "invalid_location")
        if not name:
            raise BusinessError("地点名称不能为空", 422, "invalid_location")
        if not isinstance(capacity_bytes, int) or capacity_bytes <= 0:
            raise BusinessError("容量必须为正整数", 422, "invalid_capacity")
        with self.connect() as conn:
            self._user(conn, actor_id, {"owner", "archivist"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO locations(code,name,capacity_bytes,created_at) VALUES(?,?,?,?)",
                    (code, name, capacity_bytes, now()),
                )
                self._audit(conn, None, actor_id, "location.create", {"code": code, "name": name, "capacity_bytes": capacity_bytes})
                return {"code": code, "name": name, "capacity_bytes": capacity_bytes, "occupied_bytes": 0, "available_bytes": capacity_bytes}
            except sqlite3.IntegrityError:
                conn.rollback()
                raise BusinessError("保存地点已存在", 409, "location_exists")

    def list_locations(self, user_id: str) -> list[dict]:
        with self.connect() as conn:
            self._user(conn, user_id, {"owner", "archivist", "auditor"})
            rows = conn.execute("SELECT * FROM locations ORDER BY code").fetchall()
            out = []
            for r in rows:
                occupied = conn.execute(
                    """SELECT COALESCE(SUM(cf.size),0) FROM copy_files cf
                       JOIN copies c ON cf.copy_id=c.id WHERE c.location=? AND c.state='healthy'""",
                    (r["code"],),
                ).fetchone()[0]
                copy_count = conn.execute(
                    "SELECT COUNT(*) FROM copies WHERE location=? AND state='healthy'", (r["code"],)
                ).fetchone()[0]
                out.append({
                    "code": r["code"], "name": r["name"], "capacity_bytes": r["capacity_bytes"],
                    "occupied_bytes": occupied, "available_bytes": r["capacity_bytes"] - occupied,
                    "copy_count": copy_count,
                })
            return out

    def _location_occupancy(self, conn: sqlite3.Connection, location: str) -> int:
        return conn.execute(
            """SELECT COALESCE(SUM(cf.size),0) FROM copy_files cf
               JOIN copies c ON cf.copy_id=c.id WHERE c.location=? AND c.state='healthy'""",
            (location,),
        ).fetchone()[0]

    def _activate_copy_locked(self, conn: sqlite3.Connection, version_id: int, location: str, actor_id: str) -> dict:
        cur = conn.execute(
            "INSERT INTO copies(version_id,location,state,created_at,last_verified_at) VALUES(?,?,?,?,?)",
            (version_id, location, "healthy", now(), now()),
        )
        copy_id = cur.lastrowid
        conn.execute(
            """INSERT INTO copy_files(copy_id,path,sha256,size,content)
               SELECT ?,path,sha256,size,content FROM archive_files WHERE version_id=?""",
            (copy_id, version_id),
        )
        self._recompute_version_state(conn, version_id)
        self._audit(conn, None, actor_id, "copy.create", {"copy_id": copy_id, "version_id": version_id, "location": location})
        return {"id": copy_id, "copy_id": copy_id, "version_id": version_id, "location": location, "state": "healthy", "queued": False}

    def _request_copy_locked(self, conn: sqlite3.Connection, version_id: int, location: str,
                             actor_id: str, allow_existing: bool) -> dict:
        existing = conn.execute("SELECT * FROM copies WHERE version_id=? AND location=?", (version_id, location)).fetchone()
        if existing:
            if not allow_existing:
                raise BusinessError("该版本的副本位置已存在", 409, "copy_exists")
            return {"id": existing["id"], "copy_id": existing["id"], "location": location, "state": existing["state"], "queued": False, "existed": True}
        needed = conn.execute("SELECT COALESCE(SUM(size),0) FROM archive_files WHERE version_id=?", (version_id,)).fetchone()[0]
        loc = conn.execute("SELECT * FROM locations WHERE code=?", (location,)).fetchone()
        if loc and self._location_occupancy(conn, location) + needed > loc["capacity_bytes"]:
            cur = conn.execute(
                """INSERT INTO copy_queue(version_id,location,status,attempts,created_at,updated_at)
                   VALUES(?,?, 'pending',0,?,?)""",
                (version_id, location, now(), now()),
            )
            queue_id = cur.lastrowid
            self._audit(conn, None, actor_id, "copy.queued", {
                "queue_id": queue_id, "version_id": version_id, "location": location,
                "needed": needed, "occupied": self._location_occupancy(conn, location),
                "capacity": loc["capacity_bytes"],
            })
            return {"id": None, "queue_id": queue_id, "location": location, "state": "queued", "queued": True, "needed": needed}
        return self._activate_copy_locked(conn, version_id, location, actor_id)

    def _try_activate_queue(self, conn: sqlite3.Connection, queue_row: sqlite3.Row, actor_id: str) -> bool:
        version_id, location = queue_row["version_id"], queue_row["location"]
        existing = conn.execute("SELECT * FROM copies WHERE version_id=? AND location=?", (version_id, location)).fetchone()
        if existing:
            conn.execute(
                "UPDATE copy_queue SET status='activated', copy_id=?, updated_at=? WHERE id=?",
                (existing["id"], now(), queue_row["id"]),
            )
            return True
        needed = conn.execute("SELECT COALESCE(SUM(size),0) FROM archive_files WHERE version_id=?", (version_id,)).fetchone()[0]
        loc = conn.execute("SELECT * FROM locations WHERE code=?", (location,)).fetchone()
        if loc and self._location_occupancy(conn, location) + needed > loc["capacity_bytes"]:
            conn.execute(
                "UPDATE copy_queue SET attempts=attempts+1, last_error=?, updated_at=? WHERE id=?",
                ("容量不足", now(), queue_row["id"]),
            )
            return False
        res = self._activate_copy_locked(conn, version_id, location, actor_id)
        conn.execute(
            "UPDATE copy_queue SET status='activated', copy_id=?, attempts=attempts+1, updated_at=? WHERE id=?",
            (res["copy_id"], now(), queue_row["id"]),
        )
        self._audit(conn, None, actor_id, "copy.activate", {
            "queue_id": queue_row["id"], "copy_id": res["copy_id"], "version_id": version_id, "location": location,
        })
        return True

    def _record_submission(self, conn: sqlite3.Connection, batch_id: int, actor_id: str,
                           manifest: list[dict], locations: list[str], duplicate: bool) -> None:
        conn.execute(
            """INSERT INTO intake_submissions(batch_id,actor_id,duplicate,files,locations,created_at)
               VALUES(?,?,?,?,?,?)""",
            (batch_id, actor_id, int(bool(duplicate)),
             json.dumps([{"path": x["path"], "sha256": x["sha256"], "size": x["size"]} for x in manifest], ensure_ascii=False),
             json.dumps(locations, ensure_ascii=False), now()),
        )

    def _batch_result(self, conn: sqlite3.Connection, batch: sqlite3.Row, duplicate: bool) -> dict:
        version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (batch["version_id"],)).fetchone()
        copies = conn.execute(
            "SELECT id,location,state FROM copies WHERE version_id=? ORDER BY id", (batch["version_id"],)
        ).fetchall()
        queue = conn.execute(
            "SELECT id,location,status FROM copy_queue WHERE version_id=? ORDER BY id", (batch["version_id"],)
        ).fetchall()
        return {
            "batch_id": batch["id"], "batch_key": batch["batch_key"], "duplicate": duplicate,
            "received_by": batch["created_by"],
            "version": {"id": version["id"], "version": version["version"], "state": version["state"]},
            "copies": [dict(c) for c in copies],
            "queue": [dict(q) for q in queue],
        }

    def ingest_batch(self, actor_id: str, archive_id: int, batch_key: str, files: object, locations: object) -> dict:
        """同一批移交以 batch_key 去重：重复提交只验收一次，后到者保留填写内容并看到先接收人。"""
        manifest = verify_manifest(files)
        locs: list[str] = []
        for loc in locations or []:
            loc = str(loc).strip()
            if loc and loc not in locs:
                locs.append(loc)
        if not str(batch_key or "").strip():
            raise BusinessError("batch_key 必填", 422, "invalid_batch_key")
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            self._access(conn, archive_id, actor, require_write=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                existing = conn.execute("SELECT * FROM intake_batches WHERE batch_key=?", (batch_key,)).fetchone()
                if existing:
                    self._record_submission(conn, existing["id"], actor_id, manifest, locs, duplicate=True)
                    self._audit(conn, archive_id, actor_id, "batch.duplicate", {
                        "batch_id": existing["id"], "batch_key": batch_key, "version_id": existing["version_id"],
                    })
                    return self._batch_result(conn, existing, duplicate=True)
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM archive_versions WHERE archive_id=?", (archive_id,)
                ).fetchone()[0]
                cur = conn.execute(
                    "INSERT INTO archive_versions(archive_id,version,created_by,created_at) VALUES(?,?,?,?)",
                    (archive_id, version_no, actor_id, now()),
                )
                version_id = cur.lastrowid
                for item in manifest:
                    conn.execute(
                        "INSERT INTO archive_files(version_id,path,sha256,size,content) VALUES(?,?,?,?,?)",
                        (version_id, item["path"], item["sha256"], item["size"], item["content"]),
                    )
                for loc in locs:
                    self._request_copy_locked(conn, version_id, loc, actor_id, allow_existing=True)
                cur = conn.execute(
                    "INSERT INTO intake_batches(archive_id,batch_key,version_id,locations,created_by,created_at) VALUES(?,?,?,?,?,?)",
                    (archive_id, batch_key, version_id, json.dumps(locs, ensure_ascii=False), actor_id, now()),
                )
                batch_id = cur.lastrowid
                self._record_submission(conn, batch_id, actor_id, manifest, locs, duplicate=False)
                self._recompute_version_state(conn, version_id)
                self._audit(conn, archive_id, actor_id, "batch.received", {
                    "batch_id": batch_id, "batch_key": batch_key, "version_id": version_id, "version": version_no,
                    "files": len(manifest), "locations": locs,
                })
                return self._batch_result(conn, conn.execute("SELECT * FROM intake_batches WHERE id=?", (batch_id,)).fetchone(), duplicate=False)
            except Exception:
                conn.rollback()
                raise

    def create_inspection(self, actor_id: str, location: str | None = None, note: str = "") -> dict:
        """巡检批次：先激活排队副本，再逐副本校验；失败副本留在批次供重试。"""
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            if location:
                loc = conn.execute("SELECT * FROM locations WHERE code=?", (location,)).fetchone()
                if not loc:
                    raise BusinessError("保存地点不存在", 404, "not_found")
            try:
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    "INSERT INTO inspections(location,status,note,created_by,created_at) VALUES(?, 'open', ?,?,?)",
                    (location, note.strip(), actor_id, now()),
                )
                inspection_id = cur.lastrowid
                items: list[dict] = []
                q = "SELECT * FROM copy_queue WHERE status='pending'"
                params: tuple = ()
                if location:
                    q += " AND location=?"
                    params = (location,)
                q += " ORDER BY id"
                for qe in conn.execute(q, params).fetchall():
                    ok = self._try_activate_queue(conn, qe, actor_id)
                    items.append({
                        "copy_id": None, "version_id": qe["version_id"], "location": qe["location"],
                        "status": "activated" if ok else "queued",
                    })
                cq = "SELECT * FROM copies WHERE state IN ('healthy','corrupt')"
                cparams: tuple = ()
                if location:
                    cq += " AND location=?"
                    cparams = (location,)
                cq += " ORDER BY id"
                for c in conn.execute(cq, cparams).fetchall():
                    ver = conn.execute("SELECT * FROM archive_versions WHERE id=?", (c["version_id"],)).fetchone()
                    self._access(conn, ver["archive_id"], actor, require_write=True)
                    r = self._verify_copy_locked(conn, actor_id, c)
                    if r["repaired"]:
                        status = "repaired"
                    elif r["state"] == "healthy":
                        status = "healthy"
                    else:
                        status = "failed"
                    items.append({
                        "copy_id": c["id"], "version_id": c["version_id"], "location": c["location"],
                        "status": status, "corrupt_paths": r["corrupt_paths"], "repaired": r["repaired"],
                    })
                for it in items:
                    conn.execute(
                        """INSERT INTO inspection_items(inspection_id,copy_id,version_id,location,status,detail,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,?,?)""",
                        (inspection_id, it["copy_id"], it["version_id"], it["location"], it["status"],
                         json.dumps({k: v for k, v in it.items() if k not in ("copy_id", "version_id", "location", "status")}, ensure_ascii=False),
                         now(), now()),
                    )
                self._audit(conn, None, actor_id, "inspection.create", {"inspection_id": inspection_id, "location": location, "items": len(items)})
                return {"inspection_id": inspection_id, "location": location, "items": items}
            except Exception:
                conn.rollback()
                raise

    def retry_inspection(self, actor_id: str, inspection_id: int) -> dict:
        """续作：只重试批次中未达健康/已激活状态的条目，成功条目不重复处理。"""
        with self.connect() as conn:
            actor = self._user(conn, actor_id, {"owner", "archivist"})
            insp = conn.execute("SELECT * FROM inspections WHERE id=?", (inspection_id,)).fetchone()
            if not insp:
                raise BusinessError("巡检批次不存在", 404, "not_found")
            try:
                conn.execute("BEGIN IMMEDIATE")
                items = conn.execute(
                    "SELECT * FROM inspection_items WHERE inspection_id=? ORDER BY id", (inspection_id,)
                ).fetchall()
                retried: list[dict] = []
                for it in items:
                    if it["status"] in ("healthy", "repaired", "activated"):
                        continue
                    if it["status"] == "queued":
                        qe = conn.execute(
                            "SELECT * FROM copy_queue WHERE version_id=? AND location=? AND status='pending'",
                            (it["version_id"], it["location"]),
                        ).fetchone()
                        if qe and self._try_activate_queue(conn, qe, actor_id):
                            new_status = "activated"
                        else:
                            new_status = "queued"
                    else:
                        copy = conn.execute("SELECT * FROM copies WHERE id=?", (it["copy_id"],)).fetchone()
                        if not copy:
                            continue
                        ver = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
                        self._access(conn, ver["archive_id"], actor, require_write=True)
                        r = self._verify_copy_locked(conn, actor_id, copy)
                        new_status = "repaired" if r["repaired"] else ("healthy" if r["state"] == "healthy" else "failed")
                    conn.execute(
                        "UPDATE inspection_items SET status=?, updated_at=? WHERE id=?",
                        (new_status, now(), it["id"]),
                    )
                    retried.append({"item_id": it["id"], "copy_id": it["copy_id"], "status": new_status})
                self._audit(conn, None, actor_id, "inspection.retry", {"inspection_id": inspection_id, "retried": len(retried)})
                return {"inspection_id": inspection_id, "retried": retried}
            except Exception:
                conn.rollback()
                raise

    def get_inspection(self, user_id: str, inspection_id: int) -> dict:
        with self.connect() as conn:
            self._user(conn, user_id, {"owner", "archivist", "auditor"})
            insp = conn.execute("SELECT * FROM inspections WHERE id=?", (inspection_id,)).fetchone()
            if not insp:
                raise BusinessError("巡检批次不存在", 404, "not_found")
            items = conn.execute(
                "SELECT id,copy_id,version_id,location,status,detail,created_at,updated_at FROM inspection_items WHERE inspection_id=? ORDER BY id",
                (inspection_id,),
            ).fetchall()
            return {"inspection": dict(insp), "items": [dict(x) for x in items]}

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
                "SELECT id,location,state,last_verified_at FROM copies WHERE version_id=? ORDER BY id", (version_id,)
            ).fetchall()
            archive = conn.execute("SELECT * FROM archives WHERE id=?", (version["archive_id"],)).fetchone()
            return {"version": dict(version), "archive": dict(archive), "files": [dict(x) for x in files], "copies": [dict(x) for x in copies]}

    def _recompute_version_state(self, conn: sqlite3.Connection, version_id: int) -> str:
        """副本状态一旦变化就重算版本可用性：有健康副本为 verified，否则 degraded。"""
        healthy = conn.execute(
            "SELECT COUNT(*) FROM copies WHERE version_id=? AND state='healthy'", (version_id,)
        ).fetchone()[0]
        new_state = "verified" if healthy > 0 else "degraded"
        conn.execute(
            "UPDATE archive_versions SET state=? WHERE id=? AND state<>?",
            (new_state, version_id, new_state),
        )
        return new_state

    def verify_copy(self, user_id: str, copy_id: int) -> dict:
        with self.connect() as conn:
            self._user(conn, user_id, {"owner", "archivist", "auditor"})
            try:
                conn.execute("BEGIN IMMEDIATE")
                copy = conn.execute("SELECT * FROM copies WHERE id=?", (copy_id,)).fetchone()
                if not copy:
                    raise BusinessError("副本不存在", 404, "not_found")
                return self._verify_copy_locked(conn, user_id, copy)
            except Exception:
                conn.rollback()
                raise

    def _verify_copy_locked(self, conn: sqlite3.Connection, user_id: str, copy: sqlite3.Row) -> dict:
        version = conn.execute("SELECT * FROM archive_versions WHERE id=?", (copy["version_id"],)).fetchone()
        if not version:
            raise BusinessError("档案版本不存在", 404, "not_found")
        user = self._user(conn, user_id, {"owner", "archivist", "auditor"})
        self._access(conn, version["archive_id"], user)
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
            result_state = "degraded"
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
        self._recompute_version_state(conn, copy["version_id"])
        self._audit(
            conn, version["archive_id"], user_id, "copy.verify",
            {"copy_id": copy["id"], "state": result_state, "corrupt_paths": corrupt_paths, "repaired": repaired},
        )
        return {"copy_id": copy["id"], "state": result_state, "corrupt_paths": corrupt_paths, "repaired": repaired}

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
            self._recompute_version_state(conn, version["id"])
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
                # 迁移后的版本照旧继承源版本的副本，保证可校验读回
                for sc in conn.execute("SELECT * FROM copies WHERE version_id=? ORDER BY id", (version_id,)).fetchall():
                    exists = conn.execute(
                        "SELECT id FROM copies WHERE version_id=? AND location=?", (target_version_id, sc["location"])
                    ).fetchone()
                    if exists:
                        continue
                    tcur = conn.execute(
                        "INSERT INTO copies(version_id,location,state,created_at,last_verified_at) VALUES(?,?,?,?,?)",
                        (target_version_id, sc["location"], "healthy", now(), now()),
                    )
                    tcopy_id = tcur.lastrowid
                    conn.execute(
                        """INSERT INTO copy_files(copy_id,path,sha256,size,content)
                           SELECT ?,path,sha256,size,content FROM archive_files WHERE version_id=?""",
                        (tcopy_id, target_version_id),
                    )
                self._recompute_version_state(conn, target_version_id)
                self._audit(
                    conn, source_version["archive_id"], actor_id, "format.migrate",
                    {"source_version_id": version_id, "target_version_id": target_version_id,
                     "source_path": source_path, "target_path": converted["path"], "target_format": target_format.strip()},
                )
                return {"id": target_version_id, "version": version_no, "source_version_id": version_id, "target_path": converted["path"]}
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
            return {
                "archive": dict(archive),
                "days_remaining": (deadline - date.today()).days,
                "versions": [dict(v) | {"file_count": conn.execute("SELECT COUNT(*) FROM archive_files WHERE version_id=?", (v["id"],)).fetchone()[0],
                                         "copy_count": conn.execute("SELECT COUNT(*) FROM copies WHERE version_id=?", (v["id"],)).fetchone()[0]}
                             for v in versions],
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
        path = urlparse(self.path).path.rstrip("/") or "/"
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
                return self._send(201, store.ingest_version(user, archive_id, d.get("files")))
            if parts[3] == "members":
                d = self._body()
                return self._send(201, store.grant(user, archive_id, d.get("user_id", ""), d.get("permission", "")))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and parts[3] == "status" and method == "GET":
            return self._send(200, store.archive_status(user, int(parts[2])))
        if len(parts) == 3 and parts[:2] == ["api", "versions"] and method == "GET":
            return self._send(200, store.get_version(user, int(parts[2])))
        if parts == ["api", "locations"] and method == "POST":
            d = self._body()
            return self._send(201, store.create_location(user, d.get("code", ""), d.get("name", ""), d.get("capacity_bytes", 0)))
        if parts == ["api", "locations"] and method == "GET":
            return self._send(200, store.list_locations(user))
        if len(parts) == 4 and parts[:2] == ["api", "archives"] and parts[3] == "batches" and method == "POST":
            d = self._body()
            return self._send(201, store.ingest_batch(user, int(parts[2]), d.get("batch_key", ""), d.get("files"), d.get("locations", [])))
        if parts == ["api", "inspections"] and method == "POST":
            d = self._body()
            return self._send(201, store.create_inspection(user, d.get("location"), d.get("note", "")))
        if len(parts) == 3 and parts[:2] == ["api", "inspections"] and method == "GET":
            return self._send(200, store.get_inspection(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "inspections"] and parts[3] == "retry" and method == "POST":
            return self._send(200, store.retry_inspection(user, int(parts[2])))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "copies" and method == "POST":
            d = self._body()
            return self._send(201, store.add_copy(user, int(parts[2]), d.get("location", "")))
        if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "migrate" and method == "POST":
            d = self._body()
            return self._send(201, store.migrate(user, int(parts[2]), d.get("source_path", ""), d.get("target_path", ""), d.get("target_format", ""), d.get("content_b64", "")))
        if len(parts) == 4 and parts[:2] == ["api", "copies"] and parts[3] == "verify" and method == "POST":
            return self._send(200, store.verify_copy(user, int(parts[2])))
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
