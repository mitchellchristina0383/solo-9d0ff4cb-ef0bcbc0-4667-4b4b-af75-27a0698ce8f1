"""SQLite 访问层：单连接 + 写锁，WAL 模式。"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable

from . import config

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id      TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    base_url     TEXT NOT NULL,
    capacity_mb  INTEGER NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deployments (
    id                    TEXT PRIMARY KEY,
    model_name            TEXT NOT NULL,
    mem_per_replica_mb    INTEGER NOT NULL,
    replica_count         INTEGER NOT NULL,
    status                TEXT NOT NULL,
    reason                TEXT,
    epoch                 INTEGER NOT NULL DEFAULT 1,
    attempts              INTEGER NOT NULL DEFAULT 1,
    artifact_id           TEXT,
    artifact_version      INTEGER,
    expected_sha256       TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS replicas (
    id             TEXT PRIMARY KEY,
    deployment_id  TEXT NOT NULL REFERENCES deployments(id),
    node_id        TEXT NOT NULL REFERENCES nodes(node_id),
    seq            INTEGER NOT NULL,
    epoch          INTEGER NOT NULL,
    status         TEXT NOT NULL,
    mem_mb         INTEGER NOT NULL,
    last_error     TEXT,
    expected_sha256  TEXT,
    loaded_sha256    TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    confirmed_at   TEXT,
    released_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_replicas_dep ON replicas(deployment_id);
CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id);
CREATE TABLE IF NOT EXISTS artifacts (
    id           TEXT PRIMARY KEY,
    model_name   TEXT NOT NULL,
    version      INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    filename     TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE(model_name, version),
    UNIQUE(model_name, sha256)
);
CREATE INDEX IF NOT EXISTS idx_artifacts_model ON artifacts(model_name, version);
CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id  TEXT,
    replica_id     TEXT,
    node_id        TEXT,
    event_type     TEXT NOT NULL,
    detail         TEXT,
    created_at     TEXT NOT NULL
);
"""

# 既有库的轻量迁移：补齐制品功能新增列（表本身由 SCHEMA 的 IF NOT EXISTS 建立）
MIGRATIONS = (
    ("deployments", "artifact_id", "ALTER TABLE deployments ADD COLUMN artifact_id TEXT"),
    ("deployments", "artifact_version", "ALTER TABLE deployments ADD COLUMN artifact_version INTEGER"),
    ("deployments", "expected_sha256", "ALTER TABLE deployments ADD COLUMN expected_sha256 TEXT"),
    ("replicas", "expected_sha256", "ALTER TABLE replicas ADD COLUMN expected_sha256 TEXT"),
    ("replicas", "loaded_sha256", "ALTER TABLE replicas ADD COLUMN loaded_sha256 TEXT"),
)


def _migrate(conn: sqlite3.Connection) -> None:
    for table, column, ddl in MIGRATIONS:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(ddl)


def connect() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(config.DB_PATH)), exist_ok=True)
        _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=30)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=30000")
        _conn.executescript(SCHEMA)
        _migrate(_conn)
    return _conn


@contextmanager
def tx():
    """串行化所有写事务，保证并发部署下的预留检查与插入原子。"""
    with _lock:
        conn = connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def query(sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    with _lock:
        return connect().execute(sql, tuple(params)).fetchall()


def query_one(sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    with _lock:
        return connect().execute(sql, tuple(params)).fetchone()
