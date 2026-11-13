from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def insert_source(
    conn: sqlite3.Connection, *, name: str, manifest_path: str, note: str | None = None
) -> int:
    cur = conn.execute(
        "INSERT INTO pull_sources(name, manifest_path, note, created_at) VALUES (?,?,?,?)",
        (name, manifest_path, note, _now()),
    )
    return int(cur.lastrowid)


def list_sources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, name, manifest_path, note, created_at FROM pull_sources ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_source(conn: sqlite3.Connection, source_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id, name, manifest_path, note, created_at FROM pull_sources WHERE id=?",
        (source_id,),
    ).fetchone()
    return dict(row) if row else None


def insert_task_page(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    stream_id: int,
    page_index: int,
    page_token: str,
    page_file: str,
    status: str,
    events_count: int,
    ledger_inserted: int,
    applied: int,
    quarantined: int,
    skipped: int,
    error: str | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO pull_task_pages(
            run_id, stream_id, page_index, page_token, page_file, status,
            events_count, ledger_inserted, applied_count, quarantined_count, skipped_count,
            error, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            stream_id,
            page_index,
            page_token,
            page_file,
            status,
            events_count,
            ledger_inserted,
            applied,
            quarantined,
            skipped,
            error,
            _now(),
        ),
    )
    return int(cur.lastrowid)


def list_task_pages(conn: sqlite3.Connection, run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, run_id, stream_id, page_index, page_token, page_file, status,
               events_count, ledger_inserted, applied_count, quarantined_count, skipped_count,
               error, created_at
        FROM pull_task_pages WHERE run_id=? ORDER BY page_index
        """,
        (run_id,),
    ).fetchall()
    return [dict(r) for r in rows]
