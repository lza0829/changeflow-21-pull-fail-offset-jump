from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

_COLUMNS = "id, stream_id, kind, status, loaded_count, applied_count, quarantined_count, skipped_count, cursor_before, cursor_after, report_json, created_at"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def create_run(
    conn: sqlite3.Connection, stream_id: int, cursor_before: int, kind: str = "apply"
) -> int:
    cur = conn.execute(
        """
        INSERT INTO apply_runs(
            stream_id, kind, status, loaded_count, applied_count, quarantined_count, skipped_count,
            cursor_before, cursor_after, report_json, created_at
        ) VALUES (?,?,?,0,0,0,0,?,?,?,?)
        """,
        (stream_id, kind, "running", cursor_before, cursor_before, "{}", _now()),
    )
    return int(cur.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    loaded: int,
    applied: int,
    quarantined: int,
    skipped: int,
    cursor_after: int,
    report: dict[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE apply_runs SET status=?, loaded_count=?, applied_count=?, quarantined_count=?,
            skipped_count=?, cursor_after=?, report_json=? WHERE id=?
        """,
        (
            status,
            loaded,
            applied,
            quarantined,
            skipped,
            cursor_after,
            json.dumps(report, ensure_ascii=False),
            run_id,
        ),
    )


def _row_to_run(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["report"] = json.loads(item.pop("report_json") or "{}")
    return item


def list_runs(
    conn: sqlite3.Connection,
    limit: int = 50,
    kind: str | None = None,
    stream_id: int | None = None,
) -> list[dict[str, Any]]:
    sql = f"SELECT {_COLUMNS} FROM apply_runs"
    clauses: list[str] = []
    params: list[Any] = []
    if kind is not None:
        clauses.append("kind=?")
        params.append(kind)
    if stream_id is not None:
        clauses.append("stream_id=?")
        params.append(stream_id)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_run(r) for r in rows]


def count_runs(conn: sqlite3.Connection, kind: str | None = None) -> int:
    if kind is None:
        row = conn.execute("SELECT COUNT(*) c FROM apply_runs").fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) c FROM apply_runs WHERE kind=?", (kind,)).fetchone()
    return int(row["c"])


def get_run(conn: sqlite3.Connection, run_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        f"SELECT {_COLUMNS} FROM apply_runs WHERE id=?",
        (run_id,),
    ).fetchone()
    if not row:
        return None
    return _row_to_run(row)
