from __future__ import annotations

import json
import sqlite3
from typing import Any


def insert_ledger(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    run_id: int,
    event: dict[str, Any],
    disposition: str,
    page_token: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO change_ledger(stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition, page_token)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            run_id,
            int(event["seq"]),
            event["op"],
            event["entity_key"],
            event["event_ts"],
            json.dumps(event.get("payload") or {}, ensure_ascii=False),
            disposition,
            page_token,
        ),
    )
    return int(cur.lastrowid)


def update_disposition(conn: sqlite3.Connection, ledger_id: int, disposition: str) -> None:
    conn.execute("UPDATE change_ledger SET disposition=? WHERE id=?", (disposition, ledger_id))


def count_by_run(
    conn: sqlite3.Connection, run_id: int, disposition: str | None = None
) -> int:
    if disposition is None:
        row = conn.execute(
            "SELECT COUNT(*) c FROM change_ledger WHERE run_id=?", (run_id,)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) c FROM change_ledger WHERE run_id=? AND disposition=?",
            (run_id, disposition),
        ).fetchone()
    return int(row["c"])


def list_ledger(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition, page_token
        FROM change_ledger ORDER BY id DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        out.append(item)
    return out
