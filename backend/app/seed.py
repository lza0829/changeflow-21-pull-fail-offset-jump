from app.db import connect

_DDL = """
CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS streams(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    fixture_path TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    apply_cursor INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    note TEXT,
    pull_source_id INTEGER,
    pull_position INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS apply_runs(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    kind TEXT NOT NULL DEFAULT 'apply',
    status TEXT NOT NULL,
    loaded_count INTEGER NOT NULL,
    applied_count INTEGER NOT NULL,
    quarantined_count INTEGER NOT NULL,
    skipped_count INTEGER NOT NULL,
    cursor_before INTEGER NOT NULL,
    cursor_after INTEGER NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS change_ledger(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    run_id INTEGER,
    seq INTEGER NOT NULL,
    op TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    event_ts TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    disposition TEXT NOT NULL,
    page_token TEXT,
    UNIQUE(stream_id, seq, run_id)
);
CREATE TABLE IF NOT EXISTS sink_records(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    entity_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    last_seq INTEGER NOT NULL,
    last_event_ts TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(stream_id, entity_key)
);
CREATE TABLE IF NOT EXISTS quarantine(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    entity_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_seqs(
    stream_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    applied_at TEXT NOT NULL,
    PRIMARY KEY(stream_id, seq)
);
CREATE TABLE IF NOT EXISTS pull_sources(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    manifest_path TEXT NOT NULL UNIQUE,
    note TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pull_task_pages(
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    page_index INTEGER NOT NULL,
    page_token TEXT NOT NULL,
    page_file TEXT NOT NULL,
    status TEXT NOT NULL,
    events_count INTEGER NOT NULL DEFAULT 0,
    ledger_inserted INTEGER NOT NULL DEFAULT 0,
    applied_count INTEGER NOT NULL DEFAULT 0,
    quarantined_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL
);
"""


def _ensure_column(conn, table: str, column: str, ddl: str) -> None:
    """老库原地加列（幂等），让已有数据目录升级后不丢位点与历史。"""
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _migrate(conn) -> None:
    _ensure_column(conn, "streams", "pull_source_id", "pull_source_id INTEGER")
    _ensure_column(conn, "streams", "pull_position", "pull_position INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "apply_runs", "kind", "kind TEXT NOT NULL DEFAULT 'apply'")
    _ensure_column(conn, "change_ledger", "page_token", "page_token TEXT")


def _ensure_source(conn, name: str, manifest_path: str, note: str) -> int:
    row = conn.execute(
        "SELECT id FROM pull_sources WHERE manifest_path=?", (manifest_path,)
    ).fetchone()
    if row:
        return int(row["id"])
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    cur = conn.execute(
        "INSERT INTO pull_sources(name, manifest_path, note, created_at) VALUES (?,?,?,?)",
        (name, manifest_path, note, now),
    )
    return int(cur.lastrowid)


def _ensure_stream(
    conn,
    *,
    name: str,
    fixture_path: str,
    entity_type: str,
    note: str,
    pull_source_id: int | None = None,
) -> None:
    row = conn.execute("SELECT id, pull_source_id FROM streams WHERE name=?", (name,)).fetchone()
    if row:
        if pull_source_id is not None and row["pull_source_id"] is None:
            conn.execute(
                "UPDATE streams SET pull_source_id=? WHERE id=?", (pull_source_id, row["id"])
            )
        return
    conn.execute(
        """
        INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note, pull_source_id, pull_position)
        VALUES (?,?,?,0,1,?,?,0)
        """,
        (name, fixture_path, entity_type, note, pull_source_id),
    )


def init_db():
    conn = connect()
    conn.executescript(_DDL)
    _migrate(conn)
    if conn.execute("SELECT COUNT(*) c FROM streams").fetchone()["c"] == 0:
        conn.executemany(
            """
            INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    "Orders CDC",
                    "orders_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "clean：seq 1→5 连续；含一次 delete",
                ),
                (
                    "Inventory CDC",
                    "inventory_cdc.jsonl",
                    "inventory",
                    0,
                    1,
                    "clean：seq 1→3 连续库存变更",
                ),
            ],
        )
        conn.execute("INSERT INTO settings(key, value) VALUES ('project_label', 'Changeflow')")
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('gap_policy', 'stop_quarantine')"
        )
    # 分页拉取种子：本机分页源 + 绑定位点的演示流（幂等）
    orders_src = _ensure_source(
        conn,
        "Orders 分页源",
        "orders_pages/manifest.json",
        "3 页（p1/p2/p3），seq 1→9，含 delete；演示连续拉完与跳页失败续拉",
    )
    inventory_src = _ensure_source(
        conn,
        "Inventory 分页源",
        "inventory_pages/manifest.json",
        "2 页（p1/p2），seq 1→5，库存变更",
    )
    _ensure_stream(
        conn,
        name="Orders CDC（分页拉取）",
        fixture_path="orders_pages/page1.jsonl",
        entity_type="order",
        note="绑定 Orders 分页源，位点从 0 开始逐页拉取",
        pull_source_id=orders_src,
    )
    _ensure_stream(
        conn,
        name="Inventory CDC（分页拉取）",
        fixture_path="inventory_pages/page1.jsonl",
        entity_type="inventory",
        note="绑定 Inventory 分页源，位点从 0 开始逐页拉取",
        pull_source_id=inventory_src,
    )
    conn.commit()
    conn.close()
