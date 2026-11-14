# 0-1 cut: changelog-offset-pull —— 本机分页拉取

import json

import pytest

import app.db as db
from app import seed
from app.config import FIXTURES_DIR
from app.engines.page_source import load_manifest, resolve_page_file
from app.services import pull_service


def _write_page(path, events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")


def _event(seq, key="A", op="upsert"):
    return {"seq": seq, "op": op, "entity_key": key, "event_ts": f"t{seq}", "payload": {"v": seq}}


@pytest.fixture()
def pull_env(tmp_path, monkeypatch):
    """隔离 DB + 隔离 fixtures：3 页源（p1: seq1-2, p2: seq3, p3: seq4）。"""
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    fx = tmp_path / "fixtures"
    (fx / "src1").mkdir(parents=True)
    _write_page(fx / "src1" / "page1.jsonl", [_event(1), _event(2, "B")])
    _write_page(fx / "src1" / "page2.jsonl", [_event(3)])
    _write_page(fx / "src1" / "page3.jsonl", [_event(4, "B", "delete")])
    (fx / "src1" / "manifest.json").write_text(
        json.dumps(
            {
                "name": "t-src",
                "pages": [
                    {"page_token": "p1", "file": "page1.jsonl"},
                    {"page_token": "p2", "file": "page2.jsonl"},
                    {"page_token": "p3", "file": "page3.jsonl"},
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pull_service, "FIXTURES_DIR", fx)
    seed.init_db()
    src = pull_service.register_source("t-src", "src1/manifest.json")
    pull_service.bind_stream(1, source_id=src["id"], position=0)
    return fx


# ---------- 引擎：清单与路径边界 ----------


def test_manifest_load_ok():
    manifest = load_manifest(FIXTURES_DIR, "orders_pages/manifest.json")
    assert [p["page_token"] for p in manifest["pages"]] == ["p1", "p2", "p3"]


def test_manifest_rejects_escape_and_url(tmp_path):
    with pytest.raises(ValueError):
        load_manifest(FIXTURES_DIR, "../outside/manifest.json")
    with pytest.raises(ValueError):
        load_manifest(FIXTURES_DIR, "https://evil.example/manifest.json")
    with pytest.raises(ValueError):
        load_manifest(FIXTURES_DIR, "/etc/passwd")
    with pytest.raises(ValueError):
        resolve_page_file(FIXTURES_DIR, "orders_pages/manifest.json", "../../secret")


def test_manifest_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_manifest(FIXTURES_DIR, "nope/manifest.json")


# ---------- 服务：连续拉完 ----------


def test_pull_all_to_completion_and_counts_reconcile(pull_env):
    task = pull_service.pull_stream(1, mode="all")
    assert task["status"] == "succeeded"
    assert task["report"]["pages_pulled"] == 3
    assert task["report"]["events_loaded"] == 4
    assert task["report"]["ledger_inserted"] == 4
    assert task["applied_count"] == 4
    assert task["quarantined_count"] == 0
    assert task["report"]["position_before"] == 0
    assert task["report"]["position_after"] == 3
    assert task["report"]["exhausted"] is True
    assert all(task["checks"].values()), task["checks"]
    assert task["reconcile"]["ledger_rows"] == 4
    assert task["reconcile"]["applied_seq_rows"] == 4

    state = pull_service.get_pull_state(1)
    assert state["position"] == 3
    assert state["exhausted"] is True
    assert state["stream"]["apply_cursor"] == 4

    # 拉完后再次拉取：0 页、不计数、位点不动
    again = pull_service.pull_stream(1, mode="all")
    assert again["status"] == "succeeded"
    assert again["report"]["pages_pulled"] == 0
    assert again["report"]["note"] == "no_more_pages"
    assert pull_service.get_pull_state(1)["position"] == 3


def test_pull_one_page_at_a_time(pull_env):
    t1 = pull_service.pull_stream(1, mode="one")
    assert t1["report"]["pages_pulled"] == 1
    assert pull_service.get_pull_state(1)["position"] == 1
    t2 = pull_service.pull_stream(1, mode="one")
    assert t2["report"]["position_before"] == 1
    assert t2["report"]["position_after"] == 2
    assert pull_service.get_pull_state(1)["stream"]["apply_cursor"] == 3


# ---------- 服务：失败页挡位点 + 续拉 ----------


def test_failed_page_holds_position_then_resume(pull_env):
    pull_service.pull_stream(1, mode="one")  # p1 成功，位点 1，游标 2
    # 故意失败：位点跳到 p3（跨过 p2），p3 的 seq4 相对游标 2 形成缺口 → 隔离 → 页失败
    pull_service.bind_stream(1, position=2)
    failed = pull_service.pull_stream(1, mode="one")
    assert failed["status"] == "failed"
    assert failed["report"]["failed_page"]["index"] == 2
    assert failed["report"]["position_after"] == 2  # 位点不得跨过失败页
    assert failed["quarantined_count"] == 1
    assert failed["report"]["resumable"] is True
    assert all(failed["checks"].values()), failed["checks"]
    assert pull_service.get_pull_state(1)["position"] == 2

    # 续拉：位点退回 p2，连续拉完 → p2、p3 全部成功
    pull_service.bind_stream(1, position=1)
    resumed = pull_service.pull_stream(1, mode="all")
    assert resumed["status"] == "succeeded"
    assert resumed["report"]["pages_pulled"] == 2
    assert resumed["report"]["position_after"] == 3
    assert resumed["applied_count"] == 2
    assert all(resumed["checks"].values()), resumed["checks"]
    state = pull_service.get_pull_state(1)
    assert state["position"] == 3
    assert state["stream"]["apply_cursor"] == 4


def test_missing_page_file_fails_then_recovers(pull_env):
    fx = pull_env
    (fx / "src1" / "page2.jsonl").unlink()
    failed = pull_service.pull_stream(1, mode="all")
    assert failed["status"] == "failed"
    assert failed["report"]["failed_page"]["index"] == 1
    assert failed["report"]["pages_pulled"] == 1  # p1 已成功并推进
    assert pull_service.get_pull_state(1)["position"] == 1

    _write_page(fx / "src1" / "page2.jsonl", [_event(3)])  # 源补发该页
    resumed = pull_service.pull_stream(1, mode="all")
    assert resumed["status"] == "succeeded"
    assert resumed["report"]["pages_pulled"] == 2
    assert pull_service.get_pull_state(1)["position"] == 3


def test_repull_is_idempotent(pull_env):
    pull_service.pull_stream(1, mode="all")
    pull_service.bind_stream(1, position=0)  # 位点回零重拉
    again = pull_service.pull_stream(1, mode="all")
    assert again["status"] == "succeeded"
    assert again["applied_count"] == 0
    assert again["skipped_count"] == 4  # 全部幂等跳过
    assert all(again["checks"].values()), again["checks"]


# ---------- 夹具：半页失败（真实 fixtures，对照位点 / 账本 / 隔离 / Sink） ----------


@pytest.fixture()
def half_page_env(tmp_path, monkeypatch):
    """使用 seed 的半页失败流：p1 成功，p2 缺 seq5 半页失败，p3 未被拉取。

    返回 (stream_id, source_id)，均来自 seed（源已登记、流已绑定，位点 0）。
    """
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    seed.init_db()
    from app.db import connect
    from app.repositories import pull as pull_repo
    from app.repositories import streams as streams_repo

    conn = connect()
    try:
        streams = streams_repo.list_streams(conn)
        sources = pull_repo.list_sources(conn)
    finally:
        conn.close()
    src = next(s for s in sources if s["manifest_path"] == "orders_half_pages/manifest.json")
    stream = next(s for s in streams if s["pull_source_id"] == src["id"])
    # 通过拉取台正常绑定（重置位点到 0），走真实入口
    pull_service.bind_stream(stream["id"], source_id=src["id"], position=0)
    return stream["id"], src["id"]


def _sink_payloads(stream_id=1):
    from app.db import connect
    from app.repositories import sink as sink_repo

    conn = connect()
    try:
        rows = sink_repo.list_sink(conn)
    finally:
        conn.close()
    return {r["entity_key"]: r["payload"] for r in rows if r["stream_id"] == stream_id}


def _quarantine_rows(stream_id=1):
    from app.db import connect
    from app.repositories import quarantine as quarantine_repo

    conn = connect()
    try:
        rows = quarantine_repo.list_quarantine(conn)
    finally:
        conn.close()
    return [r for r in rows if r["stream_id"] == stream_id]


def test_half_page_failure_holds_position_and_counts(half_page_env):
    """半页失败：位点停在失败页，报告失败态；账本半页与隔离 / Sink 一致。"""
    stream_id, _ = half_page_env
    task = pull_service.pull_stream(stream_id, mode="one")  # 只拉 p1 → 成功
    assert task["status"] == "succeeded"
    assert task["report"]["position_after"] == 1

    failed = pull_service.pull_stream(stream_id, mode="one")  # p2：seq3、4 应用，seq6 缺口隔离
    assert failed["status"] == "failed"
    report = failed["report"]
    # 1) 失败页不得推进位点越过该页
    assert report["failed_page"]["index"] == 1
    assert report["position_before"] == 1
    assert report["position_after"] == 1, "失败页不得推进位点"
    assert pull_service.get_pull_state(stream_id)["position"] == 1
    # 2) 报告失败态、持久位点、已入账条数一致
    assert report["pages_pulled"] == 0
    assert failed["applied_count"] == 2  # seq3、seq4 已应用
    assert failed["quarantined_count"] == 1  # seq6 缺口
    assert failed["skipped_count"] == 0
    assert report["events_loaded"] == 4  # 整页 4 条都入账
    assert report["ledger_inserted"] == 4
    rec = failed["reconcile"]
    assert rec["ledger_rows"] == 4
    assert rec["ledger_applied"] == 2
    assert rec["ledger_quarantined"] == 1
    assert rec["ledger_pending_pulled"] == 1  # seq7 裁决前中断，留 pulled
    assert rec["quarantine_rows"] == 1
    assert rec["applied_seq_rows"] == 2
    assert rec["stream_pull_position"] == 1
    assert rec["stream_apply_cursor"] == 4  # 连续应用到 seq4
    assert all(failed["checks"].values()), failed["checks"]
    assert report["resumable"] is True
    # 3) 坏事件不得绕开隔离进 Sink
    payloads = _sink_payloads(stream_id)
    assert "ORD-3004" not in payloads, "隔离事件（seq6）不得直写 Sink"
    assert payloads["ORD-3001"]["status"] == "paid"  # seq3 正常应用
    assert payloads["ORD-3003"]["status"] == "created"  # seq4 正常应用
    quarantined = _quarantine_rows(stream_id)
    assert [q["seq"] for q in quarantined] == [6]


def test_half_page_resume_from_failed_page(half_page_env):
    """续拉从失败页边界重试：失败页未被拉过，p3 的 seq8/9 不得提前入账。"""
    stream_id, _ = half_page_env
    pull_service.pull_stream(stream_id, mode="one")  # p1
    failed = pull_service.pull_stream(stream_id, mode="one")  # p2 半页失败
    assert failed["status"] == "failed"

    # 源修复：换绑补齐 seq5 的清单（位点仍在失败页下标 1）
    from app.db import connect
    from app.repositories import pull as pull_repo

    conn = connect()
    try:
        sources = pull_repo.list_sources(conn)
    finally:
        conn.close()
    repaired = next(
        s for s in sources
        if s["manifest_path"] == "orders_half_pages/manifest_repaired.json"
    )
    pull_service.bind_stream(stream_id, source_id=repaired["id"], position=1)

    resumed = pull_service.pull_stream(stream_id, mode="all")
    assert resumed["status"] == "succeeded"
    report = resumed["report"]
    assert report["position_before"] == 1
    assert report["position_after"] == 3
    assert report["pages_pulled"] == 2
    # 续拉只从失败页起重试；p3 的 seq8/9 只在 p2 成功后才应用
    assert report["events_loaded"] == 7  # p2 5 条 + p3 2 条
    assert resumed["applied_count"] == 5  # p2 的 seq5/6/7 新增 + p3 的 seq8/9
    assert resumed["applied_count"] + resumed["skipped_count"] == 7
    assert resumed["quarantined_count"] == 0
    assert resumed["reconcile"]["stream_pull_position"] == 3
    assert resumed["reconcile"]["stream_apply_cursor"] == 9
    assert all(resumed["checks"].values()), resumed["checks"]
    assert resumed["cursor_after"] == 9
    state = pull_service.get_pull_state(stream_id)
    assert state["position"] == 3
    assert state["exhausted"] is True


def test_half_page_failure_does_not_leak_later_page_to_ledger(half_page_env):
    """失败页挡位点：p3 在 p2 失败时根本没被拉取，账本 / 隔离 / 游标停在半页边界。"""
    stream_id, _ = half_page_env
    pull_service.pull_stream(stream_id, mode="one")  # p1 → seq1,2
    failed = pull_service.pull_stream(stream_id, mode="one")  # p2 半页失败
    assert failed["status"] == "failed"

    from app.db import connect
    from app.repositories import ledger as ledger_repo

    conn = connect()
    try:
        rows = ledger_repo.list_ledger(conn, limit=500)
    finally:
        conn.close()
    seqs = sorted(r["seq"] for r in rows if r["stream_id"] == stream_id)
    assert seqs == [1, 2, 3, 4, 6, 7], "只入账 p1 + p2 半页；p3 的 seq8/9 不得出现"
    dispositions = {r["seq"]: r["disposition"] for r in rows if r["stream_id"] == stream_id}
    assert dispositions[1] == "applied" and dispositions[2] == "applied"
    assert dispositions[3] == "applied" and dispositions[4] == "applied"
    assert dispositions[6] == "quarantine_gap"
    assert dispositions[7] == "pulled"
