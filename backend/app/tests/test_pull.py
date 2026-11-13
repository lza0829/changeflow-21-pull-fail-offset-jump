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
