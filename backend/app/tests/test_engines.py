from app.engines.apply_rules import classify_event, next_expected_seq


def test_next_expected():
    assert next_expected_seq(0) == 1
    assert next_expected_seq(4) == 5


def test_classify_apply_and_gap():
    event = {"seq": 1, "op": "upsert", "entity_key": "A", "event_ts": "t", "payload": {}}
    assert classify_event(event=event, cursor=0, already_applied=False) == "apply"
    gap = {"seq": 3, "op": "upsert", "entity_key": "A", "event_ts": "t", "payload": {}}
    assert classify_event(event=gap, cursor=1, already_applied=False) == "quarantine_gap"
    assert (
        classify_event(event=event, cursor=0, already_applied=True) == "skipped_duplicate"
    )


def test_run_apply_quarantined_event_never_reaches_sink(tmp_path, monkeypatch):
    """缺口隔离事件只进隔离区：账本 quarantine_gap、隔离区有行、Sink 不得写入。"""
    import json

    import app.db as db
    from app import seed
    from app.repositories import ledger as ledger_repo
    from app.repositories import quarantine as quarantine_repo
    from app.repositories import sink as sink_repo
    from app.services import apply_service

    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    fx = tmp_path / "fixtures"
    fx.mkdir()
    rows = [
        {"seq": 1, "op": "upsert", "entity_key": "A", "event_ts": "t1", "payload": {"v": 1}},
        # 缺 seq2 → seq3 形成缺口，必须被隔离
        {"seq": 3, "op": "upsert", "entity_key": "BAD", "event_ts": "t3", "payload": {"v": 3}},
    ]
    (fx / "gap.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8"
    )
    monkeypatch.setattr(apply_service, "FIXTURES_DIR", fx)
    seed.init_db()

    import app.db as dbmod
    conn = dbmod.connect()
    conn.execute("UPDATE streams SET fixture_path='gap.jsonl' WHERE id=1")
    conn.commit()
    conn.close()

    run = apply_service.run_apply(1)
    assert run["status"] == "partial"
    assert run["applied_count"] == 1
    assert run["quarantined_count"] == 1

    conn = dbmod.connect()
    try:
        ledger = {r["seq"]: r["disposition"] for r in ledger_repo.list_ledger(conn, 50)}
        quarantined = [r["seq"] for r in quarantine_repo.list_quarantine(conn)]
        sink_keys = {r["entity_key"] for r in sink_repo.list_sink(conn)}
    finally:
        conn.close()
    assert ledger[1] == "applied"
    assert ledger[3] == "quarantine_gap"
    assert quarantined == [3]
    assert sink_keys == {"A"}, "隔离事件不得直写 Sink"
