from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.apply_rules import classify_event
from app.engines.page_source import load_manifest, resolve_page_file
from app.engines.parse_change import parse_change_fixture
from app.repositories import cursors as cursor_repo
from app.repositories import ledger as ledger_repo
from app.repositories import pull as pull_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import settings as settings_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo

# 写明的页级成功边界：
# 一页内全部事件先入账本（disposition=pulled），再逐条走现有 Apply 裁决；
# 当且仅当页内事件全部裁决为 applied入 applied / skipped_duplicate（零隔离、零解析/校验错误），
# 该页记为 pulled 并推进位点（pull_position = page_index + 1）。
# 任一事件被隔离（缺口）或页文件解析失败 → 该页 failed，位点停在该页、不得跨过，
# 之后可用同一入口从该位点续拉（已应用事件按幂等跳过）。
SUCCESS_BOUNDARY = (
    "页内全部事件入账且裁决均为 applied/skipped_duplicate（零隔离、零解析错误）"
    "→ 该页成功并推进位点；任一事件被隔离或页面解析失败 → 该页失败，位点停在该页"
)


def _manifest_for_source(source: dict[str, Any]) -> dict[str, Any]:
    return load_manifest(FIXTURES_DIR, source["manifest_path"])


def list_sources() -> list[dict[str, Any]]:
    conn = connect()
    try:
        sources = pull_repo.list_sources(conn)
    finally:
        conn.close()
    for src in sources:
        try:
            manifest = _manifest_for_source(src)
            src["page_count"] = len(manifest["pages"])
            src["pages"] = manifest["pages"]
            src["error"] = None
        except Exception as exc:  # noqa: BLE001
            src["page_count"] = 0
            src["pages"] = []
            src["error"] = str(exc)
    return sources


def register_source(name: str, manifest_path: str, note: str | None = None) -> dict[str, Any]:
    # 登记前先验证清单可读、页结构合法（且路径不越出 fixtures 根目录）
    manifest = load_manifest(FIXTURES_DIR, manifest_path)
    conn = connect()
    try:
        existing = pull_repo.list_sources(conn)
        for src in existing:
            if src["manifest_path"] == manifest_path:
                raise ValueError(f"source already registered: {manifest_path}")
        source_id = pull_repo.insert_source(conn, name=name, manifest_path=manifest_path, note=note)
        conn.commit()
        source = pull_repo.get_source(conn, source_id) or {}
        source["page_count"] = len(manifest["pages"])
        source["pages"] = manifest["pages"]
        return source
    finally:
        conn.close()


def get_pull_state(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        state: dict[str, Any] = {
            "stream": stream,
            "source": None,
            "position": int(stream["pull_position"] or 0),
            "pages": [],
            "next_page": None,
            "exhausted": False,
            "error": None,
        }
        if stream["pull_source_id"]:
            source = pull_repo.get_source(conn, int(stream["pull_source_id"]))
            state["source"] = source
            if source:
                try:
                    pages = _manifest_for_source(source)["pages"]
                    pos = state["position"]
                    state["pages"] = [
                        {
                            **page,
                            "index": idx,
                            "status": "pulled" if idx < pos else ("next" if idx == pos else "pending"),
                        }
                        for idx, page in enumerate(pages)
                    ]
                    state["exhausted"] = pos >= len(pages)
                    if not state["exhausted"]:
                        state["next_page"] = {**pages[pos], "index": pos}
                except Exception as exc:  # noqa: BLE001
                    state["error"] = str(exc)
        return state
    finally:
        conn.close()


def bind_stream(
    stream_id: int, source_id: int | None = None, position: int | None = None
) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        new_source_id = source_id if source_id is not None else stream["pull_source_id"]
        if new_source_id is None:
            raise ValueError("stream 未绑定分页源")
        source = pull_repo.get_source(conn, int(new_source_id))
        if not source:
            raise ValueError("pull source not found")
        pages = _manifest_for_source(source)["pages"]  # 校验清单可读
        if position is None:
            # 换源默认位点归零；只改位点则保持源不动
            position = (
                0
                if source_id is not None and int(source_id) != int(stream["pull_source_id"] or 0)
                else int(stream["pull_position"] or 0)
            )
        position = int(position)
        if position < 0 or position > len(pages):
            raise ValueError(f"position 越界：允许 0..{len(pages)}（= 页数表示已拉完）")
        streams_repo.set_pull_binding(conn, stream_id, int(new_source_id), position)
        conn.commit()
    finally:
        conn.close()
    return get_pull_state(stream_id)


def _apply_page_events(
    conn,
    *,
    stream_id: int,
    run_id: int,
    events: list[dict[str, Any]],
    ledger_ids: list[int],
    cursor: int,
    gap_policy: str,
) -> tuple[int, dict[str, int], str | None]:
    """整页入账后，逐条走与现有 Apply 完全相同的裁决与落库路径。

    不绕开隔离直写 Sink：每个事件都过 classify_event，缺口照样进隔离区。
    返回 (新游标, 页内计数, 页级错误)。
    """
    counts = {"applied": 0, "quarantined": 0, "skipped": 0}
    page_error: str | None = None
    for event, ledger_id in zip(events, ledger_ids):
        seq = int(event["seq"])
        already = cursor_repo.is_applied(conn, stream_id, seq)
        decision = classify_event(
            event=event, cursor=cursor, already_applied=already, gap_policy=gap_policy
        )
        if decision == "skipped_duplicate":
            ledger_repo.update_disposition(conn, ledger_id, "skipped_duplicate")
            counts["skipped"] += 1
            continue
        if decision == "quarantine_gap":
            ledger_repo.update_disposition(conn, ledger_id, "quarantine_gap")
            if event["op"] == "upsert":
                sink_repo.upsert(
                    conn,
                    stream_id=stream_id,
                    entity_key=event["entity_key"],
                    payload=event.get("payload") or {},
                    seq=seq,
                    event_ts=event["event_ts"],
                )
            else:
                sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
            quarantine_repo.insert(
                conn,
                stream_id=stream_id,
                run_id=run_id,
                seq=seq,
                entity_key=event["entity_key"],
                reason="gap_before_seq",
                payload=event.get("payload") or {},
            )
            counts["quarantined"] += 1
            page_error = f"gap_before_seq: seq {seq} quarantined"
            break
        if event["op"] == "upsert":
            sink_repo.upsert(
                conn,
                stream_id=stream_id,
                entity_key=event["entity_key"],
                payload=event.get("payload") or {},
                seq=seq,
                event_ts=event["event_ts"],
            )
        else:
            sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
        cursor_repo.mark_applied(conn, stream_id, seq, run_id)
        cursor = seq
        streams_repo.set_cursor(conn, stream_id, cursor)
        ledger_repo.update_disposition(conn, ledger_id, "applied")
        counts["applied"] += 1
    return cursor, counts, page_error


def pull_stream(stream_id: int, mode: str = "one") -> dict[str, Any]:
    """按当前位点拉页：mode=one 拉一页，mode=all 连续拉到完或失败为止。

    每页：解析 → 整页入账（ledger disposition=pulled）→ 现有 Apply 裁决 →
    达到成功边界才推进位点并提交；失败页位点不动，可从该位点续拉。
    """
    if mode not in ("one", "all"):
        raise ValueError("mode 必须是 one 或 all")
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")
        if not stream["pull_source_id"]:
            raise ValueError("stream 未绑定分页源，请先在拉取台绑定源与位点")
        source = pull_repo.get_source(conn, int(stream["pull_source_id"]))
        if not source:
            raise ValueError("pull source not found")
        pages = _manifest_for_source(source)["pages"]
        position = int(stream["pull_position"] or 0)
        if position < 0 or position > len(pages):
            raise ValueError(f"保存的位点越界: {position}（页数 {len(pages)}）")

        cursor_before = int(stream["apply_cursor"] or 0)
        gap_policy = settings_repo.get(conn, "gap_policy", "stop_quarantine") or "stop_quarantine"
        run_id = runs_repo.create_run(conn, stream_id, cursor_before, kind="pull")
        conn.commit()

        cursor = cursor_before
        pos = position
        failed_page: dict[str, Any] | None = None
        page_summaries: list[dict[str, Any]] = []
        totals = {"events": 0, "ledger": 0, "applied": 0, "quarantined": 0, "skipped": 0}

        while pos < len(pages):
            page = pages[pos]
            summary: dict[str, Any] = {
                "index": pos,
                "page_token": page["page_token"],
                "file": page["file"],
            }
            try:
                page_file = resolve_page_file(FIXTURES_DIR, source["manifest_path"], page["file"])
                events = parse_change_fixture(page_file)
                seqs = [int(e["seq"]) for e in events]
                if len(set(seqs)) != len(seqs):
                    raise ValueError("duplicate seq within page")
            except Exception as exc:  # 页文件缺失/坏行 → 该页失败，位点不动
                summary.update(
                    status="failed", events=0, ledger_inserted=0,
                    applied=0, quarantined=0, skipped=0, error=f"page_load: {exc}",
                )
                pull_repo.insert_task_page(
                    conn, run_id=run_id, stream_id=stream_id, page_index=pos,
                    page_token=page["page_token"], page_file=page["file"], status="failed",
                    events_count=0, ledger_inserted=0, applied=0, quarantined=0, skipped=0,
                    error=summary["error"],
                )
                failed_page = {"index": pos, "page_token": page["page_token"], "error": summary["error"]}
                page_summaries.append(summary)
                conn.commit()
                break

            # 入账：整页事件先落账本（disposition=pulled），再统一走 Apply 裁决
            ledger_ids = [
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event,
                    disposition="pulled", page_token=page["page_token"],
                )
                for event in events
            ]

            cursor, counts, page_error = _apply_page_events(
                conn,
                stream_id=stream_id,
                run_id=run_id,
                events=events,
                ledger_ids=ledger_ids,
                cursor=cursor,
                gap_policy=gap_policy,
            )
            totals["events"] += len(events)
            totals["ledger"] += len(ledger_ids)
            totals["applied"] += counts["applied"]
            totals["quarantined"] += counts["quarantined"]
            totals["skipped"] += counts["skipped"]
            summary.update(
                events=len(events), ledger_inserted=len(ledger_ids),
                applied=counts["applied"], quarantined=counts["quarantined"],
                skipped=counts["skipped"],
            )

            if page_error is not None:
                # 页中途失败：部分进展落盘，但位点不得跨过失败页
                summary.update(status="failed", error=page_error)
                pull_repo.insert_task_page(
                    conn, run_id=run_id, stream_id=stream_id, page_index=pos,
                    page_token=page["page_token"], page_file=page["file"], status="failed",
                    events_count=len(events), ledger_inserted=len(ledger_ids),
                    applied=counts["applied"], quarantined=counts["quarantined"],
                    skipped=counts["skipped"], error=page_error,
                )
                failed_page = {"index": pos, "page_token": page["page_token"], "error": page_error}
                page_summaries.append(summary)
                pos += 1
                streams_repo.set_pull_position(conn, stream_id, pos)
                conn.commit()
                break

            # 达到成功边界 → 页记为 pulled，推进位点并提交（每页一个批次）
            summary.update(status="pulled", error=None)
            pull_repo.insert_task_page(
                conn, run_id=run_id, stream_id=stream_id, page_index=pos,
                page_token=page["page_token"], page_file=page["file"], status="pulled",
                events_count=len(events), ledger_inserted=len(ledger_ids),
                applied=counts["applied"], quarantined=counts["quarantined"],
                skipped=counts["skipped"], error=None,
            )
            pos += 1
            streams_repo.set_pull_position(conn, stream_id, pos)
            page_summaries.append(summary)
            conn.commit()
            if mode == "one":
                break

        pages_pulled = sum(1 for s in page_summaries if s["status"] == "pulled")
        status = "failed" if failed_page else "succeeded"
        report = {
            "kind": "pull",
            "mode": mode,
            "stream_id": stream_id,
            "source_id": source["id"],
            "source_name": source["name"],
            "manifest_path": source["manifest_path"],
            "success_boundary": SUCCESS_BOUNDARY,
            "gap_policy": gap_policy,
            "position_before": position,
            "position_after": pos,
            "pages_pulled": pages_pulled,
            "events_loaded": totals["events"],
            "ledger_inserted": totals["ledger"],
            "failed_page": failed_page,
            "resumable": failed_page is not None,
            "exhausted": pos >= len(pages),
            "note": None if pos < len(pages) or failed_page else ("no_more_pages" if not page_summaries else None),
            "pages": page_summaries,
            "apply_cursor_after": cursor,
            "sink_count_after": sink_repo.count_sink(conn),
            "quarantine_count_after": quarantine_repo.count_quarantine(conn),
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=totals["events"],
            applied=totals["applied"],
            quarantined=totals["quarantined"],
            skipped=totals["skipped"],
            cursor_after=cursor,
            report=report,
        )
        conn.commit()
    finally:
        conn.close()
    return get_pull_task(run_id)


def list_pull_tasks(stream_id: int | None = None, limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return runs_repo.list_runs(conn, limit=limit, kind="pull", stream_id=stream_id)
    finally:
        conn.close()


def get_pull_task(run_id: int) -> dict[str, Any]:
    """拉取任务报告：计数与页清单、账本、隔离区、Sink、流位点现场对账。"""
    conn = connect()
    try:
        run = runs_repo.get_run(conn, run_id)
        if not run or run.get("kind") != "pull":
            raise ValueError("pull task not found")
        pages = pull_repo.list_task_pages(conn, run_id)
        report = run["report"]

        ledger_total = ledger_repo.count_by_run(conn, run_id)
        ledger_applied = ledger_repo.count_by_run(conn, run_id, "applied")
        ledger_quarantined = ledger_repo.count_by_run(conn, run_id, "quarantine_gap")
        ledger_skipped = ledger_repo.count_by_run(conn, run_id, "skipped_duplicate")
        ledger_pending = ledger_repo.count_by_run(conn, run_id, "pulled")
        quarantine_rows = quarantine_repo.count_by_run(conn, run_id)
        applied_seq_rows = cursor_repo.count_applied_by_run(conn, run_id)

        pages_pulled_db = sum(1 for p in pages if p["status"] == "pulled")
        events_sum = sum(int(p["events_count"]) for p in pages)
        failed = report.get("failed_page")
        checks = {
            "拉取页数=页清单成功页数": report.get("pages_pulled") == pages_pulled_db,
            "事件条数=页清单事件合计": run["loaded_count"] == events_sum,
            "入账条数=账本行数=事件条数": ledger_total == run["loaded_count"]
            and ledger_total == report.get("ledger_inserted"),
            "applied=账本applied=applied_seqs": run["applied_count"] == ledger_applied == applied_seq_rows,
            "quarantined=账本隔离=隔离区行数": run["quarantined_count"] == ledger_quarantined == quarantine_rows,
            "位点只随成功页推进": report.get("position_after")
            == (report.get("position_before") or 0) + (report.get("pages_pulled") or 0),
            "失败页挡住位点": failed is None or report.get("position_after") == failed.get("index"),
        }
        reconcile = {
            "ledger_rows": ledger_total,
            "ledger_applied": ledger_applied,
            "ledger_quarantined": ledger_quarantined,
            "ledger_skipped": ledger_skipped,
            "ledger_pending_pulled": ledger_pending,
            "quarantine_rows": quarantine_rows,
            "applied_seq_rows": applied_seq_rows,
            "stream_pull_position": None,
            "stream_apply_cursor": None,
            "sink_rows": sink_repo.count_sink(conn),
        }
        stream = streams_repo.get_stream(conn, int(run["stream_id"]))
        if stream:
            reconcile["stream_pull_position"] = int(stream["pull_position"] or 0)
            reconcile["stream_apply_cursor"] = int(stream["apply_cursor"] or 0)
        return {**run, "pages": pages, "checks": checks, "reconcile": reconcile}
    finally:
        conn.close()
