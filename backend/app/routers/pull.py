from fastapi import APIRouter, HTTPException

from app.schemas.entities import PullBind, PullRequest, PullSourceCreate
from app.services import pull_service

router = APIRouter(tags=["pull"])


@router.get("/pull/sources")
def list_sources():
    return pull_service.list_sources()


@router.post("/pull/sources")
def create_source(body: PullSourceCreate):
    try:
        return pull_service.register_source(body.name, body.manifest_path, body.note)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/pull/tasks")
def list_tasks(stream_id: int | None = None, limit: int = 50):
    return pull_service.list_pull_tasks(stream_id=stream_id, limit=limit)


@router.get("/pull/tasks/{run_id}")
def task_detail(run_id: int):
    try:
        return pull_service.get_pull_task(run_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/streams/{stream_id}/pull")
def pull_state(stream_id: int):
    try:
        return pull_service.get_pull_state(stream_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.patch("/streams/{stream_id}/pull")
def bind(stream_id: int, body: PullBind):
    try:
        return pull_service.bind_stream(stream_id, body.source_id, body.position)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/streams/{stream_id}/pull")
def do_pull(stream_id: int, body: PullRequest):
    try:
        return pull_service.pull_stream(stream_id, body.mode)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(400, str(exc)) from exc
