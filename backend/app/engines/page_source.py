from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _confine(base_dir: Path, rel: str) -> Path:
    """把相对路径限制在 fixtures 根目录内；拒绝绝对路径、越界 .. 与 URL。

    数据边界：只允许读本机 fixtures 目录下的文件，任何外网/外盘路径一律拒绝。
    """
    rel = str(rel or "").strip()
    if not rel:
        raise ValueError("empty path")
    if "://" in rel:
        raise ValueError(f"url not allowed (local fixtures only): {rel!r}")
    candidate = Path(rel)
    if candidate.is_absolute():
        raise ValueError(f"absolute path not allowed: {rel!r}")
    base = base_dir.resolve()
    resolved = (base / candidate).resolve()
    if resolved != base and base not in resolved.parents:
        raise ValueError(f"path escapes fixtures dir: {rel!r}")
    return resolved


def load_manifest(base_dir: Path, manifest_path: str) -> dict[str, Any]:
    """读取分页源的页清单（manifest.json）。

    页顺序由清单显式给出（page_token + file），不做目录扫描。
    """
    path = _confine(base_dir, manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"manifest missing: {manifest_path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid manifest json at {manifest_path}: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"manifest must be a json object: {manifest_path}")
    pages = obj.get("pages")
    if not isinstance(pages, list) or not pages:
        raise ValueError(f"manifest must contain a non-empty 'pages' list: {manifest_path}")
    seen: set[str] = set()
    out_pages: list[dict[str, str]] = []
    for idx, page in enumerate(pages):
        if not isinstance(page, dict) or "page_token" not in page or "file" not in page:
            raise ValueError(f"manifest page #{idx} needs 'page_token' and 'file'")
        token = str(page["page_token"])
        if token in seen:
            raise ValueError(f"duplicate page_token in manifest: {token}")
        seen.add(token)
        out_pages.append({"page_token": token, "file": str(page["file"])})
    return {
        "name": str(obj.get("name") or Path(manifest_path).parent.name),
        "entity_type": obj.get("entity_type"),
        "pages": out_pages,
    }


def resolve_page_file(base_dir: Path, manifest_path: str, page_file: str) -> Path:
    """页文件相对其 manifest 所在目录解析，同样限制在 fixtures 根目录内。"""
    manifest_dir = Path(manifest_path).parent
    return _confine(base_dir, str(manifest_dir / page_file))
