"""Routes for the directory (campaign/mail rule mappings)."""
from __future__ import annotations
from pathlib import Path
from html import escape

import yaml
from fastapi import APIRouter, Depends, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..database import get_db, engine
from ..crud import get_campaigns

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))

@router.get("/directory", response_class=HTMLResponse)
def view_directory(request: Request, db: Session = Depends(get_db)):
    campaigns = get_campaigns(db)
    # Yandex из config.yaml — как было
    try:
        cfg = yaml.safe_load(open("config.yaml", "r", encoding="utf-8")) or {}
        yc = cfg.get("yandex_campaigns") or []
        yandex_map = {int(item["id"]): item.get("yandex_name") for item in yc if "id" in item}
    except Exception:
        yandex_map = {}

    # биндинги кампаний верификатора
    verifier_map = {}
    # список групп и их биндингов
    groups = []
    group_bindings = {}

    with engine.begin() as conn:
        # campaigns
        try:
            rows = conn.execute(text("""
                SELECT campaign_id, provider, verifier_name, subject_mode, COALESCE(filename_pattern,''), filename_mode
                FROM verifier_campaigns WHERE active=1
            """)).fetchall()
            for r in rows:
                verifier_map[int(r[0])] = {
                    "provider": r[1],
                    "verifier_name": r[2],
                    "subject_mode": r[3],
                    "filename_pattern": r[4],
                    "filename_mode": r[5],
                }
        except Exception:
            verifier_map = {}

        # groups
        try:
            gs = conn.execute(text("SELECT id, COALESCE(name,'') FROM campaign_groups ORDER BY id")).fetchall()
            groups = [{"id": int(g[0]), "name": g[1]} for g in gs]
        except Exception:
            groups = []

        try:
            gb = conn.execute(text("""
                SELECT group_id, provider, subject_pattern, subject_mode, COALESCE(filename_pattern,''), filename_mode
                FROM verifier_group_bindings WHERE active=1
            """)).fetchall()
            for r in gb:
                group_bindings[int(r[0])] = {
                    "provider": r[1],
                    "subject_pattern": r[2],
                    "subject_mode": r[3],
                    "filename_pattern": r[4],
                    "filename_mode": r[5],
                }
        except Exception:
            group_bindings = {}

    return templates.TemplateResponse(
        "directory.html",
        {
            "request": request,
            "campaigns": campaigns,
            "yandex_map": yandex_map,
            "verifier_map": verifier_map,
            "groups": groups,
            "group_bindings": group_bindings,
        },
    )

@router.post("/directory/yandex/update")
def update_yandex_name(payload: dict = Body(...)):
    """Обновляет секцию yandex_campaigns в config.yaml.
    payload: {"id": <int>, "yandex_name": <str>}
    """
    try:
        cid = int(payload.get("id"))
        name = str(payload.get("yandex_name", "")).strip()
        if not name:
            return JSONResponse(status_code=400, content={"error": "empty yandex_name"})

        cfg_path = "config.yaml"
        cfg = yaml.safe_load(open(cfg_path, "r", encoding="utf-8")) or {}
        yc = cfg.get("yandex_campaigns") or []

        # обновляем существующую запись либо добавляем новую
        for item in yc:
            if int(item.get("id")) == cid:
                item["yandex_name"] = name
                break
        else:
            yc.append({"id": cid, "yandex_name": name})

        cfg["yandex_campaigns"] = yc
        yaml.safe_dump(cfg, open(cfg_path, "w", encoding="utf-8"),
                       allow_unicode=True, sort_keys=False)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/directory/yandex_update", response_class=HTMLResponse)
async def campaigns_yandex_update():
    base_dir = Path(__file__).resolve().parents[2]
    script_path = base_dir / "scripts" / "yandex_import.py"

    if not script_path.exists():
        return HTMLResponse(
            f'<span class="tag is-danger is-light">Script not found</span>'
            f'<pre>{escape(str(script_path))}</pre>'
        )

    def _run():
        return subprocess.run(
            [sys.executable, "-u", str(script_path)],
            cwd=str(base_dir),
            capture_output=True,
            text=True,
            timeout=6000
        )

    try:
        res = await run_in_threadpool(_run)
    except Exception as e:
        return HTMLResponse(
            f'<span class="tag is-danger is-light">Error</span> '
            f'<small>{escape(str(e))}</small>'
        )

    if res.returncode == 0:
        return HTMLResponse('<span class="tag is-success is-light">Yandex imported</span>')

    tail = (res.stderr or res.stdout or "")[-4000:]
    return HTMLResponse(
        '<span class="tag is-danger is-light">Yandex import failed</span>'
        f'<pre>{escape(tail)}</pre>'
    )
