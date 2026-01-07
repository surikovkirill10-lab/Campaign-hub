"""Routes for the directory (campaign/mail rule mappings)."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import subprocess

import yaml
from fastapi import APIRouter, Request, Body, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text

from ..database import engine

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))

CONFIG_PATH = Path("config.yaml")
BASE_DIR = Path(__file__).resolve().parents[2]
MP_DATA_DIR = BASE_DIR / "data" / "mediaplanner"


def _load_cfg() -> dict:
    try:
        return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_cfg(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


# --- campaigns: единый источник истины через engine ---

def _ensure_campaigns_table() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id   INTEGER PRIMARY KEY,
                name TEXT
            );
        """))


def _list_campaigns():
    _ensure_campaigns_table()
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT id, COALESCE(name,'') AS name FROM campaigns")
        ).fetchall()
    return [SimpleNamespace(id=int(r[0]), name=(r[1] or "")) for r in rows]


@router.get("/directory", response_class=HTMLResponse)
def view_directory(request: Request):
    # --- параметры фильтрации/сортировки из query ---
    params = request.query_params
    q = (params.get("q") or "").strip()
    sort = params.get("sort") or "id"
    dir_ = params.get("dir") or "desc"
    reverse = (dir_ != "asc")

    # --- кампании (теперь из engine, а не из crud/get_db) ---
    campaigns = _list_campaigns()

    # поиск по ID / имени кампании
    q_lower = q.lower()
    q_id = None
    if q:
        try:
            q_id = int(q)
        except ValueError:
            q_id = None

        def camp_matches(c):
            # по ID
            if q_id is not None and getattr(c, "id", None) == q_id:
                return True
            # по имени
            name = (getattr(c, "name", "") or "").lower()
            return q_lower in name

        campaigns = [c for c in campaigns if camp_matches(c)]

    # сортировка кампаний
    if sort == "name":
        campaigns.sort(
            key=lambda c: (getattr(c, "name", "") or "").lower(),
            reverse=reverse,
        )
    else:  # sort == "id" или что-то левое — по умолчанию ID
        campaigns.sort(
            key=lambda c: getattr(c, "id", 0) or 0,
            reverse=reverse,
        )

    # --- Yandex map как было ---
    try:
        cfg = _load_cfg()
        yc = cfg.get("yandex_campaigns") or []
        yandex_map = {
            int(item["id"]): item.get("yandex_name")
            for item in yc
            if "id" in item
        }
    except Exception:
        yandex_map = {}

    # биндинги кампаний верификатора
    verifier_map = {}
    # список групп и их биндингов
    groups = []
    group_bindings = {}

    with engine.begin() as conn:
        # campaigns bindings
        try:
            rows = conn.execute(text("""
                SELECT campaign_id, provider, verifier_name, subject_mode,
                       COALESCE(filename_pattern,''), filename_mode
                FROM verifier_campaigns
                WHERE active=1
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

        # groups list
        try:
            gs = conn.execute(
                text("SELECT id, COALESCE(name,'') FROM campaign_groups")
            ).fetchall()
            groups = [{"id": int(g[0]), "name": g[1]} for g in gs]
        except Exception:
            groups = []

        # group bindings
        try:
            gb = conn.execute(text("""
                SELECT group_id, provider, subject_pattern, subject_mode,
                       COALESCE(filename_pattern,''), filename_mode
                FROM verifier_group_bindings
                WHERE active=1
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

    # --- фильтрация / сортировка групп по тем же правилам ---
    if q:
        def group_matches(g):
            if q_id is not None and g["id"] == q_id:
                return True
            return q_lower in (g["name"] or "").lower()

        groups = [g for g in groups if group_matches(g)]

    if sort == "name":
        groups.sort(key=lambda g: (g["name"] or "").lower(), reverse=reverse)
    else:
        groups.sort(key=lambda g: g["id"], reverse=reverse)

    # --- рендер шаблона ---
    return templates.TemplateResponse(
        "directory.html",
        {
            "request": request,
            "campaigns": campaigns,
            "yandex_map": yandex_map,
            "verifier_map": verifier_map,
            "groups": groups,
            "group_bindings": group_bindings,
            # чтобы в форме отрисовывались текущие значения фильтров
            "q": q,
            "sort": sort,
            "dir": dir_,
        },
    )


# --- NEW: Media planner settings page ---

@router.get("/directory/mediaplanner", response_class=HTMLResponse)
def view_mediaplanner_settings(request: Request) -> HTMLResponse:
    cfg = _load_cfg()
    mp_cfg = cfg.get("mediaplanner") or {}
    return templates.TemplateResponse(
        "directory_mediaplanner.html",
        {
            "request": request,
            "mp": mp_cfg,
        },
    )


@router.post("/directory/mediaplanner/upload_capacity", response_class=HTMLResponse)
async def upload_capacity(file: UploadFile = File(...)) -> HTMLResponse:
    MP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = MP_DATA_DIR / "capacity.xlsx"

    data = await file.read()
    dest.write_bytes(data)

    cfg = _load_cfg()
    mp_cfg = cfg.get("mediaplanner") or {}
    mp_cfg["capacity_path"] = str(dest)
    cfg["mediaplanner"] = mp_cfg
    _save_cfg(cfg)

    return HTMLResponse('<span class="tag is-success is-light">Файл ёмкости обновлён</span>')


@router.post("/directory/mediaplanner/upload_template", response_class=HTMLResponse)
async def upload_template(file: UploadFile = File(...)) -> HTMLResponse:
    MP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = MP_DATA_DIR / "media_plan_template.xlsx"

    data = await file.read()
    dest.write_bytes(data)

    cfg = _load_cfg()
    mp_cfg = cfg.get("mediaplanner") or {}
    mp_cfg["template_path"] = str(dest)
    cfg["mediaplanner"] = mp_cfg
    _save_cfg(cfg)

    return HTMLResponse('<span class="tag is-success is-light">Шаблон медиаплана обновлён</span>')


@router.post("/directory/mediaplanner/save_keys", response_class=HTMLResponse)
async def save_mediaplanner_keys(
    api_key: str = Form(""),
    folder_id: str = Form(""),
    endpoint: str = Form(""),
) -> HTMLResponse:
    cfg = _load_cfg()
    mp_cfg = cfg.get("mediaplanner") or {}

    if api_key:
        mp_cfg["yandex_api_key"] = api_key.strip()
    if folder_id:
        mp_cfg["yandex_folder_id"] = folder_id.strip()
    if endpoint:
        mp_cfg["yandex_endpoint"] = endpoint.strip()

    cfg["mediaplanner"] = mp_cfg
    _save_cfg(cfg)

    return HTMLResponse('<span class="tag is-success is-light">Ключи сохранены</span>')


@router.post("/directory/yandex/update")
async def save_yandex_name(payload: dict = Body(...)):
    """
    Сохранение yandex_name для кампании в config.yaml.
    Ждём JSON: { "id": <int>, "yandex_name": "<строка>" }
    """
    try:
        campaign_id = int(payload.get("id"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "invalid id"}, status_code=400)

    yandex_name = (payload.get("yandex_name") or "").strip()

    cfg = _load_cfg()
    yc = cfg.get("yandex_campaigns") or []

    # выкидываем старую запись с этим id
    new_list = []
    for item in yc:
        try:
            existing_id = int(item.get("id"))
        except (TypeError, ValueError):
            new_list.append(item)
            continue

        if existing_id != campaign_id:
            new_list.append(item)

    # если что‑то ввели — добавляем/обновляем
    if yandex_name:
        new_list.append({"id": campaign_id, "yandex_name": yandex_name})

    cfg["yandex_campaigns"] = new_list
    _save_cfg(cfg)

    return {"ok": True}
