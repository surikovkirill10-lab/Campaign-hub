from __future__ import annotations
from pathlib import Path
from typing import List
from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["logs"])
templates = Jinja2Templates(directory="app/templates")
LOG_PATH = Path("logs/app.log")

def _tail(path: Path, n: int) -> List[str]:
    if not path.exists(): return []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return lines[-n:]
    except Exception:
        return []

@router.get("/logs", response_class=HTMLResponse)
def logs_page(request: Request, n: int = Query(300, ge=10, le=5000), q: str = Query("")) -> HTMLResponse:
    """Полная страница логов (layout)."""
    lines = _tail(LOG_PATH, n)
    if q: lines = [ln for ln in lines if q.lower() in ln.lower()]
    return templates.TemplateResponse("logs.html", {"request": request, "lines": lines, "n": n, "q": q})

@router.get("/logs/fragment", response_class=HTMLResponse)
def logs_fragment(request: Request, n: int = Query(300, ge=10, le=5000), q: str = Query("")) -> HTMLResponse:
    """Только тело логов для авто-обновления HTMX."""
    lines = _tail(LOG_PATH, n)
    if q: lines = [ln for ln in lines if q.lower() in ln.lower()]
    return templates.TemplateResponse("partials/log_body.html", {"request": request, "lines": lines})
