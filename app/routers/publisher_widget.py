from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.services import publisher_service

# предполагаю, что в корне есть auth.py с get_current_user
from auth import get_current_user

templates = Jinja2Templates(directory="app/templates")

router = APIRouter(prefix="/pub", tags=["publishers"])


def _get_user_id(user: Any) -> Optional[int]:
    """Аккуратно достаём id из объекта или dict."""
    if user is None:
        return None
    uid = getattr(user, "id", None)
    if uid is None and isinstance(user, dict):
        uid = user.get("id")
    return uid


def require_publisher(user: Any = Depends(get_current_user)) -> Any:
    # здесь можно добавить свою проверку роли (publisher),
    # пока просто проверяем, что пользователь залогинен
    user_id = _get_user_id(user)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


@router.get("/dashboard", name="publisher_dashboard")
async def publisher_dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: Any = Depends(require_publisher),
):
    user_id = _get_user_id(current_user)
    publisher = publisher_service.get_publisher_by_user_id(db, user_id)
    if not publisher:
        raise HTTPException(status_code=403, detail="Publisher profile not configured")

    metrics = publisher_service.get_publisher_dashboard_data(db, publisher.id)

    return templates.TemplateResponse(
        "publishers/dashboard.html",
        {
            "request": request,
            "publisher": publisher,
            "metrics": metrics,
        },
    )


@router.get("/sites", name="publisher_sites")
async def publisher_sites(
    request: Request,
    db: Session = Depends(get_db),
    current_user: Any = Depends(require_publisher),
):
    user_id = _get_user_id(current_user)
    publisher = publisher_service.get_publisher_by_user_id(db, user_id)
    if not publisher:
        raise HTTPException(status_code=403, detail="Publisher profile not configured")

    sites = publisher_service.get_publisher_sites(db, publisher.id)

    return templates.TemplateResponse(
        "publishers/sites.html",
        {
            "request": request,
            "publisher": publisher,
            "sites": sites,
        },
    )


@router.get("/sites/{site_id}", name="publisher_site_detail")
async def publisher_site_detail(
    site_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: Any = Depends(require_publisher),
):
    user_id = _get_user_id(current_user)
    publisher = publisher_service.get_publisher_by_user_id(db, user_id)
    if not publisher:
        raise HTTPException(status_code=403, detail="Publisher profile not configured")

    site = publisher_service.get_site_detail(db, publisher.id, site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    placements = site.placements

    return templates.TemplateResponse(
        "publishers/site_detail.html",
        {
            "request": request,
            "publisher": publisher,
            "site": site,
            "placements": placements,
        },
    )


@router.get("/reports", name="publisher_reports")
async def publisher_reports(
    request: Request,
    db: Session = Depends(get_db),
    current_user: Any = Depends(require_publisher),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    user_id = _get_user_id(current_user)
    publisher = publisher_service.get_publisher_by_user_id(db, user_id)
    if not publisher:
        raise HTTPException(status_code=403, detail="Publisher profile not configured")

    df: Optional[datetime] = None
    dt: Optional[datetime] = None

    if date_from:
        df = datetime.fromisoformat(date_from)
    if date_to:
        dt = datetime.fromisoformat(date_to)

    report = publisher_service.get_basic_report(db, publisher.id, df, dt)

    return templates.TemplateResponse(
        "publishers/reports.html",
        {
            "request": request,
            "publisher": publisher,
            "report": report,
            "date_from": date_from,
            "date_to": date_to,
        },
    )
