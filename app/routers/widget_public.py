from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.services import widget_service

templates = Jinja2Templates(directory="app/templates")

router = APIRouter(prefix="/widget", tags=["widget"])


class InitWidgetRequest(BaseModel):
    site_token: str
    article_id: Optional[str] = None
    page_url: Optional[str] = None


class WidgetEventIn(BaseModel):
    session_token: str
    event_type: str
    video_time: Optional[float] = None
    meta: Optional[Dict[str, Any]] = None


@router.post("/init")
async def widget_init(
    payload: InitWidgetRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        result = widget_service.init_widget_session(
            db,
            site_token=payload.site_token,
            article_id=payload.article_id,
            page_url=payload.page_url or str(request.headers.get("Referer", "")),
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("User-Agent"),
            referer=request.headers.get("Referer"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return result


@router.get("/frame", response_class=HTMLResponse)
async def widget_frame(
    request: Request,
    session: str,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    try:
        ctx = widget_service.get_iframe_context(db, session_token=session)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    video = ctx["video"]
    player_config = ctx["player_config"]

    return templates.TemplateResponse(
        "widget/player_iframe.html",
        {
            "request": request,
            "session_token": session,
            "video_src": video.src_url,
            "video_poster": video.poster_url,
            "video_title": video.title,
            "player_config": player_config,
            "player_config_json": ctx["player_config_json"],
            "event_endpoint": request.url_for("widget_event"),
        },
    )


@router.post("/event")
async def widget_event(
    payload: WidgetEventIn,
    db: Session = Depends(get_db),
) -> Dict[str, str]:
    try:
        widget_service.register_widget_event(
            db,
            session_token=payload.session_token,
            event_type=payload.event_type,
            video_time=payload.video_time,
            meta=payload.meta,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return {"status": "ok"}
