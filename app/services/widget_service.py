import json
import secrets
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app import models_widget as wm

DEFAULT_PLAYER_CONFIG: Dict[str, Any] = {
    "autoplay": True,
    "muted": True,
    "width": 400,
    "height": 700,
}


def _pick_placement_for_site(
    db: Session,
    site: wm.PublisherSite,
    article_id: Optional[str],
    page_url: Optional[str],
) -> Optional[wm.WidgetPlacement]:
    query = (
        db.query(wm.WidgetPlacement)
        .filter(wm.WidgetPlacement.site_id == site.id)
        .filter(wm.WidgetPlacement.status == "active")
    )

    if article_id:
        placement = query.filter(
            wm.WidgetPlacement.external_article_id == article_id
        ).first()
        if placement:
            return placement

    if page_url:
        candidates = query.filter(wm.WidgetPlacement.page_url_pattern.isnot(None)).all()
        for candidate in candidates:
            if candidate.page_url_pattern and candidate.page_url_pattern in page_url:
                return candidate

    return query.first()


def init_widget_session(
    db: Session,
    *,
    site_token: str,
    article_id: Optional[str],
    page_url: Optional[str],
    client_ip: Optional[str],
    user_agent: Optional[str],
    referer: Optional[str],
) -> Dict[str, Any]:
    site = (
        db.query(wm.PublisherSite)
        .filter(wm.PublisherSite.public_token == site_token)
        .filter(wm.PublisherSite.is_active.is_(True))
        .first()
    )
    if not site:
        raise ValueError("Unknown or inactive site token")

    placement = _pick_placement_for_site(db, site, article_id, page_url)
    if not placement:
        raise ValueError("No active placement for this site/article/page")

    video = placement.video
    if not video or not video.is_active:
        raise ValueError("No active video for this placement")

    session_token = secrets.token_urlsafe(16)

    session = wm.WidgetSession(
        session_token=session_token,
        placement_id=placement.id,
        publisher_id=site.publisher_id,
        site_id=site.id,
        page_url=page_url,
        article_id=article_id,
        client_ip=client_ip,
        user_agent=user_agent,
        referer=referer,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    return {
        "session_token": session_token,
        "frame_url": "/widget/frame?session=" + session_token,
        "player_config": DEFAULT_PLAYER_CONFIG,
    }


def get_iframe_context(db: Session, session_token: str) -> Dict[str, Any]:
    session = (
        db.query(wm.WidgetSession)
        .filter(wm.WidgetSession.session_token == session_token)
        .first()
    )
    if not session:
        raise ValueError("Unknown session token")

    placement = session.placement
    video = placement.video

    player_config = dict(DEFAULT_PLAYER_CONFIG)
    if placement.config_json:
        try:
            cfg = json.loads(placement.config_json)
            if isinstance(cfg, dict):
                player_config.update(cfg)
        except Exception:
            pass

    return {
        "session": session,
        "placement": placement,
        "video": video,
        "player_config": player_config,
        "player_config_json": json.dumps(player_config),
    }


def register_widget_event(
    db: Session,
    *,
    session_token: str,
    event_type: str,
    video_time: Optional[float],
    meta: Optional[Dict[str, Any]],
) -> int:
    session = (
        db.query(wm.WidgetSession)
        .filter(wm.WidgetSession.session_token == session_token)
        .first()
    )
    if not session:
        raise ValueError("Unknown session token")

    meta_json = json.dumps(meta) if meta else None

    event = wm.WidgetEvent(
        session_id=session.id,
        event_type=event_type,
        video_time=video_time,
        meta_json=meta_json,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event.id
