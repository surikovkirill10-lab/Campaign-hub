from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app import models_widget as wm


def get_publisher_by_user_id(db: Session, user_id: int) -> Optional[wm.Publisher]:
    return (
        db.query(wm.Publisher)
        .filter(wm.Publisher.user_id == user_id)
        .filter(wm.Publisher.is_active.is_(True))
        .first()
    )


def get_publisher_sites(db: Session, publisher_id: int) -> List[wm.PublisherSite]:
    return (
        db.query(wm.PublisherSite)
        .filter(wm.PublisherSite.publisher_id == publisher_id)
        .order_by(wm.PublisherSite.id)
        .all()
    )


def get_publisher_dashboard_data(db: Session, publisher_id: int) -> Dict[str, int]:
    total_sessions = (
        db.query(func.count(wm.WidgetSession.id))
        .filter(wm.WidgetSession.publisher_id == publisher_id)
        .scalar()
        or 0
    )

    total_events = (
        db.query(func.count(wm.WidgetEvent.id))
        .join(wm.WidgetSession, wm.WidgetEvent.session_id == wm.WidgetSession.id)
        .filter(wm.WidgetSession.publisher_id == publisher_id)
        .scalar()
        or 0
    )

    last7 = datetime.utcnow() - timedelta(days=7)
    last7_sessions = (
        db.query(func.count(wm.WidgetSession.id))
        .filter(wm.WidgetSession.publisher_id == publisher_id)
        .filter(wm.WidgetSession.created_at >= last7)
        .scalar()
        or 0
    )

    return {
        "total_sessions": total_sessions,
        "total_events": total_events,
        "last7_sessions": last7_sessions,
    }


def get_site_detail(
    db: Session, publisher_id: int, site_id: int
) -> Optional[wm.PublisherSite]:
    site = (
        db.query(wm.PublisherSite)
        .filter(wm.PublisherSite.id == site_id)
        .filter(wm.PublisherSite.publisher_id == publisher_id)
        .first()
    )
    if not site:
        return None

    # простая лениво‑загружаемая коллекция placements
    for placement in site.placements:
        _ = placement.video  # прогреваем связь

    return site


def get_basic_report(
    db: Session,
    publisher_id: int,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
):
    q = (
        db.query(
            wm.PublisherSite.domain.label("domain"),
            func.count(wm.WidgetSession.id).label("sessions"),
            func.sum(
                func.case(
                    [(wm.WidgetEvent.event_type == "view_start", 1)],
                    else_=0,
                )
            ).label("views"),
            func.sum(
                func.case(
                    [(wm.WidgetEvent.event_type == "complete", 1)],
                    else_=0,
                )
            ).label("completes"),
        )
        .join(
            wm.WidgetSession,
            wm.WidgetSession.site_id == wm.PublisherSite.id,
        )
        .join(
            wm.WidgetEvent,
            wm.WidgetEvent.session_id == wm.WidgetSession.id,
        )
        .filter(wm.WidgetSession.publisher_id == publisher_id)
    )

    if date_from:
        q = q.filter(wm.WidgetSession.created_at >= date_from)
    if date_to:
        q = q.filter(wm.WidgetSession.created_at < date_to + timedelta(days=1))

    q = q.group_by(wm.PublisherSite.domain).order_by(wm.PublisherSite.domain)

    rows = []
    for row in q.all():
        rows.append(
            {
                "domain": row.domain,
                "sessions": row.sessions or 0,
                "views": row.views or 0,
                "completes": row.completes or 0,
            }
        )
    return rows
