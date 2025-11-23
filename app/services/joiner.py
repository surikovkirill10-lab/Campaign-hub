"""Services for joining raw metrics into unified facts."""

from __future__ import annotations

from typing import Dict, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..models import Campaign, RawSystemDaily, RawMetricaDaily
from ..crud import upsert_fact_daily


def update_facts_for_campaign(db: Session, campaign_id: int) -> int:
    """Aggregate raw metrics for a campaign and upsert into `fact_daily`.

    Returns the number of fact rows written.
    """
    # Aggregate system metrics by date
    sys_stmt = (
        select(
            RawSystemDaily.date,
            func.sum(RawSystemDaily.impressions).label("impressions"),
            func.sum(RawSystemDaily.clicks).label("clicks"),
            func.sum(RawSystemDaily.spend).label("spend"),
        )
        .where(RawSystemDaily.campaign_id == campaign_id)
        .group_by(RawSystemDaily.date)
    )
    system_data = {row.date: row for row in db.execute(sys_stmt).fetchall()}

    # Aggregate metrica metrics by date.  ``RawMetricaDaily`` does not store
    # ``bounce_rate`` directly; instead, compute it as bounces/visits per day.
    met_stmt = (
        select(
            RawMetricaDaily.date,
            func.sum(RawMetricaDaily.visits).label("visits"),
            func.sum(RawMetricaDaily.conversions).label("conversions"),
            func.sum(RawMetricaDaily.bounces).label("bounces"),
        )
        .where(RawMetricaDaily.campaign_id == campaign_id)
        .group_by(RawMetricaDaily.date)
    )
    # Build a mapping of date -> aggregated values
    metrica_data = {}
    for row in db.execute(met_stmt).fetchall():
        # row.visits may be None if no visits recorded; avoid division by zero
        bounce_rate: Optional[float] = None
        if row.visits and row.bounces is not None and row.visits != 0:
            bounce_rate = row.bounces / row.visits
        metrica_data[row.date] = {
            "visits": row.visits,
            "conversions": row.conversions,
            "bounce_rate": bounce_rate,
        }

    # Combine dates from both sources
    all_dates = set(system_data.keys()) | set(metrica_data.keys())
    written = 0
    for dt in sorted(all_dates):
        sys = system_data.get(dt)
        met = metrica_data.get(dt)
        impressions = sys.impressions if sys else None
        clicks = sys.clicks if sys else None
        spend = sys.spend if sys else None
        visits = met.get("visits") if met else None
        conversions = met.get("conversions") if met else None
        bounce_rate = met.get("bounce_rate") if met else None
        upsert_fact_daily(
            db=db,
            campaign_id=campaign_id,
            date=dt,
            impressions=impressions,
            clicks=clicks,
            spend=spend,
            visits=visits,
            conversions=conversions,
            bounce_rate=bounce_rate,
        )
        written += 1
    return written
