"""Data access layer for Campaign Hub.

This module defines a set of helper functions which encapsulate common
operations on the SQLAlchemy models.  Having a dedicated CRUD layer keeps
route handlers concise and makes unit testing easier.
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import select, insert, update, delete, func

from .models import (
    Campaign,
    MailRule,
    SourceFile,
    RawSystemDaily,
    RawMetricaDaily,
    FactDaily,
    SourceType,
)


def get_campaigns(db: Session, skip: int = 0, limit: int = 100) -> List[Campaign]:
    return db.execute(select(Campaign).offset(skip).limit(limit)).scalars().all()


def get_campaign(db: Session, campaign_id: int) -> Optional[Campaign]:
    return db.get(Campaign, campaign_id)


def create_mail_rule(db: Session, allowed_senders: list[str] | None = None,
                     subject_regex: list[str] | None = None,
                     filename_regex: list[str] | None = None,
                     folder: str | None = None,
                     date_extractors: list[dict] | None = None,
                     goal_ids: list[int] | None = None) -> MailRule:
    rule = MailRule(
        allowed_senders=allowed_senders,
        subject_regex=subject_regex,
        filename_regex=filename_regex,
        folder=folder or "INBOX",
        date_extractors=date_extractors,
        goal_ids=goal_ids,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


def create_campaign(db: Session, name: str, campaign_id: int,
                    mail_rule: MailRule | None = None, notes: str | None = None) -> Campaign:
    campaign = Campaign(id=campaign_id, name=name, mail_rule=mail_rule, notes=notes)
    db.add(campaign)
    db.commit()
    db.refresh(campaign)
    return campaign


def create_source_file(db: Session, source: SourceType, sha256: str,
                       campaign: Campaign | None, message_id: str | None,
                       sender: str | None, subject: str | None, filename: str | None,
                       period_from: date | None, period_to: date | None,
                       rows: int | None, status: str = "processed",
                       error: str | None = None) -> SourceFile:
    sf = SourceFile(
        source=source,
        campaign=campaign,
        message_id=message_id,
        sender=sender,
        subject=subject,
        filename=filename,
        sha256=sha256,
        period_from=period_from,
        period_to=period_to,
        rows=rows,
        status=status,
        error=error,
    )
    db.add(sf)
    db.commit()
    db.refresh(sf)
    return sf


def create_raw_system_daily(db: Session, file: SourceFile, campaign: Campaign,
                            records: Sequence[dict]) -> int:
    """Insert many raw system daily records.

    Each record should be a dict with keys matching the RawSystemDaily
    fields (excluding `id`, `source_file_id` and `campaign_id`).  Returns the
    number of inserted rows.
    """
    inserted = 0
    for rec in records:
        row = RawSystemDaily(
            source_file=file,
            campaign=campaign,
            date=rec.get("date"),
            impressions=rec.get("impressions"),
            clicks=rec.get("clicks"),
            spend=rec.get("spend"),
            reach=rec.get("reach"),
            frequency=rec.get("frequency"),
            ctr=rec.get("ctr"),
            view_quarter=rec.get("view_25"),
            view_half=rec.get("view_50"),
            view_three_quarters=rec.get("view_75"),
            view_full=rec.get("view_100"),
            vtr=rec.get("vtr"),
        )
        db.add(row)
        inserted += 1
    db.commit()
    return inserted


def create_raw_metrica_daily(db: Session, file: SourceFile, campaign: Campaign,
                             records: Sequence[dict]) -> int:
    inserted = 0
    for rec in records:
        row = RawMetricaDaily(
            source_file=file,
            campaign=campaign,
            date=rec.get("date"),
            visits=rec.get("visits"),
            visitors=rec.get("visitors"),
            bounces=rec.get("bounces"),
            depth=rec.get("depth"),
            time_on_site=rec.get("time_on_site"),
            conversions=rec.get("conversions"),
        )
        db.add(row)
        inserted += 1
    db.commit()
    return inserted


def upsert_fact_daily(db: Session, campaign_id: int,
                      date: date,
                      impressions: Optional[int],
                      clicks: Optional[int],
                      spend: Optional[float],
                      visits: Optional[int],
                      conversions: Optional[int],
                      bounce_rate: Optional[float]) -> FactDaily:
    """Insert or update a daily fact row and compute derived metrics."""
    # First try to load existing row
    stmt = select(FactDaily).where(FactDaily.campaign_id == campaign_id, FactDaily.date == date)
    row = db.execute(stmt).scalar_one_or_none()

    # Compute derived metrics safely
    def safe_div(numerator, denominator):
        try:
            return (numerator or 0) / (denominator or 0) if denominator else None
        except Exception:
            return None

    ctr_ext = safe_div(clicks, impressions)
    cpc = safe_div(spend, clicks)
    cpm = safe_div(spend * 1000 if spend is not None else None, impressions)
    cpa = safe_div(spend, conversions)

    # diff metrics are computed only if visits and clicks are both present
    diff_clicks = None
    diff_impressions = None

    # Update or create
    if row:
        row.impressions = impressions
        row.clicks = clicks
        row.spend = spend
        row.visits = visits
        row.conversions = conversions
        row.bounce_rate = bounce_rate
        row.ctr_ext = ctr_ext
        row.cpc = cpc
        row.cpm = cpm
        row.cpa = cpa
        row.diff_clicks = diff_clicks
        row.diff_impressions = diff_impressions
    else:
        row = FactDaily(
            campaign_id=campaign_id,
            date=date,
            impressions=impressions,
            clicks=clicks,
            spend=spend,
            visits=visits,
            conversions=conversions,
            bounce_rate=bounce_rate,
            ctr_ext=ctr_ext,
            cpc=cpc,
            cpm=cpm,
            cpa=cpa,
            diff_clicks=diff_clicks,
            diff_impressions=diff_impressions,
        )
        db.add(row)

    db.commit()
    db.refresh(row)
    return row


def list_files(db: Session, skip: int = 0, limit: int = 100) -> List[SourceFile]:
    return db.execute(select(SourceFile).offset(skip).limit(limit)).scalars().all()
