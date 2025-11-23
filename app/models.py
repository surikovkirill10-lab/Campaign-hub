"""ORM model definitions for Campaign Hub.

The models in this file describe the relational schema used by the application.
Each class corresponds to a table in the database.  SQLAlchemy is used to
generate and execute SQL queries.  See `campaign_hub/app/database.py` for the
engine and session setup.
"""

from __future__ import annotations

import enum
from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Enum,
    JSON,
)
from sqlalchemy.orm import relationship

from .database import Base


class SourceType(enum.Enum):
    """Enumeration of file sources."""

    system = "system"
    metrica = "metrica"
    verifier = "verifier"  # reserved for future sources (e.g. Weborama)


class Campaign(Base):
    """Represents an advertising campaign known to the system."""

    __tablename__ = "campaigns"

    id: int = Column(Integer, primary_key=True, index=True)
    name: str = Column(String, nullable=False)
    mail_rule_id: Optional[int] = Column(Integer, ForeignKey("mail_rules.id"))
    is_active: bool = Column(Boolean, default=True)
    notes: Optional[str] = Column(String)

    mail_rule = relationship("MailRule", back_populates="campaigns")
    source_files = relationship("SourceFile", back_populates="campaign")
    raw_system_daily = relationship("RawSystemDaily", back_populates="campaign")
    raw_metrica_daily = relationship("RawMetricaDaily", back_populates="campaign")
    fact_daily = relationship("FactDaily", back_populates="campaign")


class MailRule(Base):
    """Describes how to locate e‑mail attachments for a given campaign.

    A mail rule specifies allowed senders, subject/filename patterns and
    instructions for extracting dates.  Multiple campaigns can share the same
    rule if they come from a common source.
    """

    __tablename__ = "mail_rules"

    id: int = Column(Integer, primary_key=True, index=True)
    allowed_senders: Optional[JSON] = Column(JSON)
    subject_regex: Optional[JSON] = Column(JSON)
    filename_regex: Optional[JSON] = Column(JSON)
    folder: str = Column(String, default="INBOX")
    date_extractors: Optional[JSON] = Column(JSON)
    goal_ids: Optional[JSON] = Column(JSON)

    campaigns = relationship("Campaign", back_populates="mail_rule")


class SourceFile(Base):
    """Represents an imported file (either from the system or from email).

    The `sha256` field is used to prevent duplicate processing.  If a file
    originates from an e‑mail, the `message_id` and `sender`/`subject` fields
    capture metadata about the message.
    """

    __tablename__ = "source_files"

    id: int = Column(Integer, primary_key=True, index=True)
    source: SourceType = Column(Enum(SourceType), nullable=False)
    campaign_id: Optional[int] = Column(Integer, ForeignKey("campaigns.id"))
    message_id: Optional[str] = Column(String)
    sender: Optional[str] = Column(String)
    subject: Optional[str] = Column(String)
    filename: Optional[str] = Column(String)
    sha256: str = Column(String, unique=True)
    received_at: datetime = Column(DateTime, default=datetime.utcnow)
    period_from: Optional[date] = Column(Date)
    period_to: Optional[date] = Column(Date)
    rows: Optional[int] = Column(Integer)
    status: str = Column(String, default="processed")  # processed|error|skipped
    error: Optional[str] = Column(String)

    campaign = relationship("Campaign", back_populates="source_files")
    raw_system_daily = relationship("RawSystemDaily", back_populates="source_file")
    raw_metrica_daily = relationship("RawMetricaDaily", back_populates="source_file")


class RawSystemDaily(Base):
    """Denormalised daily statistics from the advertiser's system.

    This table stores the raw day‑level metrics extracted from XLSX exports of
    your platform.  Each row corresponds to a single day and campaign.
    """

    __tablename__ = "raw_system_daily"

    id: int = Column(Integer, primary_key=True, index=True)
    source_file_id: int = Column(Integer, ForeignKey("source_files.id"))
    campaign_id: int = Column(Integer, ForeignKey("campaigns.id"))
    date: date = Column(Date, nullable=False)
    impressions: Optional[int] = Column(Integer)
    clicks: Optional[int] = Column(Integer)
    spend: Optional[float] = Column(Float)
    reach: Optional[int] = Column(Integer)
    frequency: Optional[float] = Column(Float)
    ctr: Optional[float] = Column(Float)
    view_quarter: Optional[int] = Column(Integer, name="view_25")
    view_half: Optional[int] = Column(Integer, name="view_50")
    view_three_quarters: Optional[int] = Column(Integer, name="view_75")
    view_full: Optional[int] = Column(Integer, name="view_100")
    vtr: Optional[float] = Column(Float)

    source_file = relationship("SourceFile", back_populates="raw_system_daily")
    campaign = relationship("Campaign", back_populates="raw_system_daily")


class RawMetricaDaily(Base):
    """Denormalised daily statistics from Yandex Metrica.

    Each row captures visits, bounce statistics and goal conversions for a
    campaign on a given day.  Additional columns can be added to accommodate
    custom metrics or multiple goals.
    """

    __tablename__ = "raw_metrica_daily"

    id: int = Column(Integer, primary_key=True, index=True)
    source_file_id: int = Column(Integer, ForeignKey("source_files.id"))
    campaign_id: int = Column(Integer, ForeignKey("campaigns.id"))
    date: date = Column(Date, nullable=False)
    visits: Optional[int] = Column(Integer)
    visitors: Optional[int] = Column(Integer)
    bounces: Optional[int] = Column(Integer)
    depth: Optional[float] = Column(Float)
    time_on_site: Optional[float] = Column(Float)  # seconds
    conversions: Optional[int] = Column(Integer)

    source_file = relationship("SourceFile", back_populates="raw_metrica_daily")
    campaign = relationship("Campaign", back_populates="raw_metrica_daily")


class FactDaily(Base):
    """Aggregated daily facts combining system and Metrica metrics.

    This table represents the unified view used by the UI.  It contains
    campaign/day combinations along with derived measures (CTR, CPC, etc.).
    Fact rows are regenerated each time new source data is imported.
    """

    __tablename__ = "fact_daily"

    id: int = Column(Integer, primary_key=True, index=True)
    campaign_id: int = Column(Integer, ForeignKey("campaigns.id"))
    date: date = Column(Date, nullable=False)
    impressions: Optional[int] = Column(Integer)
    clicks: Optional[int] = Column(Integer)
    spend: Optional[float] = Column(Float)
    ctr_ext: Optional[float] = Column(Float)
    visits: Optional[int] = Column(Integer)
    conversions: Optional[int] = Column(Integer)
    bounce_rate: Optional[float] = Column(Float)
    cpc: Optional[float] = Column(Float)
    cpm: Optional[float] = Column(Float)
    cpa: Optional[float] = Column(Float)
    diff_clicks: Optional[float] = Column(Float)
    diff_impressions: Optional[float] = Column(Float)

    campaign = relationship("Campaign", back_populates="fact_daily")
