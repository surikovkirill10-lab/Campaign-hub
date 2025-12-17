from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    Float,
)
from sqlalchemy.orm import relationship

from .database import Base


class Publisher(Base):
    __tablename__ = "publishers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    # здесь просто храним ID юзера из вашей существующей таблицы пользователей
    user_id = Column(Integer, nullable=False, unique=True)
    contact_email = Column(String, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    sites = relationship("PublisherSite", back_populates="publisher")



class PublisherSite(Base):
    __tablename__ = "publisher_sites"

    id = Column(Integer, primary_key=True, index=True)
    publisher_id = Column(Integer, ForeignKey("publishers.id"), nullable=False)
    name = Column(String, nullable=False)
    domain = Column(String, nullable=False)
    public_token = Column(String, nullable=False, unique=True, index=True)
    secret_key = Column(String, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    publisher = relationship("Publisher", back_populates="sites")
    placements = relationship("WidgetPlacement", back_populates="site")


class WidgetVideo(Base):
    __tablename__ = "widget_videos"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    duration_sec = Column(Integer, nullable=True)
    src_type = Column(String, nullable=False)  # mp4 | hls | youtube | internal
    src_url = Column(String, nullable=False)
    poster_url = Column(String, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    placements = relationship("WidgetPlacement", back_populates="video")


class WidgetPlacement(Base):
    __tablename__ = "widget_placements"

    id = Column(Integer, primary_key=True, index=True)
    site_id = Column(Integer, ForeignKey("publisher_sites.id"), nullable=False)
    external_article_id = Column(String, nullable=True)
    page_url_pattern = Column(String, nullable=True)
    video_id = Column(Integer, ForeignKey("widget_videos.id"), nullable=False)
    config_json = Column(Text, nullable=True)
    status = Column(String, default="active", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    site = relationship("PublisherSite", back_populates="placements")
    video = relationship("WidgetVideo", back_populates="placements")
    sessions = relationship("WidgetSession", back_populates="placement")


class WidgetSession(Base):
    __tablename__ = "widget_sessions"

    id = Column(Integer, primary_key=True, index=True)
    session_token = Column(String, nullable=False, unique=True, index=True)
    placement_id = Column(Integer, ForeignKey("widget_placements.id"), nullable=False)
    publisher_id = Column(Integer, ForeignKey("publishers.id"), nullable=False)
    site_id = Column(Integer, ForeignKey("publisher_sites.id"), nullable=False)
    page_url = Column(String, nullable=True)
    article_id = Column(String, nullable=True)
    client_ip = Column(String, nullable=True)
    user_agent = Column(Text, nullable=True)
    referer = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    placement = relationship("WidgetPlacement", back_populates="sessions")
    site = relationship("PublisherSite")
    publisher = relationship("Publisher")
    events = relationship("WidgetEvent", back_populates="session")


class WidgetEvent(Base):
    __tablename__ = "widget_events"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("widget_sessions.id"), nullable=False)
    event_type = Column(String, nullable=False)
    event_ts = Column(DateTime, default=datetime.utcnow, nullable=False)
    video_time = Column(Float, nullable=True)  # seconds
    meta_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    session = relationship("WidgetSession", back_populates="events")
