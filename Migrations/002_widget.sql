-- 002_widget.sql - tables for external video widget

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS publishers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    user_id INTEGER NOT NULL UNIQUE,
    contact_email TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS publisher_sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    publisher_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    domain TEXT NOT NULL,
    public_token TEXT NOT NULL UNIQUE,
    secret_key TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

CREATE TABLE IF NOT EXISTS widget_videos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    duration_sec INTEGER,
    src_type TEXT NOT NULL, -- mp4 | hls | youtube | internal
    src_url TEXT NOT NULL,
    poster_url TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS widget_placements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER NOT NULL,
    external_article_id TEXT,
    page_url_pattern TEXT,
    video_id INTEGER NOT NULL,
    config_json TEXT,
    status TEXT NOT NULL DEFAULT 'active', -- active | paused
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (site_id) REFERENCES publisher_sites(id),
    FOREIGN KEY (video_id) REFERENCES widget_videos(id)
);

CREATE TABLE IF NOT EXISTS widget_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_token TEXT NOT NULL UNIQUE,
    placement_id INTEGER NOT NULL,
    publisher_id INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    page_url TEXT,
    article_id TEXT,
    client_ip TEXT,
    user_agent TEXT,
    referer TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (placement_id) REFERENCES widget_placements(id),
    FOREIGN KEY (publisher_id) REFERENCES publishers(id),
    FOREIGN KEY (site_id) REFERENCES publisher_sites(id)
);

CREATE TABLE IF NOT EXISTS widget_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    video_time REAL,
    meta_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES widget_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_widget_events_session ON widget_events(session_id);
CREATE INDEX IF NOT EXISTS idx_widget_sessions_token ON widget_sessions(session_token);
CREATE INDEX IF NOT EXISTS idx_publisher_sites_token ON publisher_sites(public_token);
