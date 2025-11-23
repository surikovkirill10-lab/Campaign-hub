import os, sqlite3

DB = os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))
con = sqlite3.connect(DB); cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS campaign_yandex (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_id    INTEGER NOT NULL UNIQUE,
  enabled        INTEGER NOT NULL DEFAULT 0,   -- 0/1
  yandex_name    TEXT    NOT NULL,
  yandex_mailbox TEXT    NOT NULL DEFAULT 'INBOX',
  created_at     TEXT    DEFAULT (datetime('now')),
  updated_at     TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS yandex_daily_metrics (
  campaign_id   INTEGER,
  report_date   TEXT,
  visits        REAL,
  visitors      REAL,
  bounce_rate   REAL,
  page_depth    REAL,
  avg_time_sec  REAL,
  PRIMARY KEY (campaign_id, report_date)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS yandex_import_files (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_id     INTEGER,
  message_id      TEXT,
  subject         TEXT,
  attachment_name TEXT,
  report_date     TEXT,
  processed_at    TEXT,
  UNIQUE (message_id, attachment_name)
);
""")

# клики Cats по дням (заполняется вашим текущим импортёром Cats)
cur.execute("""
CREATE TABLE IF NOT EXISTS cats_clicks_daily (
  campaign_id  INTEGER,
  report_date  TEXT,
  clicks       INTEGER,
  PRIMARY KEY (campaign_id, report_date)
);
""")

# Вьюха: дневные KPI + доходимость; формат времени сразу HH:MM:SS
cur.execute("""
CREATE VIEW IF NOT EXISTS campaign_kpis_daily AS
SELECT
  ym.campaign_id,
  ym.report_date,
  IFNULL(cc.clicks, 0)                              AS clicks,
  ym.visits,
  ym.visitors,
  ym.bounce_rate,
  ym.page_depth,
  ym.avg_time_sec,
  printf('%02d:%02d:%02d',
         CAST(ym.avg_time_sec/3600 AS INTEGER),
         CAST(ym.avg_time_sec/60   AS INTEGER) % 60,
         CAST(ym.avg_time_sec      AS INTEGER) % 60) AS avg_time_hms,
  CASE WHEN IFNULL(cc.clicks,0) > 0
       THEN ym.visits * 1.0 / cc.clicks
       ELSE NULL END                                 AS reachability
FROM yandex_daily_metrics ym
LEFT JOIN cats_clicks_daily cc
  ON cc.campaign_id = ym.campaign_id
 AND cc.report_date = ym.report_date;
""")

con.commit(); con.close()
print("MIGRATE: OK ->", DB)
