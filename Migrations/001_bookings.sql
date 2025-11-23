-- migrations/001_bookings.sql
-- SQLite schema for Bookings (Брони), KPI, and directories (clients, agencies)

PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- --- directories ---
CREATE TABLE IF NOT EXISTS clients (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  name          TEXT NOT NULL UNIQUE,
  brand         TEXT,
  legal_entity  TEXT,
  inn           TEXT,
  notes         TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS agencies (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  name          TEXT NOT NULL UNIQUE,
  notes         TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT DEFAULT (datetime('now'))
);

-- --- bookings ---
CREATE TABLE IF NOT EXISTS bookings (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  ext_row_id             TEXT,
  month_str              TEXT,
  name                   TEXT NOT NULL,
  campaign_id            INTEGER,
  client_id              INTEGER REFERENCES clients(id) ON DELETE SET NULL,
  agency_id              INTEGER REFERENCES agencies(id) ON DELETE SET NULL,
  brand                  TEXT,
  legal_entity           TEXT,
  format                 TEXT,
  buying_model           TEXT,
  status                 TEXT,
  contract_type          TEXT,
  start_date             TEXT,
  end_date               TEXT,
  budget_after_vat       REAL,
  budget_client_net      REAL,
  budget_client_gross    REAL,
  budget_after_sk        REAL,
  inventory_total_plan   REAL,
  inventory_fact         REAL,
  inventory_commercial   REAL,
  inventory_bonus        REAL,
  price_unit             REAL,
  price_unit_with_bonus  REAL,
  price_unit_with_vat    REAL,
  cpm_cpc_to_platform    REAL,
  vz_percent             REAL,
  refund_amount          REAL,
  plan_payment_date      TEXT,
  fact_payment_date      TEXT,
  act_number             TEXT,
  act_id                 TEXT,
  contract_id            TEXT,
  initial_contract_id    TEXT,
  sales_manager          TEXT,
  account_manager        TEXT,
  comment_sales          TEXT,
  comment_accounts       TEXT,
  raw_json               TEXT,
  created_at             TEXT DEFAULT (datetime('now')),
  updated_at             TEXT DEFAULT (datetime('now')),
  UNIQUE(name, start_date, end_date) ON CONFLICT IGNORE
);

CREATE INDEX IF NOT EXISTS idx_bookings_campaign ON bookings(campaign_id);
CREATE INDEX IF NOT EXISTS idx_bookings_client ON bookings(client_id);
CREATE INDEX IF NOT EXISTS idx_bookings_agency ON bookings(agency_id);
CREATE INDEX IF NOT EXISTS idx_bookings_period ON bookings(start_date, end_date);

-- --- KPI targets per booking (edit via UI) ---
CREATE TABLE IF NOT EXISTS booking_kpis (
  booking_id       INTEGER PRIMARY KEY,
  kpi_impressions  REAL,
  kpi_clicks       REAL,
  kpi_uniques      REAL,
  kpi_ctr          REAL,
  kpi_freq         REAL,
  kpi_conversions  REAL,
  kpi_spend        REAL,
  kpi_cpm          REAL,
  kpi_cpc          REAL,
  kpi_cpa          REAL,
  updated_at       TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (booking_id) REFERENCES bookings(id) ON DELETE CASCADE
);

CREATE TRIGGER IF NOT EXISTS trg_bookings_updated_at
AFTER UPDATE ON bookings
BEGIN
  UPDATE bookings SET updated_at = datetime('now') WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_kpis_updated_at
AFTER UPDATE ON booking_kpis
BEGIN
  UPDATE booking_kpis SET updated_at = datetime('now') WHERE booking_id = NEW.booking_id;
END;

COMMIT;
