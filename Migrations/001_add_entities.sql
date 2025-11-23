PRAGMA foreign_keys=ON;

-- Клиенты
CREATE TABLE IF NOT EXISTS clients (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  industry TEXT,
  kpi_notes TEXT
);

-- Флайты
CREATE TABLE IF NOT EXISTS flights (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL,
  name TEXT,
  start_date DATE,
  end_date DATE,
  budget DECIMAL,
  status TEXT,
  notes TEXT,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
);
CREATE INDEX IF NOT EXISTS ix_flights_cid ON flights(campaign_id);

-- Креативы
CREATE TABLE IF NOT EXISTS creatives (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT,         -- banner/video/..
  url TEXT,          -- ссылка на файл/хранилище
  status TEXT,
  notes TEXT
);

-- Связь флайт ↔ креатив (M:N)
CREATE TABLE IF NOT EXISTS flight_creatives (
  flight_id INTEGER NOT NULL,
  creative_id INTEGER NOT NULL,
  goal TEXT,
  planned_impressions INTEGER,
  planned_spend DECIMAL,
  PRIMARY KEY (flight_id, creative_id),
  FOREIGN KEY (flight_id) REFERENCES flights(id),
  FOREIGN KEY (creative_id) REFERENCES creatives(id)
);

-- Дневная статистика по флайтам
CREATE TABLE IF NOT EXISTS flight_daily_stats (
  id INTEGER PRIMARY KEY,
  flight_id INTEGER NOT NULL,
  date DATE NOT NULL,
  impressions INTEGER,
  clicks INTEGER,
  reach INTEGER,
  ctr FLOAT,
  vtr FLOAT,
  visits INTEGER,
  bounce_rate FLOAT,
  time_sec FLOAT,
  depth FLOAT,
  FOREIGN KEY (flight_id) REFERENCES flights(id)
);
CREATE INDEX IF NOT EXISTS ix_fdst_flight_date ON flight_daily_stats(flight_id, date);

-- Группы кампаний
CREATE TABLE IF NOT EXISTS campaign_groups (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Участники групп (M:N)
CREATE TABLE IF NOT EXISTS campaign_group_members (
  group_id INTEGER NOT NULL,
  campaign_id INTEGER NOT NULL,
  order_num INTEGER,
  PRIMARY KEY (group_id, campaign_id),
  FOREIGN KEY (group_id) REFERENCES campaign_groups(id),
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
);

-- Финансовый модуль закрытия
CREATE TABLE IF NOT EXISTS financial_closures (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL,
  flight_id INTEGER,
  client_kpi_ctr FLOAT,
  client_kpi_reach INTEGER,
  client_kpi_freq FLOAT,
  planned_units INTEGER,
  actual_units INTEGER,
  planned_spend DECIMAL,
  actual_spend DECIMAL,
  revenue DECIMAL,
  margin DECIMAL,
  closure_date DATE,
  notes TEXT,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
  FOREIGN KEY (flight_id) REFERENCES flights(id)
);
CREATE INDEX IF NOT EXISTS ix_fin_close_camp ON financial_closures(campaign_id, closure_date);

-- Маржинальность (Cats доп.выгрузка)
CREATE TABLE IF NOT EXISTS margin_data (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL,
  flight_id INTEGER,
  date DATE NOT NULL,
  unit_type TEXT,         -- click/impression/...
  planned_price DECIMAL,
  actual_price DECIMAL,
  units_purchased INTEGER,
  underdelivery INTEGER,
  notes TEXT,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
  FOREIGN KEY (flight_id) REFERENCES flights(id)
);
CREATE INDEX IF NOT EXISTS ix_margin_camp_date ON margin_data(campaign_id, date);

-- Данные верификатора (дневной срез по кампаниям)
CREATE TABLE IF NOT EXISTS verificator_daily (
  id INTEGER PRIMARY KEY,
  campaign_id INTEGER NOT NULL,
  date DATE NOT NULL,
  impressions_delta FLOAT,
  clicks_delta FLOAT,
  ivt_impressions INTEGER,
  sivt_impressions INTEGER,
  givt_impressions INTEGER,
  ivt_clicks INTEGER,
  sivt_clicks INTEGER,
  givt_clicks INTEGER,
  brand_safety FLOAT,
  post_view_conversions INTEGER,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
);
CREATE INDEX IF NOT EXISTS ix_verif_camp_date ON verificator_daily(campaign_id, date);
