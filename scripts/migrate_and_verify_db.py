import os, sqlite3, datetime, csv

DB = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "yandex_metrics.db"))
print("DB path:", DB, "exists=", os.path.exists(DB))

con = sqlite3.connect(DB)
cur = con.cursor()

def table_exists(name):
    return cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None

def column_exists(table, col):
    row = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c[1] == col for c in row)

# 1) Гарантируем наличие актуальных таблиц
if not table_exists("yandex_daily_metrics"):
    cur.execute("""
    CREATE TABLE yandex_daily_metrics (
      campaign_id   INTEGER,
      report_date   TEXT,
      visits        REAL,
      visitors      REAL,
      bounce_rate   REAL,
      page_depth    REAL,
      avg_time_sec  REAL,
      PRIMARY KEY (campaign_id, report_date)
    );""")
    print("Created: yandex_daily_metrics")

if not table_exists("yandex_import_files"):
    cur.execute("""
    CREATE TABLE yandex_import_files (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      campaign_id     INTEGER,
      message_id      TEXT,
      subject         TEXT,
      attachment_name TEXT,
      report_date     TEXT,
      processed_at    TEXT,
      UNIQUE (message_id, attachment_name)
    );""")
    print("Created: yandex_import_files")
else:
    # Мигрируем старую схему → добавим недостающие поля
    if not column_exists("yandex_import_files", "subject"):
        cur.execute("ALTER TABLE yandex_import_files ADD COLUMN subject TEXT;")
        print("ALTER: yandex_import_files + subject")
    if not column_exists("yandex_import_files", "campaign_id"):
        cur.execute("ALTER TABLE yandex_import_files ADD COLUMN campaign_id INTEGER;")
        print("ALTER: yandex_import_files + campaign_id")
    if not column_exists("yandex_import_files", "attachment_name"):
        cur.execute("ALTER TABLE yandex_import_files ADD COLUMN attachment_name TEXT;")
        print("ALTER: yandex_import_files + attachment_name")
    if not column_exists("yandex_import_files", "report_date"):
        cur.execute("ALTER TABLE yandex_import_files ADD COLUMN report_date TEXT;")
        print("ALTER: yandex_import_files + report_date")
    if not column_exists("yandex_import_files", "processed_at"):
        cur.execute("ALTER TABLE yandex_import_files ADD COLUMN processed_at TEXT;")
        print("ALTER: yandex_import_files + processed_at")

con.commit()

# 2) Печатаем список таблиц и размеры
print("\n== Tables ==")
for (tname,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    cnt = cur.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
    print(f"{tname}: {cnt}")

# 3) Последние 10 записей журнала (без падения, если что-то пусто)
print("\n== Last 10 processed attachments ==")
cols = [c[1] for c in cur.execute("PRAGMA table_info(yandex_import_files)")]
sel = []
for c in ("processed_at","subject","attachment_name","report_date","campaign_id"):
    sel.append(c if c in cols else "NULL AS "+c)
q = "SELECT " + ", ".join(sel) + " FROM yandex_import_files ORDER BY processed_at DESC LIMIT 10"
for r in cur.execute(q).fetchall():
    print(f"{r[0]} | camp={r[4]} | date={r[3]} | file={r[2]} | subj={r[1]}")

# 4) Последние метрики по всем кампаниям
print("\n== Recent daily metrics (last 10 rows) ==")
for r in cur.execute("""
  SELECT campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec
  FROM yandex_daily_metrics
  ORDER BY report_date DESC, campaign_id
  LIMIT 10
""").fetchall():
    cid, d, v, u, br, pd, ts = r
    try:
        ts = int(round(float(ts))); h, rem = divmod(ts, 3600); m, s = divmod(rem, 60)
        tstr = f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        tstr = "00:00:00"
    print(f"camp={cid} | {d} | visits={int(v)} | users={int(u)} | bounce={br:.3f} | depth={pd:.3f} | avg_time={tstr}")

# 5) Экспорт за 30 дней
print("\n== Export CSV (last 30 days) ==")
since = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
rows = cur.execute("""
  SELECT campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec
  FROM yandex_daily_metrics WHERE report_date >= ?
  ORDER BY report_date, campaign_id
""", (since,)).fetchall()
if rows:
    outp = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "export", "yandex_daily_metrics_last30.csv"))
    with open(outp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["campaign_id","report_date","visits","visitors","bounce_rate","page_depth","avg_time_sec"])
        w.writerows(rows)
    print("CSV:", outp)
else:
    print("no rows")

con.close()
