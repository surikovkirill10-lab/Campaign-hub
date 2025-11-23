import os, sqlite3
db = os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))
print("DB:", db)
con = sqlite3.connect(db); cur = con.cursor()
for (t,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"{t}: {cnt}")
print("\n-- yandex_import_files (last 5) --")
for r in cur.execute("SELECT processed_at,campaign_id,report_date,attachment_name FROM yandex_import_files ORDER BY processed_at DESC LIMIT 5"):
    print(r)
print("\n-- yandex_daily_metrics (last 5) --")
for r in cur.execute("SELECT campaign_id,report_date,visits,visitors FROM yandex_daily_metrics ORDER BY report_date DESC, campaign_id LIMIT 5"):
    print(r)
con.close()
