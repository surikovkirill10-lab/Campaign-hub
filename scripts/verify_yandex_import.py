import os, sqlite3, yaml, datetime

CFG = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
name_map = {c["yandex_name"]: c["id"] for c in (CFG.get("yandex_campaigns") or []) if "yandex_name" in c and "id" in c}

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "yandex_metrics.db")
print(f"DB path: {os.path.abspath(db_path)}  exists={os.path.exists(db_path)}")
if not os.path.exists(db_path):
    raise SystemExit("❌ БД не найдена")

con = sqlite3.connect(db_path)
cur = con.cursor()

# Показать список таблиц и их размеры
print("\n== Tables ==")
for t in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    tn = t[0]
    cnt = cur.execute(f"SELECT COUNT(*) FROM {tn}").fetchone()[0]
    print(f"{tn}: {cnt}")

# Последние обработанные письма (журнал), чтобы убедиться что взяли именно таблицы, а не графики
print("\n== Last 10 processed attachments ==")
rows = cur.execute("""
  SELECT processed_at, subject, attachment_name, report_date, campaign_id
  FROM yandex_import_files
  ORDER BY processed_at DESC
  LIMIT 10
""").fetchall()
for r in rows:
    print(f"{r[0]} | camp={r[4]} | date={r[3]} | file={r[2]} | subj={r[1]}")

# Витрина по метрикам: последние записи по каждой кампании из конфига
def fmt_time(sec):
    try:
        sec = int(round(float(sec)))
        h, m = divmod(sec, 3600)
        m, s = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return "00:00:00"

print("\n== Recent daily metrics by campaign ==")
for yname, cid in name_map.items():
    print(f"\n-- {yname} (campaign_id={cid}) --")
    rows = cur.execute("""
      SELECT report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec
      FROM yandex_daily_metrics
      WHERE campaign_id = ?
      ORDER BY report_date DESC
      LIMIT 10
    """,(cid,)).fetchall()
    if not rows:
        print("  (no rows)")
    else:
        for d,v,uu,br,pd,ts in rows:
            print(f"  {d} | visits={int(v)} | users={int(uu)} | bounce={br:.3f} | depth={pd:.3f} | avg_time={fmt_time(ts)}")

# Экспорт последних 30 дней в CSV для ручной проверки (если данные есть)
today = datetime.date.today()
since = (today - datetime.timedelta(days=30)).isoformat()
exp = cur.execute("""
  SELECT campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec
  FROM yandex_daily_metrics
  WHERE report_date >= ?
  ORDER BY report_date, campaign_id
""",(since,)).fetchall()

if exp:
    import csv
    outp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "export", "yandex_daily_metrics_last30.csv")
    with open(outp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["campaign_id","report_date","visits","visitors","bounce_rate","page_depth","avg_time_sec"])
        w.writerows(exp)
    print(f"\nCSV exported: {outp}")
else:
    print("\nCSV export skipped (no rows for last 30 days).")

# Быстрая диагностика возможных причин пустого импорта
print("\n== Quick sanity checks ==")
# 1) Есть ли письма в журнале за последние 3 дня?
three_days = (today - datetime.timedelta(days=3)).isoformat()
cnt3 = cur.execute("""
  SELECT COUNT(*) FROM yandex_import_files
  WHERE processed_at >= ?
""", (three_days,)).fetchone()[0]
print(f"Processed in last 3 days: {cnt3}")

# 2) Сколько уникальных message_id и сколько всего строк по файлам
tot, uniq = cur.execute("""
  SELECT COUNT(*), COUNT(DISTINCT message_id) FROM yandex_import_files
""").fetchone()
print(f"Import files total={tot}, unique messages={uniq}")

con.close()
