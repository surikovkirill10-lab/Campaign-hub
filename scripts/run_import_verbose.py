import os, sys, json, yaml, sqlite3, subprocess, datetime

cwd = os.getcwd()
imp_path = os.path.abspath(os.path.join("scripts","yandex_import.py"))
cfg_path = os.path.abspath("config.yaml")
db_path  = os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))

def fts(p):
    try:
        st = os.stat(p)
        return f"{st.st_size} bytes, mtime={datetime.datetime.fromtimestamp(st.st_mtime)}"
    except Exception:
        return "missing"

print("=== ENV ===")
print("CWD:", cwd)
print("Python:", sys.executable)
print("Import script:", imp_path, "->", fts(imp_path))
print("Config:", cfg_path, "->", fts(cfg_path))
print("DB path:", db_path, "->", fts(db_path))

# Печатаем, что именно в yandex_campaigns
try:
    with open(cfg_path,"r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    camps = cfg.get("yandex_campaigns") or []
    print("yandex_campaigns:", json.dumps(camps, ensure_ascii=False))
except Exception as e:
    print("! cannot read config.yaml:", repr(e))

print("\n=== RUN IMPORT ===")
# Запускаем импортёр в том же консольном stdout/stderr (стриминг прямо в PowerShell)
ret = subprocess.run([sys.executable, "-u", imp_path], check=False)
print("Importer exit code:", ret.returncode)

print("\n=== DB QUICK CHECK ===")
try:
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Список таблиц и их размеры
    tabs = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    if not tabs:
        print("(no tables)")
    else:
        for (tname,) in tabs:
            cnt = cur.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
            print(f"{tname}: {cnt}")

    # Последние записи журнала (если таблица есть)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='yandex_import_files'")
    if cur.fetchone():
        print("\n-- last 10 from yandex_import_files --")
        for r in cur.execute("""
          SELECT processed_at, campaign_id, report_date, attachment_name, substr(subject,1,80)
          FROM yandex_import_files ORDER BY processed_at DESC LIMIT 10
        """):
            print(r)

    # Последние метрики (если таблица есть)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='yandex_daily_metrics'")
    if cur.fetchone():
        print("\n-- last 10 from yandex_daily_metrics --")
        for r in cur.execute("""
          SELECT campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec
          FROM yandex_daily_metrics ORDER BY report_date DESC, campaign_id LIMIT 10
        """):
            print(r)

    con.close()
except Exception as e:
    print("DB check error:", repr(e))
