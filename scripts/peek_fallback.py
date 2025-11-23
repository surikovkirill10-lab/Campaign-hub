import sqlite3, os, json
db = os.path.join(os.getcwd(), "campaign_hub.db")
if not os.path.exists(db):
    print("NO_DB", db); raise SystemExit()
con = sqlite3.connect(db); cur = con.cursor()
try:
    rows = cur.execute("SELECT id, name FROM campaigns ORDER BY id DESC LIMIT 10").fetchall()
    print("ROWS:", rows)
except Exception as e:
    print("ERR:", e)
finally:
    con.close()
