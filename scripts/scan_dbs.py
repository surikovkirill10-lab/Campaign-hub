import os, sqlite3, json
root = os.getcwd()
dbs = []
for base,dirs,files in os.walk(root):
    for fn in files:
        if fn.lower().endswith(".db"):
            dbs.append(os.path.join(base, fn))

res = []
for db in dbs:
    try:
        con = sqlite3.connect(db)
        cur = con.cursor()
        tabs = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        meta = {"db": db, "tables": []}
        for t in tabs:
            try:
                cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except Exception:
                cnt = None
            meta["tables"].append({"name": t, "count": cnt})
        con.close()
        res.append(meta)
    except Exception as e:
        res.append({"db": db, "error": str(e)})

print(json.dumps(res, ensure_ascii=False, indent=2))
