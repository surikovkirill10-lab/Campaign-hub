from fastapi import APIRouter
import os, sqlite3

router = APIRouter()

def _db_path():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(root, "yandex_metrics.db")

@router.get("/postclick/{cid}.json")
def postclick_json(cid: int):
    db = _db_path()
    if not os.path.exists(db):
        return {"rows": [], "error": "db_not_found"}
    con = sqlite3.connect(db); cur = con.cursor()
    q = """SELECT report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec, reachability
           FROM campaign_kpis_daily
           WHERE campaign_id=?
           ORDER BY report_date ASC"""
    rows = [{"date": d, "visits": v, "visitors": u, "bounce_rate": br,
             "page_depth": pd, "avg_time_sec": ts, "reachability": r}
            for (d,v,u,br,pd,ts,r) in cur.execute(q,(cid,))]
    con.close()
    return {"rows": rows}
