from flask import Blueprint, jsonify, current_app
import os, sqlite3

bp = Blueprint("postclick", __name__)

def _db_path():
    # БД лежит в корне проекта рядом со scripts (как мы создавали ранее)
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(root, "yandex_metrics.db")

@bp.get("/postclick/<int:cid>.json")
def postclick_json(cid: int):
    db = _db_path()
    if not os.path.exists(db):
        return jsonify({"rows": [], "error": "db_not_found"}), 200
    con = sqlite3.connect(db); cur = con.cursor()
    # Берём уже готовую витрину с доходимостью
    q = """SELECT report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec, reachability
           FROM campaign_kpis_daily
           WHERE campaign_id = ?
           ORDER BY report_date ASC"""
    rows = [{"date": d,
             "visits": v,
             "visitors": u,
             "bounce_rate": br,
             "page_depth": pd,
             "avg_time_sec": ts,
             "reachability": r} for (d,v,u,br,pd,ts,r) in cur.execute(q, (cid,))]
    con.close()
    return jsonify({"rows": rows}), 200
