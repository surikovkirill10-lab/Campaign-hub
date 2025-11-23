from fastapi import APIRouter, Request
from starlette.responses import RedirectResponse

router = APIRouter()

def _fallback_insert(cid:int, name:str):
    try:
        import sqlite3
        con = sqlite3.connect("c:/campaign_hub_windows_09/campaign_hub.db")
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS campaigns (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT OR IGNORE INTO campaigns (id,name) VALUES (?,?)", (cid, name))
        con.commit(); con.close()
    except Exception:
        pass

@router.api_route("/campaigns/add2", methods=["GET","POST"])
async def campaigns_add2(request: Request):
    # читаем id/name из query (GET) или формы (POST)
    cid = ""; name = ""
    if request.method == "GET":
        q = request.query_params
        cid  = (q.get("id") or q.get("add") or "").strip()
        name = (q.get("name") or "").strip()
    else:
        form = await request.form()
        cid  = (form.get("id") or form.get("add") or form.get("campaign_id") or "").strip()
        name = (form.get("name") or form.get("campaign_name") or "").strip()

    if not cid.isdigit():
        return RedirectResponse(url="/campaigns", status_code=303)

    # Пытаемся через ваш crud, если он есть
    ok = False
    try:
        from app.services import crud
        if hasattr(crud, "add"):
            crud.add(id=int(cid), name=name); ok=True
        elif hasattr(crud, "add_campaign"):
            crud.add_campaign(id=int(cid), name=name); ok=True
    except Exception:
        ok = False

    if not ok:
        _fallback_insert(int(cid), name)

    return RedirectResponse(url="/campaigns", status_code=303)
