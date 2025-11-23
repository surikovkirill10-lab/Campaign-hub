from fastapi import APIRouter, Request
from starlette.responses import RedirectResponse

router = APIRouter()

@router.api_route("/campaigns/add", methods=["GET","POST"])
async def campaigns_add_get(id: str = "", add: str = "", name: str = ""):
    cid = (id or add or "").strip()
    if not cid.isdigit():
        return RedirectResponse(url="/campaigns", status_code=303)
    return RedirectResponse(url=f"/campaigns?add={cid}", status_code=303)

@router.api_route("/campaigns/add", methods=["GET","POST"])
async def campaigns_add_post(request: Request):
    form = await request.form()
    cid = (form.get("id") or form.get("add") or form.get("campaign_id") or "").strip()
    if not cid.isdigit():
        return RedirectResponse(url="/campaigns", status_code=303)
    return RedirectResponse(url=f"/campaigns?add={cid}", status_code=303)


# --- Add campaign via GET/POST (redirects to /campaigns?add=<id>) ---------------------------
from fastapi import Request
from starlette.responses import RedirectResponse

@router.api_route("/campaigns/add", methods=["GET","POST"])
async def campaigns_add_get(id: str = "", add: str = "", name: str = ""):
    cid = (id or add or "").strip()
    if not cid.isdigit():
        return RedirectResponse(url="/campaigns", status_code=303)
    return RedirectResponse(url=f"/campaigns?add={cid}", status_code=303)

@router.api_route("/campaigns/add", methods=["GET","POST"])
async def campaigns_add_post(request: Request):
    form = await request.form()
    cid = (form.get("id") or form.get("add") or form.get("campaign_id") or "").strip()
    if not cid.isdigit():
        return RedirectResponse(url="/campaigns", status_code=303)
    return RedirectResponse(url=f"/campaigns?add={cid}", status_code=303)


from fastapi import Request
from starlette.responses import RedirectResponse

@router.api_route("/campaigns/add", methods=["GET","POST"])
async def campaigns_add_unified(request: Request):
    # читаем id/name из query (GET) или формы (POST)
    cid = name = ""
    try:
        if request.method == "GET":
            qp = dict(request.query_params)
            cid  = (qp.get("id")   or qp.get("add") or "").strip()
            name = (qp.get("name") or "").strip()
        else:
            form = await request.form()
            cid  = (form.get("id") or form.get("add") or form.get("campaign_id") or "").strip()
            name = (form.get("name") or form.get("campaign_name") or "").strip()
    except Exception:
        pass

    if not cid.isdigit():
        return RedirectResponse(url="/campaigns", status_code=303)

    # пытаемся через crud
    ok = False
    try:
        from app.services import crud
        if hasattr(crud, "add"):
            crud.add(id=int(cid), name=name); ok = True
        elif hasattr(crud, "add_campaign"):
            crud.add_campaign(id=int(cid), name=name); ok = True
    except Exception:
        ok = False

    # fallback: локальная SQLite c:/campaign_hub_windows_09/campaign_hub.db (таблица campaigns)
    if not ok:
        try:
            import sqlite3
            con = sqlite3.connect("c:/campaign_hub_windows_09/campaign_hub.db")
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS campaigns (id INTEGER PRIMARY KEY, name TEXT)")
            cur.execute("INSERT OR IGNORE INTO campaigns (id,name) VALUES (?,?)", (int(cid), name))
            con.commit(); con.close()
        except Exception:
            pass

    return RedirectResponse(url="/campaigns", status_code=303)
