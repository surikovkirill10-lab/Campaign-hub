from fastapi import APIRouter, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path
import pandas as pd
import sqlite3
import os
import math
from datetime import date
import requests
import xml.etree.ElementTree as ET
from app.services.config_store import get_effective_system_config
from app.services.cats_export import _ensure_session  # авторизованная requests.Session  :contentReference[oaicite:4]{index=4}
from fastapi import APIRouter, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import text
from app.database import engine
import html
from datetime import datetime
import xml.etree.ElementTree as ET



router = APIRouter()
templates = Jinja2Templates(directory="app/templates")




# ------------------ утилиты ------------------

def _ensure_daily_tables():
    """Гарантируем, что в БД есть нужные таблицы и индексы."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_system_daily(
                campaign_id INTEGER NOT NULL,
                date        TEXT    NOT NULL, -- YYYY-MM-DD
                impressions INTEGER,
                clicks      INTEGER,
                reach       INTEGER,
                view_100    INTEGER,
                PRIMARY KEY (campaign_id, date)
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_daily(
                campaign_id INTEGER NOT NULL,
                date        TEXT    NOT NULL, -- YYYY-MM-DD
                impressions INTEGER,
                clicks      INTEGER,
                PRIMARY KEY (campaign_id, date)
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rsd_date  ON raw_system_daily(date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fd_date   ON fact_daily(date);"))

def _parse_cats_shortage_xls(xml_bytes: bytes, metric_type: str) -> dict[int, float]:
    """
    Парсим SpreadsheetML (.xls) от Cats (shortage/campaigns/cpm|cpc)
    и возвращаем словарь {campaign_id: avg_price}.

    metric_type: "cpm" или "cpc".
    """
    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
    root = ET.fromstring(xml_bytes)

    ws = root.find(".//ss:Worksheet", ns)
    if ws is None:
        return {}

    table = ws.find("ss:Table", ns)
    if table is None:
        return {}

    rows = table.findall("ss:Row", ns)
    if not rows:
        return {}

    def row_vals(row):
        return [
            (cell.find("ss:Data", ns).text
             if cell.find("ss:Data", ns) is not None else None)
            for cell in row.findall("ss:Cell", ns)
        ]

    header = row_vals(rows[0])
    header_l = [(h or "").strip().lower() for h in header]

    # ID кампании
    try:
        id_idx = header_l.index("id")
    except ValueError:
        id_idx = 0

    # "Средний CPM, руб." / "Средний CPC, руб."
    mt = metric_type.lower()
    avg_idx = None
    for i, h in enumerate(header_l):
        if "средний" in h and mt in h:
            avg_idx = i
            break
    if avg_idx is None:
        for i, h in enumerate(header_l):
            if mt in h:
                avg_idx = i
                break
    if avg_idx is None:
        raise RuntimeError(f"Не нашёл колонку со средним {metric_type} в Cats-отчёте: {header}")

    def parse_num(x):
        if x is None:
            return None
        s = str(x)
        s = (
            s.replace("\xa0", "")
             .replace(" ", "")
             .replace(",", ".")
             .replace("руб.", "")
             .replace("р.", "")
             .strip()
        )
        if not s or s.lower() in ("nan", "none"):
            return None
        try:
            return float(s)
        except Exception:
            return None

    result: dict[int, float] = {}
    for row in rows[1:]:
        vals = row_vals(row)
        if not any(vals):
            continue
        need_len = max(id_idx, avg_idx) + 1
        if len(vals) < need_len:
            vals += [None] * (need_len - len(vals))

        cid_raw = vals[id_idx]
        avg_raw = vals[avg_idx]

        try:
            cid = int(str(cid_raw).strip())
        except Exception:
            continue

        avg_price = parse_num(avg_raw)
        if avg_price is None:
            continue

        result[cid] = avg_price

    return result


def _sync_csv_daily_to_db(cid: int) -> int:
    """
    Читает daily CSV кампании и делает UPSERT в raw_system_daily + fact_daily.
    Возвращает число обработанных (вставленных/обновлённых) дней.
    """
    p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
    if not p.exists():
        return 0

    try:
        df = pd.read_csv(p)
    except Exception:
        return 0

    # нормализация колонок
    c_date = "date" if "date" in df.columns else ("День" if "День" in df.columns else None)
    c_impr = "impressions" if "impressions" in df.columns else ("Показы" if "Показы" in df.columns else None)
    c_clk  = "clicks"      if "clicks"      in df.columns else ("Переходы" if "Переходы" in df.columns else None)
    c_uni  = "uniques"     if "uniques"     in df.columns else ("Охват"    if "Охват"    in df.columns else None)

    # view_100: прямая колонка или реконструкция из vtr_percent
    c_v100 = None
    for cand in ("view_100","views_100","views100","video_views_100"):
        if cand in df.columns:
            c_v100 = cand; break
    c_vtrp = "vtr_percent" if "vtr_percent" in df.columns else ("VTR" if "VTR" in df.columns else None)

    if not c_date or not c_impr:
        return 0

    def _num(x):
        try:
            s = str(x).replace("\xa0","").replace(" ","").replace(",",".").replace("%","").strip()
            return float(s) if s not in ("", "nan", "None") else None
        except Exception:
            return None

    def _norm_date(x: str):
        s = str(x).strip()
        if len(s) >= 10 and s[4]=='-' and s[7]=='-':  # YYYY-MM-DD
            return s[:10]
        if len(s) >= 10 and s[2]=='.' and s[5]=='.':  # DD.MM.YYYY
            d,m,y = s[:10].split('.'); return f"{y}-{m}-{d}"
        return None

    # готовим пачки UPSERT
    rsd_rows, fd_rows = [], []
    for _, r in df.iterrows():
        rawd = r.get(c_date)
        if rawd is None: 
            continue
        s = str(rawd).strip().lower()
        if s in ("", "nan", "none", "total", "итого"):
            continue

        dkey = _norm_date(rawd)
        if not dkey:
            continue

        im = _num(r.get(c_impr))
        cl = _num(r.get(c_clk)) if c_clk else None
        un = _num(r.get(c_uni)) if c_uni else None

        v100 = _num(r.get(c_v100)) if c_v100 else None
        if v100 is None and im is not None and c_vtrp:
            vtrp = _num(r.get(c_vtrp))
            if vtrp is not None:
                v100 = im * (vtrp/100.0)

        rsd_rows.append({
            "cid": cid,
            "date": dkey,
            "impressions": int(round(im)) if im is not None else None,
            "clicks":      int(round(cl)) if cl is not None else None,
            "reach":       int(round(un)) if un is not None else None,
            "view_100":    int(round(v100)) if v100 is not None else None,
        })
        fd_rows.append({
            "cid": cid,
            "date": dkey,
            "impressions": int(round(im)) if im is not None else None,
            "clicks":      int(round(cl)) if cl is not None else None,
        })

    if not rsd_rows:
        return 0

    _ensure_daily_tables()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_system_daily (campaign_id, date, impressions, clicks, reach, view_100)
                VALUES (:cid, :date, :impressions, :clicks, :reach, :view_100)
                ON CONFLICT(campaign_id, date) DO UPDATE SET
                    impressions = COALESCE(excluded.impressions, raw_system_daily.impressions),
                    clicks      = COALESCE(excluded.clicks,      raw_system_daily.clicks),
                    reach       = COALESCE(excluded.reach,       raw_system_daily.reach),
                    view_100    = COALESCE(excluded.view_100,    raw_system_daily.view_100)
            """),
            rsd_rows
        )
        conn.execute(
            text("""
                INSERT INTO fact_daily (campaign_id, date, impressions, clicks)
                VALUES (:cid, :date, :impressions, :clicks)
                ON CONFLICT(campaign_id, date) DO UPDATE SET
                    impressions = COALESCE(excluded.impressions, fact_daily.impressions),
                    clicks      = COALESCE(excluded.clicks,      fact_daily.clicks)
            """),
            fd_rows
        )
    return len(rsd_rows)


def _to_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.replace("\xa0","").replace(" ", "").replace(",", ".").strip()
        if s == "":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    try:
        return float(x)
    except Exception:
        return None

def safe_ratio(numer, denom, ndigits=2):
    n = _to_float(numer)
    d = _to_float(denom)
    if n is None or d is None:
        return None
    if not math.isfinite(n) or not math.isfinite(d):
        return None
    if d <= 0.0:
        return None
    try:
        val = n / d
    except Exception:
        return None
    if not math.isfinite(val):
        return None
    return round(val, ndigits)

def _campaign_totals(cid: int):
    p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
    except Exception:
        return None
    if df is None or getattr(df, 'empty', False):
        return None

    total_idx = None
    for col in ("date", "День"):
        if col in df.columns:
            mask = df[col].isna() | df[col].astype(str).str.strip().str.lower().isin(["nan","none","nat","total",""])
            idx = list(df[mask].index)
            if idx:
                total_idx = idx[-1]
                break
    try:
        row = df.iloc[-1] if total_idx is None else df.loc[total_idx]
    except Exception:
        return None

    def to_num(x):
        try:
            return float(str(x).replace(" ","").replace(",",".")) if pd.notna(x) else None
        except Exception:
            return None

    impressions = to_num(row.get("impressions", row.get("Показы")))
    clicks      = to_num(row.get("clicks", row.get("Переходы")))
    uniques     = to_num(row.get("uniques", row.get("Охват")))
    ctr_raw     = to_num(row.get("ctr_percent", row.get("CTR")))
    freq_val    = to_num(row.get("freq"))
    ctr_ratio   = (ctr_raw/100.0) if ctr_raw is not None else None
    if freq_val is None:
        freq_val = safe_ratio(impressions, uniques, ndigits=2)
    return {
        "impressions": int(impressions) if impressions else None,
        "clicks":      int(clicks)      if clicks else None,
        "uniques":     int(uniques)     if uniques else None,
        "ctr_ratio":   ctr_ratio,
        "freq":        freq_val,
    }

def _fetch_all_campaigns_from_db():
    dbp = os.path.join(os.getcwd(), "campaign_hub.db")
    con = sqlite3.connect(dbp)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS campaigns (id INTEGER PRIMARY KEY, name TEXT)")
    rows = cur.execute("SELECT id, COALESCE(name,'') FROM campaigns").fetchall()
    con.close()

    class C: pass
    cs = []
    for cid, cname in rows:
        o = C()
        o.id = cid
        o.name = cname
        o.min_date = None
        o.max_date = None
        o.impressions = None
        o.clicks = None
        o.uniques = None
        o.ctr_ext = None
        o.freq = None
        o.spend = None
        o.conversions = None
        cs.append(o)
    return cs

def _ensure_list():
    """Пробуем взять из app.services.crud, иначе читаем SQLite напрямую."""
    campaigns = []
    try:
        from app.services import crud
        if hasattr(crud, "list"):
            campaigns = crud.list()
        elif hasattr(crud, "list_campaigns"):
            campaigns = crud.list_campaigns()
    except Exception:
        pass
    if campaigns:
        return campaigns
    return _fetch_all_campaigns_from_db()

def _update_name(cid: int, name: str):
    """Правка названия кампании в CRUD или SQLite."""
    try:
        from app.services import crud
        if hasattr(crud, "update"):
            crud.update(id=cid, name=name)
            return
        if hasattr(crud, "update_campaign"):
            crud.update_campaign(id=cid, name=name)
            return
    except Exception:
        pass
    dbp = os.path.join(os.getcwd(), "campaign_hub.db")
    con = sqlite3.connect(dbp)
    cur = con.cursor()
    cur.execute("UPDATE campaigns SET name=? WHERE id=?", (name, cid))
    con.commit(); con.close()

def _delete_campaign(cid: int):
    """Удаление кампании из CRUD или SQLite."""
    try:
        from app.services import crud
        if hasattr(crud, "delete"):
            crud.delete(id=cid); return
        if hasattr(crud, "delete_campaign"):
            crud.delete_campaign(id=cid); return
    except Exception:
        pass
    dbp = os.path.join(os.getcwd(), "campaign_hub.db")
    con = sqlite3.connect(dbp)
    cur = con.cursor()
    cur.execute("DELETE FROM campaigns WHERE id=?", (cid,))
    con.commit(); con.close()

def _find_campaign(cid: int):
    for c in _ensure_list():
        if int(getattr(c, "id")) == int(cid):
            return c
    return None

def _apply_totals(c):
    totals = _campaign_totals(c.id)
    if totals:
        if totals["impressions"] is not None: c.impressions = totals["impressions"]
        if totals["clicks"]      is not None: c.clicks      = totals["clicks"]
        if totals["uniques"]     is not None: c.uniques     = totals["uniques"]
        c.ctr_ext = totals["ctr_ratio"] if totals.get("ctr_ratio") is not None else getattr(c, "ctr_ext", None)
        c.freq    = totals["freq"]      if totals.get("freq")      is not None else getattr(c, "freq", None)
    return c

def _render_row_html(c):
    """Возвращает HTML одной обычной строки таблицы + соседний details контейнер."""
    ctr_txt = ("{:.2f}%".format(c.ctr_ext*100)) if (getattr(c, "ctr_ext", None) is not None) else "–"
    freq_txt = ("{:.2f}".format(c.freq)) if (getattr(c, "freq", None) is not None) else "–"
    period = f"{c.min_date} — {c.max_date}" if (getattr(c, "min_date", None) and getattr(c, "max_date", None)) else "—"
    spend = getattr(c, "spend", None) or "–"
    conv  = getattr(c, "conversions", None) or "–"
    imp   = getattr(c, "impressions", None) or "–"
    clk   = getattr(c, "clicks", None) or "–"
    uni   = getattr(c, "uniques", None) or "–"

    return HTMLResponse(
        "".join([
            f"<tr id='row-{c.id}'>",
            f"<td>{c.id}</td>",
            f"<td>{c.name}</td>",
            f"<td>{period}</td>",
            f"<td><span id='imp-{c.id}'>{imp}</span></td>",
            f"<td><span id='clk-{c.id}'>{clk}</span></td>",
            f"<td>{spend}</td>",
            f"<td><span id='uniq-{c.id}'>{uni}</span></td>",
            f"<td>{conv}</td>",
            f"<td><span id='ctr-{c.id}'>{ctr_txt}</span></td>",
            f"<td><span id='freq-{c.id}'>{freq_txt}</span></td>",
            "<td class='is-narrow'>",
            f"<button type='button' class='button is-info is-small' "
            f"hx-post='/campaigns/{c.id}/pull' hx-target='#camp-{c.id}-status' hx-swap='innerHTML'>Update</button>",
            f"<span id='camp-{c.id}-status' class='tag is-light'>—</span>",
            f"<button type='button' class='button is-warning is-small' "
            f"hx-post='/campaigns/{c.id}/yandex_update' hx-target='#camp-{c.id}-status' hx-swap='innerHTML'>Yandex</button>",
            f"<button type='button' class='button is-small is-light' "
            f"hx-get='/campaigns/{c.id}/daily5' hx-target='#details-{c.id}' hx-swap='outerHTML'><span hx-indicator='#details-{c.id}'>Details</span></button>",
            f"<button type='button' class='button is-small is-light is-danger is-outlined' "
            f"hx-get='/campaigns/{c.id}/daily_empty' hx-target='#details-{c.id}' hx-swap='outerHTML'>Hide</button>",
            f"<button type='button' class='button is-small is-primary is-light' "
            f"hx-get='/campaigns/{c.id}/edit' hx-target='#row-{c.id}' hx-swap='outerHTML'>Edit</button>",
            f"<button type='button' class='button is-small is-danger is-light' "
            f"hx-post='/campaigns/{c.id}/delete' hx-target='#row-{c.id}' hx-swap='outerHTML'>Delete</button>",
            "</td></tr>",
            f"<tr><td colspan='11' id='details-{c.id}'></td></tr>",
        ])
    )



def _render_edit_row_html(c):
    """HTML строки в режиме редактирования (правим ТОЛЬКО name)."""
    from html import escape as _esc

    period  = f"{c.min_date} — {c.max_date}" if (getattr(c, "min_date", None) and getattr(c, "max_date", None)) else "—"
    imp     = getattr(c, "impressions", None) or "–"
    clk     = getattr(c, "clicks", None) or "–"
    uni     = getattr(c, "uniques", None) or "–"
    ctr_txt = ("{:.2f}%".format(c.ctr_ext*100)) if (getattr(c, "ctr_ext", None) is not None) else "–"
    freq_txt= ("{:.2f}".format(c.freq)) if (getattr(c, "freq", None) is not None) else "–"
    spend   = getattr(c, "spend", None) or "–"
    conv    = getattr(c, "conversions", None) or "–"

    safe_name = _esc((getattr(c, "name", "") or ""), quote=True).replace("'", "&#39;")
    form_id = f"edit-form-{c.id}"

    return HTMLResponse(
        "".join([
            f"<tr id='row-{c.id}'>",
            f"<td>{c.id}</td>",
            "<td>",
            f"<form id='{form_id}' hx-post='/campaigns/{c.id}/update' hx-target='#row-{c.id}' hx-swap='outerHTML' class='is-inline'>",
            f"<input class='input is-small' type='text' name='name' value='{safe_name}' style='max-width:22rem' />",
            "</form>",
            "</td>",
            f"<td>{period}</td>",
            f"<td><span id='imp-{c.id}'>{imp}</span></td>",
            f"<td><span id='clk-{c.id}'>{clk}</span></td>",
            f"<td>{spend}</td>",
            f"<td><span id='uniq-{c.id}'>{uni}</span></td>",
            f"<td>{conv}</td>",
            f"<td><span id='ctr-{c.id}'>{ctr_txt}</span></td>",
            f"<td><span id='freq-{c.id}'>{freq_txt}</span></td>",
            "<td class='is-narrow'>",
            f"<button type='submit' form='{form_id}' class='button is-success is-small'>Save</button>",
            f"<button type='button' class='button is-light is-small' hx-get='/campaigns/{c.id}/row' hx-target='#row-{c.id}' hx-swap='outerHTML'>Cancel</button>",
            "</td></tr>",
            f"<tr><td colspan='11' id='details-{c.id}'></td></tr>",
        ])
    )

@router.get("/campaigns/{cid}/edit", response_class=HTMLResponse)
def campaigns_edit(cid: int):
    c = _find_campaign(cid)
    if not c:
        return HTMLResponse("", status_code=404)
    _apply_totals(c)
    return _render_edit_row_html(c)

@router.post("/campaigns/{cid}/update", response_class=HTMLResponse)
def campaigns_update(cid: int, name: str = Form(...)):
    # По соображениям целостности НЕ меняем ID, правим только name
    _update_name(cid, name.strip())
    c = _find_campaign(cid)
    if not c:
        return HTMLResponse("", status_code=404)
    c.name = name.strip()
    _apply_totals(c)
    return _render_row_html(c)

@router.post("/campaigns/{cid}/delete", response_class=HTMLResponse)
def campaigns_delete(cid: int):
    _delete_campaign(cid)
    # Возвращаем пустую замену, чтобы hx-swap="outerHTML" стёр строку
    return HTMLResponse("")

# ------- уже существующие твои обработчики ниже как были -------

@router.post("/campaigns/{cid}/pull", response_class=HTMLResponse)
def campaigns_pull(cid: int):
    try:
        from app.services.cats_export import export_and_ingest
        res = export_and_ingest(str(cid))
        totals = _campaign_totals(cid) or {}
        rows = res.get("ingest", {}).get("rows", "-")
        badge = f'<span class="tag is-success">OK · {rows} rows</span> '
        def span(k, v): return f'<span id="{k}-{cid}" hx-swap-oob="true">{v}</span>'
        imp  = totals.get("impressions", "–")
        clk  = totals.get("clicks", "–")
        uni  = totals.get("uniques", "–")
        ctr  = "{:.2%}".format(totals["ctr_ratio"]) if totals.get("ctr_ratio") is not None else "–"
        fr   = "{:.2f}".format(totals["freq"]) if totals.get("freq") is not None else "–"
        oob = "".join([span("imp", imp), span("clk", clk), span("uniq", uni), span("ctr", ctr), span("freq", fr)])
        return HTMLResponse(badge + oob)
    except Exception as e:
        return HTMLResponse(f'<span class="tag is-danger">FAIL</span> <small>{e}</small>', status_code=500)

@router.get("/campaigns/{cid}/daily2", response_class=HTMLResponse)
def campaigns_daily2(cid: int):
    p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
    if not p.exists():
        return HTMLResponse(f'<td colspan="11" id="details-{cid}"><p>No data (daily2)</p></td>')
    df = pd.read_csv(p)
    date_col = df["date"] if "date" in df.columns else df.get("День")
    imp  = df["impressions"] if "impressions" in df.columns else df.get("Показы")
    clk  = df["clicks"]      if "clicks"      in df.columns else df.get("Переходы")
    uni  = df["uniques"]     if "uniques"     in df.columns else df.get("Охват")
    ctrp = df["ctr_percent"] if "ctr_percent" in df.columns else df.get("CTR")
    vtrp = df["vtr_percent"] if "vtr_percent" in df.columns else df.get("VTR")
    freq = df["freq"]        if "freq"        in df.columns else None
    def num(x):
        try:
            return float(str(x).replace(" ","").replace(",","."))
        except Exception:
            return None
    if freq is None and imp is not None and uni is not None:
        impn = imp.apply(num); unin = uni.apply(num)
        freq = [round((i/u),2) if (i is not None and u and u>0) else None for i,u in zip(impn,unin)]
    def fmt_pct(v): 
        if v is None: return "—"
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return "—"
    def as_int(x): 
        try:
            return int(x) if pd.notna(x) else "—"
        except Exception:
            return "—"
    rows = []
    for i in range(len(df.index)):
        raw = date_col.iloc[i] if date_col is not None else None
        d   = "Total" if (raw is None or str(raw).strip().lower() in ("nan","nat")) else raw
        im  = imp.iloc[i]  if imp  is not None else None
        ck  = clk.iloc[i]  if clk  is not None else None
        un  = uni.iloc[i]  if uni  is not None else None
        cp  = ctrp.iloc[i] if ctrp is not None else None
        vp  = vtrp.iloc[i] if vtrp is not None else None
        fq  = (freq[i] if isinstance(freq,list) and i < len(freq) else (freq.iloc[i] if hasattr(freq,'iloc') and i<len(freq) else None))
        rows.append(f"<tr><td>{d}</td><td>{as_int(im)}</td><td>{as_int(ck)}</td><td>{fmt_pct(cp)}</td><td>{fmt_pct(vp)}</td><td>{as_int(un)}</td><td>{fq if fq is not None else '—'}</td></tr>")
    inner = "<table class='table is-narrow is-striped is-fullwidth'><thead><tr><th>Дата</th><th>Показы</th><th>Клики</th><th>CTR</th><th>VTR</th><th>Охват</th><th>Частота</th></tr></thead><tbody>"+ "".join(rows) +"</tbody></table>"
    return HTMLResponse(f"<td colspan='11' id='details-{cid}' class='p-0'>{inner}</td>")

@router.get("/campaigns/{cid}/daily_empty", response_class=HTMLResponse)
def campaigns_daily_empty(cid: int):
    return HTMLResponse(f"<td colspan='11' id='details-{cid}'></td>")

@router.get("/campaigns/{cid}/daily3", response_class=HTMLResponse)
def campaigns_daily3(cid: int):
    # ... (твой неизменённый код daily3)
    from pathlib import Path
    import os, sqlite3
    import pandas as pd
    def fmt_time_sec(seconds):
        try:
            s = int(round(float(seconds))); h, rem = divmod(s, 3600); m, s2 = divmod(rem, 60)
            return f"{h:02d}:{m:02d}:{s2:02d}"
        except Exception:
            return "—"
    def fmt_float2(v):
        try: return f"{float(v):.2f}"
        except Exception: return "—"
    def fmt_pct2(v):
        try: return f"{float(v):.2f}%"
        except Exception: return "—"
    def norm_date_for_yandex(x):
        s = str(x).strip()
        if len(s) >= 10 and s[4]=='-' and s[7]=='-':  # YYYY-MM-DD
            return s[:10]
        if len(s) >= 10 and s[2]=='.' and s[5]=='.':  # DD.MM.YYYY
            d,m,y = s[:10].split('.'); return f"{y}-{m}-{d}"
        return None
    def reachability(vis, clk):
        try:
            vis = float(vis) if vis is not None else 0.0
            clk = float(clk) if clk is not None else 0.0
            return f"{(vis/clk):.2f}" if clk > 0 else "—"
        except Exception:
            return "—"
    def as_int(x):
        try: return int(float(x)) if pd.notna(x) else "—"
        except Exception: return "—"
    def num(x):
        try: return float(str(x).replace(" ","").replace(",",".")) if pd.notna(x) else None
        except Exception: return None
    p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
    if not p.exists():
        return HTMLResponse(f'<td colspan="11" id="details-{cid}"><p>No data (daily3)</p></td>')
    df = pd.read_csv(p)
    date_col = df["date"]         if "date"         in df.columns else df.get("День")
    imp      = df["impressions"]  if "impressions"  in df.columns else df.get("Показы")
    clk      = df["clicks"]       if "clicks"       in df.columns else df.get("Переходы")
    uni      = df["uniques"]      if "uniques"      in df.columns else df.get("Охват")
    ctrp     = df["ctr_percent"]  if "ctr_percent"  in df.columns else df.get("CTR")
    vtrp     = df["vtr_percent"]  if "vtr_percent"  in df.columns else df.get("VTR")
    freq     = df["freq"]         if "freq"         in df.columns else None
    if freq is None and imp is not None and uni is not None:
        impn = imp.apply(num); unin = uni.apply(num)
        freq = [round((i/u),2) if (i is not None and u and u>0) else None for i,u in zip(impn,unin)]
    ymap = {}
    try:
        ydbp = os.path.join(os.getcwd(), "yandex_metrics.db")
        con = sqlite3.connect(ydbp); cur = con.cursor()
        for rd, vis, br, pdpth, ats in cur.execute("""
            SELECT report_date, visits, bounce_rate, page_depth, avg_time_sec
              FROM yandex_daily_metrics
             WHERE campaign_id=?
        """, (cid,)):
            ymap[str(rd)] = (vis, br, pdpth, ats)
        con.close()
    except Exception:
        ymap = {}
    head = (
      "<thead><tr>"
      "<th>Дата</th><th>Показы</th><th>Клики</th><th>CTR</th><th>VTR</th><th>Охват</th><th>Частота</th>"
      "<th>Визиты</th><th>Доходимость</th><th>Отказы</th><th>Глубина</th><th>Время</th>"
      "</tr></thead>"
    )
    rows_html = []
    n = len(df.index)
    for i in range(n):
        raw = date_col.iloc[i] if date_col is not None else None
        is_total = (raw is None or str(raw).strip().lower() in ("nan","nat","total","итого"))
        d = "Total" if is_total else raw
        im  = imp.iloc[i]  if imp  is not None else None
        ck  = clk.iloc[i]  if clk  is not None else None
        un  = uni.iloc[i]  if uni  is not None else None
        cp  = ctrp.iloc[i] if ctrp is not None else None
        vp  = vtrp.iloc[i] if vtrp is not None else None
        fq  = (freq[i] if isinstance(freq,list) and i < len(freq) else (freq.iloc[i] if hasattr(freq,'iloc') and i<len(freq) else None))
        vis = br = pdpth = ats = None
        rch = "—"
        if not is_total:
            key = norm_date_for_yandex(raw)
            if key and key in ymap:
                vis, br, pdpth, ats = ymap[key]
                rch = reachability(vis, ck)
        rows_html.append(
            "<tr>"
            f"<td>{d}</td>"
            f"<td>{as_int(im)}</td>"
            f"<td>{as_int(ck)}</td>"
            f"<td>{fmt_pct2(cp)}</td>"
            f"<td>{fmt_pct2(vp)}</td>"
            f"<td>{as_int(un)}</td>"
            f"<td>{fq if fq is not None else '—'}</td>"
            f"<td>{as_int(vis) if vis is not None else '—'}</td>"
            f"<td>{rch}</td>"
            f"<td>{fmt_pct2(br) if br is not None else '—'}</td>"
            f"<td>{fmt_float2(pdpth) if pdpth is not None else '—'}</td>"
            f"<td>{fmt_time_sec(ats) if ats is not None else '—'}</td>"
            "</tr>"
        )
    inner = "<table class='table is-narrow is-striped is-fullwidth'>" + head + "<tbody>" + "".join(rows_html) + "</tbody></table>"
    return HTMLResponse(f"<td colspan='11' id='details-{cid}' class='p-0'>{inner}</td>")

@router.get("/campaigns/{cid}/daily5", response_class=HTMLResponse)
def campaigns_daily5(cid: int):
    """
    Daily-таблица:
      • Редактируются: impressions, clicks, uniques, visits, bounce_rate, page_depth, avg_time_sec,
                       + verif_impressions, verif_clicks, verif_ctr_percent, verif_viewability_percent,
                         verif_vtr_percent, verif_unsafe_percent, verif_givt_percent, verif_sivt_percent.
      • Считаются (не редактируются): CTR, VTR, Частота, Доходимость.
      • Verifier: читаем из БД (verifier_daily_metric, с маппингом verifier_campaigns при наличии),
        если БД пусто — пробуем CSV (verif_*/moat_*/ias_*/dv_*).
      • Колонки Verifier и Δ всегда рендерятся (если нет значений — «—»).
      • Δ: проценты — в p.p., счётчики — % к нашим.
      • Total считает всё сам; Охват total — из _campaign_totals.
    """
    import os, sqlite3, math
    import pandas as pd
    from pathlib import Path
    from html import escape as _esc

    # ---------- helpers ----------
    def _date_iso(x):
        s = str(x).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-": return s[:10]              # YYYY-MM-DD
        if len(s) >= 10 and s[2] == "." and s[5] == ".": d,m,y = s[:10].split("."); return f"{y}-{m}-{d}"  # DD.MM.YYYY
        return s

    def _num(x):
        if x is None or (isinstance(x, float) and not math.isfinite(x)): return None
        s = str(x).strip()
        if s == "" or s == "—": return None
        s = s.replace("\xa0","").replace(" ","").replace(",", ".").replace("%","")
        try: return float(s)
        except Exception:
            try:
                if ":" in s:
                    parts = [int(p) for p in s.split(":")]
                    while len(parts) < 3: parts.insert(0,0)
                    h,m,sec = parts[-3],parts[-2],parts[-1]
                    return float(h*3600+m*60+sec)
            except Exception: return None
            return None

    def _fmt_int(v):
        try: return f"{int(round(float(v))):,}".replace(",", " ")
        except Exception: return "—"

    def _fmt_f2(v):
        try: return f"{float(v):.2f}"
        except Exception: return "—"

    def _fmt_pct2(v):
        try: return f"{float(v):.2f}%"
        except Exception: return "—"

    def _fmt_pp2(v):
        try:
            vv = float(v)
            sign = "+" if vv >= 0 else ""
            return f"{sign}{vv:.2f} pp"
        except Exception:
            return "—"

    def _fmt_time(v):
        try:
            v = int(round(float(v))); h, rem = divmod(v, 3600); m, s = divmod(rem, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
        except Exception:
            return "—"

    # кликабельная ячейка
    def cell_btn(d_iso: str, metric: str, txt: str, edited: bool):
        cls = "dcell" + (" is-overridden" if edited else "")
        url = f"/campaigns/{cid}/daily5/cell?date={_esc(d_iso)}&metric={_esc(metric)}"
        return (
            f"<button type='button' class='{cls}' data-url='{url}' title='Кликните для редактирования' "
            f"hx-on=\"click: htmx.ajax('GET', this.dataset.url, {{target:this, swap:'outerHTML'}})\">"
            f"{_esc(txt)}</button>"
        )

    def _find_series(df, names):
        for nm in names:
            if nm in df.columns: return df[nm]
        return None

    # ---------- Cats CSV ----------
    p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
    if not p.exists():
        return HTMLResponse(f"<td colspan='11' id='details-{cid}'><p>No data (daily5)</p></td>")
    df = pd.read_csv(p)

    date_col = _find_series(df, ["date", "День"])
    our_imp  = _find_series(df, ["impressions", "Показы"])
    our_clk  = _find_series(df, ["clicks", "Переходы"])
    our_uniq = _find_series(df, ["uniques", "Охват"])
    vtrp     = _find_series(df, ["vtr_percent", "VTR"])  # не редактируем

    # ---------- Verifier: БД (приоритет) + CSV (фоллбек) ----------
    def _csv_maps():
        maps = {k:{} for k in ("imp","clk","ctrp","vtrp","viewp","unsafe","givt","sivt","meas")}
        if date_col is None: return maps
        cols = {
            "imp":  _find_series(df, ["verif_impressions","verifier_impressions","moat_impressions","ias_impressions","dv_impressions"]),
            "clk":  _find_series(df, ["verif_clicks","verifier_clicks","moat_clicks","ias_clicks","dv_clicks"]),
            "ctrp": _find_series(df, ["verif_ctr_percent","verifier_ctr_percent","moat_ctr","ias_ctr","dv_ctr"]),
            "vtrp": _find_series(df, ["verif_vtr_percent","verifier_vtr_percent","moat_vtr","ias_vtr","dv_vtr"]),
            "viewp":_find_series(df, ["verif_viewability_percent","verifier_viewability_percent","viewability_percent","moat_viewability","ias_viewability","dv_viewability"]),
            "unsafe":_find_series(df, ["unsafe_percent","verifier_unsafe_percent"]),
            "givt": _find_series(df, ["givt_percent","verifier_givt_percent"]),
            "sivt": _find_series(df, ["sivt_percent","verifier_sivt_percent"]),
            "meas": _find_series(df, ["measured_impressions","verifier_measured_impressions","moat_measured_impressions","ias_measured_impressions","dv_measured_impressions"]),
        }
        for i in range(len(df.index)):
            raw = date_col.iloc[i]
            s = str(raw).strip().lower()
            if s in ("","nan","nat","total","итого"): continue
            d = _date_iso(raw)
            for k, series in cols.items():
                if series is not None:
                    maps[k][d] = _num(series.iloc[i])
        return maps

    def _db_maps():
        maps = {k:{} for k in ("imp","clk","ctrp","vtrp","viewp","unsafe","givt","sivt","meas")}
        try:
            dbp = os.path.join(os.getcwd(), "campaign_hub.db")
            con = sqlite3.connect(dbp); cur = con.cursor()

            def table_exists(name):
                return bool(cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())

            def cols(name):
                return [r[1] for r in cur.execute(f"PRAGMA table_info({name})").fetchall()]

            def pick(hay, cand):
                for c in cand:
                    if c in hay: return c
                return None

            # маппинг кампании -> verifier_campaign_id (если есть)
            vid = cid
            if table_exists("verifier_campaigns"):
                cs = cols("verifier_campaigns")
                myc = pick(cs, ["campaign_id","cid","campaign"])
                vrf = pick(cs, ["verifier_campaign_id","verifier_id","verif_campaign_id","vid"])
                if myc and vrf:
                    r = cur.execute(f"SELECT {vrf} FROM verifier_campaigns WHERE {myc}=?", (cid,)).fetchone()
                    if r and r[0] is not None:
                        vid = r[0]

            tname = "verifier_daily_metric"
            if not table_exists(tname):
                con.close()
                return maps

            cs = cols(tname)
            idc   = pick(cs, ["verifier_campaign_id","campaign_id","cid","campaign"])
            dc    = pick(cs, ["date","report_date","day"])
            ic    = pick(cs, ["impressions","impr","imp"])
            kc    = pick(cs, ["clicks","clk"])
            ctrc  = pick(cs, ["ctr_percent","ctr"])
            vtrc  = pick(cs, ["vtr_percent","vtr"])
            viewc = pick(cs, ["viewability_percent","viewability","visible_percent"])
            unsfc = pick(cs, ["unsafe_percent","unsafe"])
            givtc = pick(cs, ["givt_percent","givt"])
            sivtc = pick(cs, ["sivt_percent","sivt"])
            measc = pick(cs, ["measured_impressions","measured","meas_impressions","viewable_measured","view_measured"])
            if not idc or not dc:
                con.close()
                return maps

            q = f"""SELECT {dc},
                            {ic or 'NULL'},
                            {kc or 'NULL'},
                            {ctrc or 'NULL'},
                            {vtrc or 'NULL'},
                            {viewc or 'NULL'},
                            {unsfc or 'NULL'},
                            {givtc or 'NULL'},
                            {sivtc or 'NULL'},
                            {measc or 'NULL'}
                     FROM {tname} WHERE {idc}=?"""
            for row in cur.execute(q, (vid,)).fetchall():
                d = _date_iso(row[0]); 
                if not d: continue
                maps["imp"][d]   = _num(row[1])
                maps["clk"][d]   = _num(row[2])
                maps["ctrp"][d]  = _num(row[3])
                maps["vtrp"][d]  = _num(row[4])
                maps["viewp"][d] = _num(row[5])
                maps["unsafe"][d]= _num(row[6])
                maps["givt"][d]  = _num(row[7])
                maps["sivt"][d]  = _num(row[8])
                maps["meas"][d]  = _num(row[9])
            con.close()
        except Exception:
            pass
        return maps

    maps_csv = _csv_maps()
    maps_db  = _db_maps()
    # приоритет БД; если в БД пусто для ключа — берём CSV
    ver = {k: (maps_db[k] if maps_db[k] else maps_csv[k]) for k in ("imp","clk","ctrp","vtrp","viewp","unsafe","givt","sivt","meas")}

    # ---------- Yandex ----------
    ymap = {}
    try:
        ydbp = os.path.join(os.getcwd(), "yandex_metrics.db")
        con_y = sqlite3.connect(ydbp); cur_y = con_y.cursor()
        for rd, vis, br, pdpth, ats in cur_y.execute(
            "SELECT report_date, visits, bounce_rate, page_depth, avg_time_sec FROM yandex_daily_metrics WHERE campaign_id=?", (cid,)
        ):
            br_pct = br * 100.0 if br is not None else None
            ymap[str(rd)] = (vis, br_pct, pdpth, ats)
        con_y.close()
    except Exception:
        pass

    overrides = _overrides_map(cid)

    # ---------- totals ----------
    sum_imp = sum_clk = sum_vis = 0.0
    w_vtr_num = w_vtr_den = 0.0
    br_w_num = br_w_den = 0.0
    pd_w_num = pd_w_den = 0.0
    ats_w_num = ats_w_den = 0.0

    # verifier totals
    vrf_imp_sum = 0.0
    vrf_clk_sum = 0.0
    vrf_ctr_w_num = vrf_ctr_w_den = 0.0
    vrf_view_w_num = vrf_view_w_den = 0.0
    vrf_vtr_w_num = vrf_vtr_w_den = 0.0
    unsafe_w_num = unsafe_w_den = 0.0
    givt_w_num = givt_w_den = 0.0
    sivt_w_num = sivt_w_den = 0.0

    # ---------- header ----------
    head = (
        "<thead><tr>"
        "<th>Дата</th><th>Показы</th><th>Клики</th><th>CTR</th><th>VTR</th><th>Охват</th><th>Частота</th>"
        "<th>Визиты</th><th>Доходимость</th><th>Отказы</th><th>Глубина</th><th>Время</th>"
        # verifier:
        "<th>Verifier Impr</th><th>Verifier Clicks</th><th>Verifier CTR</th>"
        "<th>Viewability</th><th>Verifier VTR</th><th>Unsafe</th><th>GIVT</th><th>SIVT</th>"
        # deltas:
        "<th>Δ Impr</th><th>Δ Clicks</th><th>Δ CTR</th><th>Δ Viewab.</th><th>Δ VTR</th>"
        "</tr></thead>"
    )

    # ---------- rows ----------
    rows = []
    for i in range(len(df.index)):
        raw = date_col.iloc[i] if date_col is not None else None
        if raw is None or str(raw).strip().lower() in ("nan","nat","total","итого"): continue
        d_iso = _date_iso(raw)

        # база (+ overrides)
        im = our_imp.iloc[i] if our_imp is not None else None
        ck = our_clk.iloc[i] if our_clk is not None else None
        un = our_uniq.iloc[i] if our_uniq is not None else None
        vp = vtrp.iloc[i] if vtrp is not None else None

        ov = overrides.get(d_iso, {})
        if "impressions" in ov: im = ov["impressions"]
        if "clicks"      in ov: ck = ov["clicks"]
        if "uniques"     in ov: un = ov["uniques"]

        vis = br = pdpth = ats = None
        if d_iso in ymap: vis, br, pdpth, ats = ymap[d_iso]
        if "visits"       in ov: vis   = ov["visits"]
        if "bounce_rate"  in ov: br    = ov["bounce_rate"]
        if "page_depth"   in ov: pdpth = ov["page_depth"]
        if "avg_time_sec" in ov: ats   = ov["avg_time_sec"]

        im_v, ck_v, un_v = _num(im), _num(ck), _num(un)
        vp_v             = _num(vp)
        vis_v, br_v      = _num(vis), _num(br)
        pd_v,  ats_v     = _num(pdpth), _num(ats)

        ctr_v   = (100.0*ck_v/im_v) if (im_v and im_v>0 and ck_v is not None) else None
        freq_v  = (im_v/un_v)       if (un_v and un_v>0 and im_v is not None) else None
        reach_v = (100.0*vis_v/ck_v) if (ck_v and ck_v>0 and vis_v is not None) else None

        if im_v  is not None: sum_imp += im_v
        if ck_v  is not None: sum_clk += ck_v
        if vis_v is not None: sum_vis += vis_v
        if vp_v  is not None and im_v is not None: w_vtr_num += (vp_v*im_v); w_vtr_den += im_v
        if br_v  is not None and vis_v is not None: br_w_num += (br_v*vis_v); br_w_den += vis_v
        if pd_v  is not None and vis_v is not None: pd_w_num += (pd_v*vis_v); pd_w_den += vis_v
        if ats_v is not None and vis_v is not None: ats_w_num += (ats_v*vis_v); ats_w_den += vis_v

        # verifier значения на дату
        vi_v   = ver["imp"].get(d_iso)
        vc_v   = ver["clk"].get(d_iso)
        vctr_v = ver["ctrp"].get(d_iso)
        vvtr_v = ver["vtrp"].get(d_iso)
        vview_v= ver["viewp"].get(d_iso)
        vmeas_v= ver["meas"].get(d_iso)
        vunsf_v= ver["unsafe"].get(d_iso)
        vgivt_v= ver["givt"].get(d_iso)
        vsivt_v= ver["sivt"].get(d_iso)

        # учтём оверрайды верификатора
        if "verif_impressions"          in ov: vi_v   = _num(ov["verif_impressions"])
        if "verif_clicks"               in ov: vc_v   = _num(ov["verif_clicks"])
        if "verif_ctr_percent"          in ov: vctr_v = _num(ov["verif_ctr_percent"])
        if "verif_viewability_percent"  in ov: vview_v= _num(ov["verif_viewability_percent"])
        if "verif_vtr_percent"          in ov: vvtr_v = _num(ov["verif_vtr_percent"])
        if "verif_unsafe_percent"       in ov: vunsf_v= _num(ov["verif_unsafe_percent"])
        if "verif_givt_percent"         in ov: vgivt_v= _num(ov["verif_givt_percent"])
        if "verif_sivt_percent"         in ov: vsivt_v= _num(ov["verif_sivt_percent"])
        if "verif_measured_impressions" in ov: vmeas_v= _num(ov["verif_measured_impressions"])

        # totals verifier (взвешивания)
        if vi_v   is not None: vrf_imp_sum += vi_v
        if vc_v   is not None: vrf_clk_sum += vc_v
        if vctr_v is not None:
            w = vi_v if (vi_v is not None) else (im_v if im_v is not None else None)
            if w is not None: vrf_ctr_w_num += (vctr_v*w); vrf_ctr_w_den += w
        if vvtr_v is not None:
            w2 = vi_v if (vi_v is not None) else (im_v if im_v is not None else None)
            if w2 is not None: vrf_vtr_w_num += (vvtr_v*w2); vrf_vtr_w_den += w2
        if vview_v is not None:
            wv = vmeas_v if (vmeas_v is not None) else (vi_v if vi_v is not None else (im_v if im_v is not None else None))
            if wv is not None: vrf_view_w_num += (vview_v*wv); vrf_view_w_den += wv
        if vunsf_v is not None:
            wu = vmeas_v if (vmeas_v is not None) else (vi_v if vi_v is not None else (im_v if im_v is not None else None))
            if wu is not None: unsafe_w_num += (vunsf_v*wu); unsafe_w_den += wu
        if vgivt_v is not None:
            wg = vmeas_v if (vmeas_v is not None) else (vi_v if vi_v is not None else (im_v if im_v is not None else None))
            if wg is not None: givt_w_num += (vgivt_v*wg); givt_w_den += wg
        if vsivt_v is not None:
            ws = vmeas_v if (vmeas_v is not None) else (vi_v if vi_v is not None else (im_v if im_v is not None else None))
            if ws is not None: sivt_w_num += (vsivt_v*ws); sivt_w_den += ws

        # deltas
        d_imp    = (vi_v / im_v - 1.0) * 100.0 if (vi_v is not None and im_v and im_v>0) else None
        d_clk    = (vc_v / ck_v - 1.0) * 100.0 if (vc_v is not None and ck_v and ck_v>0) else None
        d_ctr_pp = (vctr_v - ctr_v) if (vctr_v is not None and ctr_v is not None) else None
        d_view_pp= (vview_v - 0.0) if (vview_v is not None) else None
        d_vtr_pp = (vvtr_v - vp_v) if (vvtr_v is not None and vp_v is not None) else None

        # строка
        from html import escape as _esc2
        row_html = (
            "<tr>"
            f"<td>{_esc2(str(raw))}</td>"
            f"<td>{cell_btn(d_iso,'impressions', _fmt_int(im_v), 'impressions' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'clicks',      _fmt_int(ck_v), 'clicks' in ov)}</td>"
            f"<td><span class='dcell is-static' id='ctr-{cid}-{d_iso}'>{_fmt_pct2(ctr_v)}</span></td>"
            f"<td><span class='dcell is-static' id='vtr-{cid}-{d_iso}'>{_fmt_pct2(vp_v)}</span></td>"
            f"<td>{cell_btn(d_iso,'uniques',     _fmt_int(un_v), 'uniques' in ov)}</td>"
            f"<td><span class='dcell is-static' id='freq-{cid}-{d_iso}'>{_fmt_f2(freq_v)}</span></td>"
            f"<td>{cell_btn(d_iso,'visits',      _fmt_int(vis_v), 'visits' in ov)}</td>"
            f"<td><span class='dcell is-static' id='rch-{cid}-{d_iso}'>{_fmt_pct2(reach_v)}</span></td>"
            f"<td>{cell_btn(d_iso,'bounce_rate', _fmt_pct2(br_v), 'bounce_rate' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'page_depth',  _fmt_f2(pd_v), 'page_depth' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'avg_time_sec',_fmt_time(ats_v), 'avg_time_sec' in ov)}</td>"
            # verifier — ТЕПЕРЬ кликабельные:
            f"<td>{cell_btn(d_iso,'verif_impressions',         _fmt_int(vi_v),    'verif_impressions' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_clicks',              _fmt_int(vc_v),    'verif_clicks' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_ctr_percent',         _fmt_pct2(vctr_v), 'verif_ctr_percent' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_viewability_percent', _fmt_pct2(vview_v),'verif_viewability_percent' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_vtr_percent',         _fmt_pct2(vvtr_v), 'verif_vtr_percent' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_unsafe_percent',      _fmt_pct2(vunsf_v),'verif_unsafe_percent' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_givt_percent',        _fmt_pct2(vgivt_v),'verif_givt_percent' in ov)}</td>"
            f"<td>{cell_btn(d_iso,'verif_sivt_percent',        _fmt_pct2(vsivt_v),'verif_sivt_percent' in ov)}</td>"
            # deltas (идентификаторы для OOB)
            f"<td><span class='dcell is-static' id='dlt-imp-{cid}-{d_iso}'>{_fmt_pct2(d_imp)}</span></td>"
            f"<td><span class='dcell is-static' id='dlt-clk-{cid}-{d_iso}'>{_fmt_pct2(d_clk)}</span></td>"
            f"<td><span class='dcell is-static' id='dlt-ctr-{cid}-{d_iso}'>{_fmt_pp2(d_ctr_pp)}</span></td>"
            f"<td><span class='dcell is-static' id='dlt-view-{cid}-{d_iso}'>{_fmt_pp2(d_view_pp)}</span></td>"
            f"<td><span class='dcell is-static' id='dlt-vtr-{cid}-{d_iso}'>{_fmt_pp2(d_vtr_pp)}</span></td>"
            "</tr>"
        )
        rows.append(row_html)

    # ---------- totals ----------
    totals = _campaign_totals(cid) or {}
    reach_total = totals.get("uniques", None)

    ctr_total  = (100.0*sum_clk/sum_imp) if (sum_imp and sum_imp>0) else None
    vtr_total  = (w_vtr_num/w_vtr_den)   if (w_vtr_den and w_vtr_den>0) else None
    freq_total = (sum_imp/reach_total)   if (reach_total and reach_total>0) else None
    br_total   = (br_w_num/br_w_den)     if (br_w_den and br_w_den>0) else None
    pd_total   = (pd_w_num/pd_w_den)     if (pd_w_den and pd_w_den>0) else None
    ats_total  = (ats_w_num/ats_w_den)   if (ats_w_den and ats_w_den>0) else None

    vrf_ctr_total   = (vrf_ctr_w_num/vrf_ctr_w_den)   if (vrf_ctr_w_den  and vrf_ctr_w_den>0)   else None
    vrf_view_total  = (vrf_view_w_num/vrf_view_w_den) if (vrf_view_w_den and vrf_view_w_den>0)  else None
    vrf_vtr_total   = (vrf_vtr_w_num/vrf_vtr_w_den)   if (vrf_vtr_w_den  and vrf_vtr_w_den>0)   else None
    unsafe_total    = (unsafe_w_num/unsafe_w_den)     if (unsafe_w_den   and unsafe_w_den>0)    else None
    givt_total      = (givt_w_num/givt_w_den)         if (givt_w_den     and givt_w_den>0)      else None
    sivt_total      = (sivt_w_num/sivt_w_den)         if (sivt_w_den     and sivt_w_den>0)      else None

    d_imp_tot    = ((vrf_imp_sum/sum_imp - 1.0)*100.0) if (vrf_imp_sum and sum_imp and sum_imp>0) else None
    d_clk_tot    = ((vrf_clk_sum/sum_clk - 1.0)*100.0) if (vrf_clk_sum and sum_clk and sum_clk>0) else None
    d_ctr_pp_tot = (vrf_ctr_total - ctr_total) if (vrf_ctr_total is not None and ctr_total is not None) else None
    d_view_pp_tot= (vrf_view_total - 0.0) if (vrf_view_total is not None) else None
    d_vtr_pp_tot = (vrf_vtr_total - vtr_total) if (vrf_vtr_total is not None and vtr_total is not None) else None

    foot = (
        "<tfoot><tr>"
        "<th>Total</th>"
        f"<th><span id='tot-impr-{cid}'>{_fmt_int(sum_imp)}</span></th>"
        f"<th><span id='tot-clk-{cid}'>{_fmt_int(sum_clk)}</span></th>"
        f"<th><span id='tot-ctr-{cid}'>{_fmt_pct2(ctr_total)}</span></th>"
        f"<th><span id='tot-vtr-{cid}'>{_fmt_pct2(vtr_total)}</span></th>"
        f"<th><span id='tot-uniq-{cid}'>{_fmt_int(reach_total)}</span></th>"
        f"<th><span id='tot-freq-{cid}'>{_fmt_f2(freq_total)}</span></th>"
        f"<th><span id='tot-vis-{cid}'>{_fmt_int(sum_vis)}</span></th>"
        f"<th><span>{_fmt_pct2((100.0*sum_vis/sum_clk) if (sum_clk and sum_clk>0) else None)}</span></th>"
        f"<th><span id='tot-br-{cid}'>{_fmt_pct2(br_total)}</span></th>"
        f"<th><span id='tot-pd-{cid}'>{_fmt_f2(pd_total)}</span></th>"
        f"<th><span id='tot-ats-{cid}'>{_fmt_time(ats_total)}</span></th>"
        # verifier totals:
        f"<th><span id='tot-vrf-imp-{cid}'>{_fmt_int(vrf_imp_sum)}</span></th>"
        f"<th><span id='tot-vrf-clk-{cid}'>{_fmt_int(vrf_clk_sum)}</span></th>"
        f"<th><span id='tot-vrf-ctr-{cid}'>{_fmt_pct2(vrf_ctr_total)}</span></th>"
        f"<th><span id='tot-vrf-view-{cid}'>{_fmt_pct2(vrf_view_total)}</span></th>"
        f"<th><span id='tot-vrf-vtr-{cid}'>{_fmt_pct2(vrf_vtr_total)}</span></th>"
        f"<th><span id='tot-vrf-unsafe-{cid}'>{_fmt_pct2(unsafe_total)}</span></th>"
        f"<th><span id='tot-vrf-givt-{cid}'>{_fmt_pct2(givt_total)}</span></th>"
        f"<th><span id='tot-vrf-sivt-{cid}'>{_fmt_pct2(sivt_total)}</span></th>"
        # deltas totals:
        f"<th><span id='tot-delta-imp-{cid}'>{_fmt_pct2(d_imp_tot)}</span></th>"
        f"<th><span id='tot-delta-clk-{cid}'>{_fmt_pct2(d_clk_tot)}</span></th>"
        f"<th><span id='tot-delta-ctr-{cid}'>{_fmt_pp2(d_ctr_pp_tot)}</span></th>"
        f"<th><span id='tot-delta-view-{cid}'>{_fmt_pp2(d_view_pp_tot)}</span></th>"
        f"<th><span id='tot-delta-vtr-{cid}'>{_fmt_pp2(d_vtr_pp_tot)}</span></th>"
        "</tr></tfoot>"
    )

    inner = "<table class='table is-narrow is-striped is-fullwidth'>" + head + "<tbody>" + "".join(rows) + "</tbody>" + foot + "</table>"
    return HTMLResponse(f"<td colspan='11' id='details-{cid}' class='p-0'>{inner}</td>")



@router.post("/campaigns/{cid}/yandex_update", response_class=HTMLResponse)
def campaigns_yandex_update(cid: int):
    from pathlib import Path as _Path
    try:
        base_dir = _Path(__file__).resolve().parents[2]
        script_path = base_dir / "scripts" / "yandex_import.py"
        import sys as _sys, subprocess as _sub
        r = _sub.run([_sys.executable, "-u", str(script_path)], capture_output=True, text=True, timeout=600)
        if r.returncode == 0:
            return HTMLResponse('<span class="tag is-success">Yandex imported</span>')
        err = (r.stderr or r.stdout) or ""
        err = err[-4000:].replace("<","&lt;").replace(">","&gt;")
        return HTMLResponse(f'<span class="tag is-danger">Yandex import failed</span><pre>{err}</pre>', status_code=500)
    except Exception as e:
        return HTMLResponse(f'<span class="tag is-danger">Yandex error</span> <small>{e}</small>', status_code=500)



@router.get("/campaigns", response_class=HTMLResponse)
def campaigns_list_view(request: Request):
    # query params
    q = request.query_params.get("q", "") or ""
    sort = request.query_params.get("sort", "id")
    direction = request.query_params.get("dir", "desc")
    direction = direction if direction in ("asc", "desc") else "desc"

    campaigns = _ensure_list()

    # подтянем метрики перед сортировкой
    for c in campaigns:
        _apply_totals(c)

    # фильтр
    q_norm = q.strip().lower()
    if q_norm:
        def _match(c):
            name = (getattr(c, "name", "") or "").lower()
            if q_norm in name:
                return True
            try:
                return str(int(c.id)) == q_norm
            except Exception:
                return False
        campaigns = [c for c in campaigns if _match(c)]

    # сортировка
    keymap = {
        "id":          lambda c: int(getattr(c, "id", 0)),
        "name":        lambda c: (getattr(c, "name", "") or "").lower(),
        "impressions": lambda c: getattr(c, "impressions", -1) or -1,
        "clicks":      lambda c: getattr(c, "clicks", -1) or -1,
        "uniques":     lambda c: getattr(c, "uniques", -1) or -1,
        "ctr_ext":     lambda c: getattr(c, "ctr_ext", -1.0) if getattr(c, "ctr_ext", None) is not None else -1.0,
        "freq":        lambda c: getattr(c, "freq", -1.0) if getattr(c, "freq", None) is not None else -1.0,
    }
    keyfunc = keymap.get(sort, keymap["id"])
    rev = (direction == "desc")
    campaigns = sorted(campaigns, key=keyfunc, reverse=rev)

    return templates.TemplateResponse("campaigns.html", {
        "request": request, "campaigns": campaigns, "q": q, "sort": sort, "dir": direction
    })


@router.post("/campaigns", response_class=HTMLResponse)
def campaigns_add(id: int = Form(...), name: str = Form(...)):
    try:
        from app.services import crud
        if hasattr(crud, "add"):
            crud.add(id=id, name=name)
        elif hasattr(crud, "add_campaign"):
            crud.add_campaign(id=id, name=name)
        else:
            raise RuntimeError("crud.add not found")
    except Exception:
        # fallback в локальную SQLite
        import sqlite3, os
        dbp = os.path.join(os.getcwd(), "campaign_hub.db")
        con = sqlite3.connect(dbp)
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS campaigns (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT OR IGNORE INTO campaigns (id, name) VALUES (?,?)", (id, name))
        con.commit(); con.close()
    return RedirectResponse(url="/campaigns", status_code=303)


@router.get("/campaigns/", response_class=HTMLResponse)
def campaigns_list_view_slash(request: Request):
    return campaigns_list_view(request)

def _parse_spreadsheetml_campaigns(xml_bytes: bytes):
    """
    Возвращает список (cid:int, name:str) из таблицы 'Кампании' (SpreadsheetML XML).
    Ищет заголовки 'ID' и 'Название', есть fallback на индексы (1,2).
    """
    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
    root = ET.fromstring(xml_bytes)
    ws = root.find(".//ss:Worksheet", ns)
    if ws is None:
        return []
    table = ws.find("ss:Table", ns)
    if table is None:
        return []
    rows = table.findall("ss:Row", ns)
    if not rows:
        return []

    # заголовок
    header = []
    for cell in rows[0].findall("ss:Cell", ns):
        d = cell.find("ss:Data", ns)
        header.append(d.text if d is not None else None)

    try:
        id_idx = header.index("ID")
        name_idx = header.index("Название")
    except Exception:
        id_idx, name_idx = 1, 2  # fallback как в твоём примере

    out = []
    for r in rows[1:]:
        vals = []
        for cell in r.findAll("ss:Cell", ns) if hasattr(r, "findAll") else r.findall("ss:Cell", ns):
            d = cell.find("ss:Data", ns)
            vals.append(d.text if d is not None else None)
        need = max(id_idx, name_idx) + 1
        if len(vals) < need:
            vals += [None]*(need - len(vals))
        cid_raw = vals[id_idx]
        name = vals[name_idx]
        n = (name or "").strip().lower()
        if "foxible" in n or "test" in n or "тест" in n:
            continue

        try:
            cid = int(str(cid_raw).strip()) if cid_raw not in (None, "", "None") else None
        except Exception:
            cid = None
        if cid:
            out.append((cid, name))
    return out


@router.post("/campaigns/import_cats", response_class=HTMLResponse)
def campaigns_import_cats():
    """
    Массовый импорт списка РК из Cats (SpreadsheetML XML).
    Авторизация — через _ensure_session(); добавляем только новые ID.
    Возвращаем <tr> (ошибки и статус) либо набор новых строк таблицы.
    """
    import traceback
    import xml.etree.ElementTree as ET

    def msg_row(text, cls="is-warning"):
        return HTMLResponse(f"<tr><td colspan='11'><span class='tag {cls}'>{text}</span></td></tr>")

    # 1) Конфиг system (ВАЖНО: тут уже секция system!)
    syscfg = get_effective_system_config("config.yaml") or {}
    base = (syscfg.get("connect_url") or syscfg.get("base_url") or "").rstrip("/")
    if not base:
        return msg_row("No connect_url/base_url in system section", "is-danger")

    # 2) Авторизованная сессия Cats
    try:
        s = _ensure_session()
    except Exception as e:
        return msg_row(f"Cats auth failed: {e}", "is-danger")

    # 3) Первый день текущего месяца
    date_str = date.today().replace(day=1).strftime("%d.%m.%Y")

    # 4) Экспорт «все кампании» (через сессию, без редиректа на login)
    export_url = (f"{base}/campaigns/show/main/?limit_date_begin={date_str}"
                  "&limit_date_end=&name=&campaign_id=&mediaplan_id=&agency_id=&advertiser_id="
                  "&manager_id=&organization_id=4&with%5B0%5D=formats&with%5B1%5D=category&export=xls")

    r = s.get(export_url, timeout=60, allow_redirects=False)
    loc = r.headers.get("Location", "")
    if r.status_code in (301, 302, 303, 307, 308) and "home/login" in (loc or ""):
        return msg_row("Cats session expired: redirected to login.", "is-danger")
    if r.status_code != 200:
        return msg_row(f"Cats export HTTP {r.status_code}", "is-danger")

    head = r.content[:800].decode("utf-8", errors="ignore").lower()
    if "<html" in head and "login" in head:
        return msg_row("Cats returned login page (auth required).", "is-danger")
    if "<workbook" not in head and "<?xml" not in head:
        snippet = head.replace("<", "&lt;").replace(">", "&gt;")
        return msg_row(f"Cats returned non-XML. Snippet: {snippet}", "is-danger")

    # 5) Парс SpreadsheetML -> (id, name)
    try:
        ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
        root = ET.fromstring(r.content)
        ws = root.find(".//ss:Worksheet", ns)
        table = ws.find("ss:Table", ns) if ws is not None else None
        rows = table.findall("ss:Row", ns) if table is not None else []
        if not rows:
            return msg_row("Empty export (no rows).", "is-light")

        header = []
        for c in rows[0].findall("ss:Cell", ns):
            d = c.find("ss:Data", ns)
            header.append(d.text if d is not None else None)
        try:
            id_idx = header.index("ID")
            name_idx = header.index("Название")
        except Exception:
            id_idx, name_idx = 1, 2  # fallback

        pairs = []
        for r0 in rows[1:]:
            vals = []
            for c in r0.findall("ss:Cell", ns):
                d = c.find("ss:Data", ns)
                vals.append(d.text if d is not None else None)
            need = max(id_idx, name_idx) + 1
            if len(vals) < need:
                vals += [None] * (need - len(vals))
            cid_raw = vals[id_idx]
            name = vals[name_idx]
            n = (name or "").strip().lower()
            if "foxible" in n or "test" in n or "тест" in n:
                continue

            try:
                cid = int(str(cid_raw).strip())
            except Exception:
                cid = None
            if cid:
                pairs.append((cid, name))
    except Exception as e:
        return msg_row(f"Parse error: {e}", "is-danger")

    if not pairs:
        return msg_row("No campaigns in export.", "is-light")

    # 6) Добавляем только новые
    existing = set(int(getattr(c, "id")) for c in _ensure_list())
    new_ids = []
    for cid, name in pairs:
        n = (name or "").strip().lower()
        if "foxible" in n or "test" in n or "тест" in n:
            continue

        if cid in existing or cid in new_ids:
            continue
        try:
            try:
                from app.services import crud
                if hasattr(crud, "add"):
                    crud.add(id=cid, name=name)
                elif hasattr(crud, "add_campaign"):
                    crud.add_campaign(id=cid, name=name)
                else:
                    raise RuntimeError("crud.add not found")
            except Exception:
                dbp = os.path.join(os.getcwd(), "campaign_hub.db")
                con = sqlite3.connect(dbp)
                cur = con.cursor()
                cur.execute("CREATE TABLE IF NOT EXISTS campaigns (id INTEGER PRIMARY KEY, name TEXT)")
                cur.execute("INSERT OR IGNORE INTO campaigns (id, name) VALUES (?,?)", (cid, name))
                con.commit(); con.close()
            new_ids.append(cid)
        except Exception:
            print("Import row failed:\n", traceback.format_exc())

    if not new_ids:
        return msg_row("No new campaigns (nothing to add).", "is-light")

    # 7) Рендерим только новые строки
    latest = _ensure_list()
    by_id = {int(getattr(x, "id")): x for x in latest}
    html_parts = []
    for cid in new_ids:
        c = by_id.get(cid)
        if not c:
            continue
        _apply_totals(c)
        row_resp = _render_row_html(c)
        html_parts.append(row_resp.body.decode("utf-8", errors="ignore"))

    return HTMLResponse("".join(html_parts))





@router.get("/campaigns/{cid}/row", response_class=HTMLResponse)
def campaigns_row(cid: int):
    c = _find_campaign(cid)
    if not c:
        return HTMLResponse("", status_code=404)
    _apply_totals(c)
    return _render_row_html(c)

@router.get("/campaigns/groups", response_class=HTMLResponse)
def campaigns_groups(request: Request, q: str = "", sort: str = "id", dir: str = "desc"):
    # Собираем список групп и агрегаты по ним
    with engine.begin() as conn:
        groups = conn.execute(text("SELECT id, COALESCE(name,'') AS name FROM campaign_groups ORDER BY id")).fetchall()
        # Число участников
        members_count = dict(conn.execute(text("""
            SELECT group_id, COUNT(*) AS c
            FROM campaign_group_members
            GROUP BY group_id
        """)).fetchall())

        # Агрегаты из raw_system_daily (предпочтительно)
        agg_rs = { gid: (impr, clk, reach, v100) for gid, impr, clk, reach, v100 in conn.execute(text("""
            SELECT m.group_id,
                   SUM(r.impressions) AS impr,
                   SUM(r.clicks)      AS clk,
                   SUM(r.reach)       AS reach,
                   SUM(r.view_100)    AS v100
            FROM campaign_group_members m
            JOIN raw_system_daily r ON r.campaign_id = m.campaign_id
            GROUP BY m.group_id
        """)).fetchall() }

        # Фоллбек по fact_daily (если у группы нет rsd)
        agg_fd = { gid: (impr, clk) for gid, impr, clk in conn.execute(text("""
            SELECT m.group_id, SUM(f.impressions) AS impr, SUM(f.clicks) AS clk
            FROM campaign_group_members m
            JOIN fact_daily f ON f.campaign_id = m.campaign_id
            GROUP BY m.group_id
        """)).fetchall() }

    records = []
    for r in groups:
        gid = int(r[0]); name = r[1]
        if q:
            if q.isdigit():
                if str(gid).find(q) < 0 and name.lower().find(q.lower()) < 0: continue
            else:
                if name.lower().find(q.lower()) < 0: continue

        if gid in agg_rs:
            impr, clk, reach, v100 = agg_rs[gid]
        else:
            # fallback к fact_daily
            impr, clk = agg_fd.get(gid, (0, 0))
            reach, v100 = 0, 0

        impr = int(impr or 0); clk = int(clk or 0); reach = int(reach or 0); v100 = int(v100 or 0)
        ctr = (clk / impr) if impr else None
        freq = (impr / reach) if (reach and reach > 0) else None
        vtr = (v100 / impr) if impr else None

        records.append({
            "id": gid,
            "name": name,
            "members": int(members_count.get(gid, 0) or 0),
            "impr": impr, "clk": clk, "reach": reach,
            "ctr": ctr, "freq": freq, "vtr": vtr
        })

    key = (sort or "id")
    rev = (dir or "desc") != "asc"
    def kf(x):
        if key in ("impr","clk","reach","ctr","freq","vtr"):
            return x[key] if x[key] is not None else -1
        if key == "name":
            return x["name"].lower()
        return x["id"]
    records.sort(key=kf, reverse=rev)

    return templates.TemplateResponse("campaigns_groups.html",
        {"request": request, "groups": records, "q": q, "sort": sort, "dir": dir})

@router.get("/campaigns/groups/{gid}/daily", response_class=HTMLResponse)
def campaigns_groups_daily(request: Request, gid: int):
    # Чтение группы и членов
    with engine.begin() as conn:
        g = conn.execute(text("SELECT id, COALESCE(name,'') FROM campaign_groups WHERE id=:gid"), {"gid": gid}).fetchone()
        if not g:
            return HTMLResponse(f"<div class='notification is-danger'>Group {gid} not found</div>", status_code=404)
        members = [int(r[0]) for r in conn.execute(
            text("SELECT campaign_id FROM campaign_group_members WHERE group_id=:gid"), {"gid": gid}
        ).fetchall()]

    rows = []
    if not members:
        return templates.TemplateResponse("campaigns_groups_daily.html",
            {"request": request, "group": {"id": int(g[0]), "name": g[1]}, "members": members, "rows": rows})

    # Агрегация по raw_system_daily (предпочтительно)
    with engine.begin() as conn:
        rsd = conn.execute(text("""
            SELECT r.date,
                   SUM(r.impressions) AS impr,
                   SUM(r.clicks)      AS clk,
                   SUM(r.reach)       AS reach,
                   SUM(r.view_100)    AS v100
              FROM campaign_group_members m
              JOIN raw_system_daily r ON r.campaign_id = m.campaign_id
             WHERE m.group_id=:gid
          GROUP BY r.date
          ORDER BY r.date
        """), {"gid": gid}).fetchall()

    if rsd:
        total_impr = 0
        total_clk  = 0
        total_reach_raw = 0
        total_v100 = 0

        for d, impr, clk, reach, v100 in rsd:
            impr  = int(impr or 0)
            clk   = int(clk  or 0)
            reach = int(reach or 0) if reach is not None else None
            v100f = float(v100 or 0.0)

            total_impr += impr
            total_clk  += clk
            total_v100 += v100f
            if reach is not None:
                total_reach_raw += reach

            ctr  = (clk / impr) if impr else None
            freq = (impr / reach) if (reach and reach > 0) else None
            vtr  = (v100f / impr) if impr and v100f is not None else None

            rows.append({"date": d, "impr": impr, "clk": clk, "reach": reach, "ctr": ctr, "freq": freq, "vtr": vtr})

        # --- TOTAL строка ---
        total_reach_adj = int(round(total_reach_raw / 1.17)) if total_reach_raw > 0 else None
        total_ctr  = (total_clk / total_impr) if total_impr else None
        total_freq = (total_impr / total_reach_adj) if (total_reach_adj and total_reach_adj > 0) else None
        total_vtr  = (total_v100 / total_impr) if total_impr and total_v100 is not None else None

        rows.append({
            "date": "Total",
            "impr": int(total_impr),
            "clk":  int(total_clk),
            "reach": total_reach_adj,
            "ctr":  total_ctr,
            "freq": total_freq,
            "vtr":  total_vtr
        })

    else:
        # Фоллбек по fact_daily (reach/view_100 могут отсутствовать)
        with engine.begin() as conn:
            fdf = conn.execute(text("""
                SELECT f.date, SUM(f.impressions) AS impr, SUM(f.clicks) AS clk
                  FROM campaign_group_members m
                  JOIN fact_daily f ON f.campaign_id = m.campaign_id
                 WHERE m.group_id=:gid
              GROUP BY f.date
              ORDER BY f.date
            """), {"gid": gid}).fetchall()

        total_impr = 0
        total_clk  = 0

        for d, impr, clk in fdf:
            impr = int(impr or 0)
            clk  = int(clk  or 0)
            total_impr += impr
            total_clk  += clk
            ctr = (clk / impr) if impr else None
            rows.append({"date": d, "impr": impr, "clk": clk, "reach": None, "ctr": ctr, "freq": None, "vtr": None})

        # TOTAL без reach/v100 (данных нет во фоллбеке)
        total_ctr = (total_clk / total_impr) if total_impr else None
        rows.append({
            "date": "Total",
            "impr": int(total_impr),
            "clk":  int(total_clk),
            "reach": None,
            "ctr":  total_ctr,
            "freq": None,
            "vtr":  None
        })

    return templates.TemplateResponse(
        "campaigns_groups_daily.html",
        {"request": request, "group": {"id": int(g[0]), "name": g[1]}, "members": members, "rows": rows}
    )



@router.post("/campaigns/groups")
def campaigns_groups_create(payload: dict = Body(...)):
    name = str(payload.get("name", "")).strip()
    ids = payload.get("campaign_ids") or []
    if not name:
        return JSONResponse(status_code=400, content={"error": "Group name is required"})
    if not ids or not isinstance(ids, list):
        return JSONResponse(status_code=400, content={"error": "At least one campaign ID is required"})

    # Валидация: все ID должны существовать в campaigns
    ids_clean = []
    bad = []
    for x in ids:
        try:
            xid = int(x)
            ids_clean.append(xid)
        except Exception:
            bad.append(x)
    with engine.begin() as conn:
        if ids_clean:
            rows = conn.execute(text(f"SELECT id FROM campaigns WHERE id IN ({','.join([':i'+str(n) for n,_ in enumerate(ids_clean)])})"),
                                { 'i'+str(n): v for n, v in enumerate(ids_clean)}).fetchall()
            have = set(int(r[0]) for r in rows)
            for xid in ids_clean:
                if xid not in have:
                    bad.append(xid)

    if bad:
        return JSONResponse(status_code=400, content={"error": "ID not found", "bad_ids": [str(x) for x in bad]})

    # Создаём группу и членов
    with engine.begin() as conn:
        gid = conn.execute(text("INSERT INTO campaign_groups(name) VALUES(:name)"), {"name": name}).lastrowid
        # INSERT OR IGNORE (на случай дублей)
        conn.execute(text("INSERT OR IGNORE INTO campaign_group_members(group_id, campaign_id) VALUES " + ",".join([f"(:gid, :c{n})" for n,_ in enumerate(ids_clean)])),
                     dict({"gid": gid}, **{ f"c{n}": v for n, v in enumerate(ids_clean)}))
    return {"status": "ok"}

@router.get("/campaigns/creatives", response_class=HTMLResponse)
def campaigns_creatives(request: Request):
    return templates.TemplateResponse("campaigns_creatives.html", {"request": request})

@router.post("/campaigns/pull_all", response_class=HTMLResponse)
def campaigns_pull_all():
    """
    Массовый апдейт всех кампаний.
    Возвращает:
      1) краткий summary для #bulk-status (hx-target у кнопки),
      2) набор out-of-band <span ... hx-swap-oob="true"> для обновления ячеек
         всех строк таблицы и статусов #camp-<cid>-status.
    """
    try:
        from app.services.cats_export import export_and_ingest
    except Exception as e:
        msg = str(e).replace("<","&lt;").replace(">","&gt;")
        return HTMLResponse(f"<span class='tag is-danger'>Init error: {msg}</span>", status_code=500)

    campaigns = _ensure_list()
    total = len(campaigns)
    ok = 0
    oob_parts = []

    def span_metric(cid, key, val):
        # Обновляем <span id="imp-<cid>"> ... и т.д.
        return f'<span id="{key}-{cid}" hx-swap-oob="true">{val}</span>'

    for c in campaigns:
        cid = int(getattr(c, "id"))
        try:
            res = export_and_ingest(str(cid))
            rows = res.get("ingest", {}).get("rows", "-")

            totals = _campaign_totals(cid) or {}
            imp = totals.get("impressions", "–")
            clk = totals.get("clicks", "–")
            uni = totals.get("uniques", "–")
            ctr = "{:.2%}".format(totals["ctr_ratio"]) if totals.get("ctr_ratio") is not None else "–"
            fr  = "{:.2f}".format(totals["freq"])      if totals.get("freq")      is not None else "–"

            # статус в ячейке действий
            oob_parts.append(
                f'<span id="camp-{cid}-status" class="tag is-success" hx-swap-oob="true">OK · {rows} rows</span>'
            )
            # метрики в строке
            oob_parts.extend([
                span_metric(cid, "imp",  imp),
                span_metric(cid, "clk",  clk),
                span_metric(cid, "uniq", uni),
                span_metric(cid, "ctr",  ctr),
                span_metric(cid, "freq", fr),
            ])

            ok += 1
        except Exception as e:
            err = str(e).replace("<","&lt;").replace(">","&gt;")[:300]
            oob_parts.append(
                f'<span id="camp-{cid}-status" class="tag is-danger" hx-swap-oob="true">FAIL</span>'
            )

    summary = f"<span class='tag is-info'>Updated {ok} / {total}</span>"
    # summary уедет в #bulk-status, всё остальное — OOB-апдейты по id-элементам таблицы
    return HTMLResponse(summary + "".join(oob_parts))


@router.post("/campaigns/{cid}/daily5/save", response_class=HTMLResponse)
def campaigns_daily5_save(cid: int, date: str = Form(...), metric: str = Form(...), value: str = Form("")):
    """
    Сохраняем базовую/верификаторскую метрику, пересчитываем суточные CTR/Freq/Reachability,
    Δ по дню (включая Δ Viewability) и тотальные Δ и тотал‑поля верификатора. Возвращаем кнопку‑ячейку + OOB.
    """
    import os, sqlite3, math
    import pandas as pd
    from pathlib import Path
    from html import escape as _esc

    def _date_iso(x):
        s = str(x).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-": return s[:10]
        if len(s) >= 10 and s[2] == "." and s[5] == ".": d,m,y = s[:10].split("."); return f"{y}-{m}-{d}"
        return s

    def _num(x):
        if x is None or (isinstance(x, float) and not math.isfinite(x)): return None
        s = str(x).strip()
        if s == "" or s == "—": return None
        s = s.replace("\xa0","").replace(" ","").replace(",", ".").replace("%","")
        try: return float(s)
        except Exception:
            try:
                if ":" in s:
                    parts = [int(p) for p in s.split(":")]
                    while len(parts) < 3: parts.insert(0,0)
                    h,m,sec = parts[-3],parts[-2],parts[-1]; return float(h*3600+m*60+sec)
            except Exception: return None
            return None

    def _fmt_int(v):
        try: return f"{int(round(float(v))):,}".replace(",", " ")
        except Exception: return "—"

    def _fmt_f2(v):
        try: return f"{float(v):.2f}"
        except Exception: return "—"

    def _fmt_pct2(v):
        try: return f"{float(v):.2f}%"
        except Exception: return "—"

    def _fmt_pp2(v):
        try:
            vv = float(v)
            sign = "+" if vv >= 0 else ""
            return f"{sign}{vv:.2f} pp"
        except Exception:
            return "—"

    def _fmt_time(v):
        try:
            v = int(round(float(v))); h, rem = divmod(v, 3600); m, s = divmod(rem, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
        except Exception:
            return "—"

    editable_metrics = {
        "impressions","clicks","uniques","visits","bounce_rate","page_depth","avg_time_sec",
        "verif_impressions","verif_clicks","verif_ctr_percent","verif_viewability_percent",
        "verif_vtr_percent","verif_unsafe_percent","verif_givt_percent","verif_sivt_percent",
        "verif_measured_impressions"
    }

    d_iso = _date_iso(date)
    val_clean = (value or "").strip()
    if metric not in editable_metrics:
        return HTMLResponse("<span class='dcell is-static'>—</span>")

    # сохранить override
    if val_clean == "" or val_clean == "—":
        _save_override(cid, d_iso, metric, None)
        edited = False
    else:
        _save_override(cid, d_iso, metric, val_clean)
        edited = True

    # ----- Маржинальность -----

def _ensure_margin_table():
    """
    Таблицы для расчёта маржинальности по брони (история + актуальное).

    1) margin_stats — "последнее значение" по каждому booking_id на месяц + комментарий:
        booking_id        — ID строки в bookings
        campaign_id       — ID кампании в Cats
        month             — YYYY-MM-01 (первый день месяца, чтобы отличать периоды)
        metric_type       — 'cpm' или 'cpc'
        client_price      — клиентская цена (из Bookings), CPM или CPC
        cats_avg_price    — средний CPM/CPC из shortage-выгрузки Cats
        purchase_percent  — (cats_avg_price / client_price) * 100
        comment           — комментарий в UI
        updated_at        — ISO‑штамп

    2) margin_snapshots — история "накопительным эффектом":
        snapshot_date     — YYYY-MM-DD (дата выгрузки/апдейта)
        Остальные поля — те же, что и в margin_stats, но с ключом (booking_id, month, snapshot_date)
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS margin_stats(
                booking_id       INTEGER NOT NULL,
                campaign_id      INTEGER,
                month            TEXT    NOT NULL,
                metric_type      TEXT    NOT NULL,
                client_price     REAL,
                cats_avg_price   REAL,
                purchase_percent REAL,
                comment          TEXT,
                updated_at       TEXT,
                PRIMARY KEY (booking_id, month)
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_margin_month    ON margin_stats(month);"))        
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_margin_campaign ON margin_stats(campaign_id);"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS margin_snapshots(
                booking_id       INTEGER NOT NULL,
                campaign_id      INTEGER,
                month            TEXT    NOT NULL,
                snapshot_date    TEXT    NOT NULL,  -- YYYY-MM-DD
                metric_type      TEXT    NOT NULL,  -- cpm/cpc
                client_price     REAL,
                cats_avg_price   REAL,
                purchase_percent REAL,
                updated_at       TEXT,
                PRIMARY KEY (booking_id, month, snapshot_date)
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_margin_snap_month  ON margin_snapshots(month);"))        
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_margin_snap_date   ON margin_snapshots(snapshot_date);"))        
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_margin_snap_metric ON margin_snapshots(metric_type);"))

def _purchase_percent_class(p) -> str:
    """
    Подбор цвета в UI:
      <15%  — цвет яйца дрозда (голубой)
      15–20 — зелёный
      20–25 — жёлтый
      >25   — ярко‑красный
    """
    try:
        val = float(p)
    except Exception:
        return ""

    if val < 15.0:
        return "is-thrush"   # самое выгодное
    if val < 20.0:
        return "is-good"
    if val <= 25.0:
        return "is-warning"
    return "is-bad"

def _allow_margin_access(request: Request) -> bool:
    """
    Доступ к разделу маржинальности:
      - если нет request.state.user/role — никого не режем (чтобы не ломать проект);
      - если есть роли — пускаем только admin и traffic.
    """
    user = getattr(request.state, "user", None)
    if not user:
        return True  # нет системы пользователей — пускаем всех

    role = getattr(user, "role", None) or getattr(user, "role_name", None)
    if role is None:
        return True

    return str(role).lower() in ("admin", "traffic")

def _parse_shortage_spreadsheet(xml_bytes: bytes, metric_type: str):
    """
    Парсим XML‑Excel с /statistics/shortage/campaigns/<cpc|cpm>/

    Возвращает:
      { campaign_id: {"name": "...", "avg_price": float}, ... }

    Нас интересуют:
      - колонка ID
      - Название кампании
      - Средний CPM, руб. / Средний CPC, руб.
    """
    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return {}

    ws = root.find(".//ss:Worksheet", ns)
    if ws is None:
        return {}
    table = ws.find("ss:Table", ns)
    if table is None:
        return {}

    rows = table.findall("ss:Row", ns)
    if not rows:
        return {}

    # шапка
    header = []
    for cell in rows[0].findall("ss:Cell", ns):
        d = cell.find("ss:Data", ns)
        header.append(d.text if d is not None else None)

    def idx_of(title):
        try:
            return header.index(title)
        except ValueError:
            return -1

    id_idx = idx_of("ID")
    name_idx = idx_of("Название кампании")

    target = "cpm" if metric_type.lower() == "cpm" else "cpc"
    avg_idx = -1
    for i, t in enumerate(header):
        if not t:
            continue
        tl = t.lower()
        if "средний" in tl and target in tl:
            avg_idx = i
            break

    if id_idx < 0 or name_idx < 0 or avg_idx < 0:
        return {}

    out = {}

    for r in rows[1:]:
        vals = []

        # учитываем ss:Index (пропуски колонок)
        last = 0
        for cell in r.findall("ss:Cell", ns):
            idx_attr = cell.get("{%s}Index" % ns["ss"])
            if idx_attr:
                idx = int(idx_attr)
                while len(vals) < idx - 1:
                    vals.append(None)
            d = cell.find("ss:Data", ns)
            vals.append(d.text if d is not None else None)

        need = max(id_idx, name_idx, avg_idx) + 1
        if len(vals) < need:
            vals += [None] * (need - len(vals))

        cid_raw = vals[id_idx]
        name = vals[name_idx] or ""
        if not cid_raw:
            continue

        try:
            cid = int(str(cid_raw).strip())
        except Exception:
            continue

        name_clean = name.strip()
        # убираем кампании, которые начинаются с Foxible
        if name_clean.lower().startswith("foxible"):
            continue

        price_raw = vals[avg_idx]
        if price_raw is None:
            continue

        s = str(price_raw)
        s = s.replace("\xa0", "").replace(" ", "")
        s = s.replace("руб.", "").replace("руб", "").replace("₽", "")
        s = s.replace(",", ".").strip()
        price = _to_float(s)
        if price is None:
            continue

        out[cid] = {
            "name": name_clean,
            "avg_price": price,
        }

    return out

def _fetch_shortage_map(metric_type: str, date_str_ddmmyyyy: str):
    """
    Выкачиваем выгрузку shortage из Cats для текущего месяца и парсим её.
    metric_type: 'cpm' или 'cpc'
    date_str_ddmmyyyy: '01.12.2025' (limit_date_begin)
    """
    syscfg = get_effective_system_config("config.yaml") or {}
    base = (syscfg.get("connect_url") or syscfg.get("base_url") or "").rstrip("/")
    if not base:
        raise RuntimeError("No connect_url/base_url in system config")

    s = _ensure_session()

    if metric_type.lower() == "cpm":
        export_url = (
            f"{base}/statistics/shortage/campaigns/cpm/"
            f"?days_finish_begin=&days_finish_end="
            f"&limit_date_begin={date_str_ddmmyyyy}&limit_date_end="
            f"&name=&campaign_id=&mediaplan_id=&agency_id=&advertiser_id=&manager_id="
            f"&organization_id=4"
            f"&status%5B0%5D=active&status%5B1%5D=blocked&status%5B2%5D=finished"
            f"&payout_model%5B0%5D=cpm"
            f"&export=xls"
        )
    else:
        export_url = (
            f"{base}/statistics/shortage/campaigns/cpc/"
            f"?days_finish_begin=&days_finish_end="
            f"&limit_date_begin={date_str_ddmmyyyy}&limit_date_end="
            f"&name=&campaign_id=&mediaplan_id=&agency_id=&advertiser_id=&manager_id="
            f"&organization_id=4"
            f"&status%5B0%5D=active&status%5B1%5D=blocked&status%5B2%5D=finished"
            f"&payout_model%5B0%5D=cpc&payout_model%5B1%5D=cpc_ic"
            f"&export=xls"
        )

    r = s.get(export_url, timeout=120, allow_redirects=False)
    loc = r.headers.get("Location", "")

    if r.status_code in (301, 302, 303, 307, 308) and "home/login" in (loc or ""):
        raise RuntimeError("Cats session expired: redirected to login")
    if r.status_code != 200:
        raise RuntimeError(f"Cats shortage export HTTP {r.status_code}")

    head = r.content[:800].decode("utf-8", errors="ignore").lower()
    if "<html" in head and "login" in head:
        raise RuntimeError("Cats returned login page for shortage export")

    return _parse_shortage_spreadsheet(r.content, metric_type)



    # ----- Cats -----
    p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
    df = pd.read_csv(p) if p.exists() else None
    date_col = df["date"] if (df is not None and "date" in df.columns) else (df["День"] if (df is not None and "День" in df.columns) else None)
    our_imp  = df["impressions"] if (df is not None and "impressions" in df.columns) else (df.get("Показы") if df is not None else None)
    our_clk  = df["clicks"]      if (df is not None and "clicks"      in df.columns) else (df.get("Переходы") if df is not None else None)
    our_uniq = df["uniques"]     if (df is not None and "uniques"     in df.columns) else (df.get("Охват") if df is not None else None)
    vtrp     = df["vtr_percent"] if (df is not None and "vtr_percent" in df.columns) else (df.get("VTR") if df is not None else None)

    # ----- Verifier из БД + CSV -----
    def _find_series(df, names):
        for nm in names:
            if df is not None and nm in df.columns: return df[nm]
        return None

    def _csv_maps():
        maps = {k:{} for k in ("imp","clk","ctrp","vtrp","viewp","unsafe","givt","sivt","meas")}
        if df is None or date_col is None: return maps
        cols = {
            "imp":  _find_series(df, ["verif_impressions","verifier_impressions","moat_impressions","ias_impressions","dv_impressions"]),
            "clk":  _find_series(df, ["verif_clicks","verifier_clicks","moat_clicks","ias_clicks","dv_clicks"]),
            "ctrp": _find_series(df, ["verif_ctr_percent","verifier_ctr_percent","moat_ctr","ias_ctr","dv_ctr"]),
            "vtrp": _find_series(df, ["verif_vtr_percent","verifier_vtr_percent","moat_vtr","ias_vtr","dv_vtr"]),
            "viewp":_find_series(df, ["verif_viewability_percent","verifier_viewability_percent","viewability_percent","moat_viewability","ias_viewability","dv_viewability"]),
            "unsafe":_find_series(df, ["unsafe_percent","verifier_unsafe_percent"]),
            "givt": _find_series(df, ["givt_percent","verifier_givt_percent"]),
            "sivt": _find_series(df, ["sivt_percent","verifier_sivt_percent"]),
            "meas": _find_series(df, ["measured_impressions","verifier_measured_impressions","moat_measured_impressions","ias_measured_impressions","dv_measured_impressions"]),
        }
        for i in range(len(df.index)):
            raw = date_col.iloc[i]
            s = str(raw).strip().lower()
            if s in ("","nan","nat","total","итого"): continue
            d = _date_iso(raw)
            for k, series in cols.items():
                if series is not None:
                    maps[k][d] = _num(series.iloc[i])
        return maps

    def _db_maps():
        maps = {k:{} for k in ("imp","clk","ctrp","vtrp","viewp","unsafe","givt","sivt","meas")}
        try:
            dbp = os.path.join(os.getcwd(), "campaign_hub.db")
            con = sqlite3.connect(dbp); cur = con.cursor()

            def table_exists(name):
                return bool(cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())
            def cols(name):
                return [r[1] for r in cur.execute(f"PRAGMA table_info({name})").fetchall()]
            def pick(hay, cand):
                for c in cand:
                    if c in hay: return c
                return None

            vid = cid
            if table_exists("verifier_campaigns"):
                cs = cols("verifier_campaigns")
                myc = pick(cs, ["campaign_id","cid","campaign"])
                vrf = pick(cs, ["verifier_campaign_id","verifier_id","verif_campaign_id","vid"])
                if myc and vrf:
                    r = cur.execute(f"SELECT {vrf} FROM verifier_campaigns WHERE {myc}=?", (cid,)).fetchone()
                    if r and r[0] is not None:
                        vid = r[0]

            tname = "verifier_daily_metric"
            if not table_exists(tname):
                con.close(); return maps
            cs = cols(tname)
            idc   = pick(cs, ["verifier_campaign_id","campaign_id","cid","campaign"])
            dc    = pick(cs, ["date","report_date","day"])
            ic    = pick(cs, ["impressions","impr","imp"])
            kc    = pick(cs, ["clicks","clk"])
            ctrc  = pick(cs, ["ctr_percent","ctr"])
            vtrc  = pick(cs, ["vtr_percent","vtr"])
            viewc = pick(cs, ["viewability_percent","viewability","visible_percent"])
            unsfc = pick(cs, ["unsafe_percent","unsafe"])
            givtc = pick(cs, ["givt_percent","givt"])
            sivtc = pick(cs, ["sivt_percent","sivt"])
            measc = pick(cs, ["measured_impressions","measured","meas_impressions","viewable_measured","view_measured"])
            if not idc or not dc:
                con.close(); return maps
            q = f"SELECT {dc},{ic or 'NULL'},{kc or 'NULL'},{ctrc or 'NULL'},{vtrc or 'NULL'},{viewc or 'NULL'},{unsfc or 'NULL'},{givtc or 'NULL'},{sivtc or 'NULL'},{measc or 'NULL'} FROM {tname} WHERE {idc}=?"
            for row in cur.execute(q, (vid,)).fetchall():
                d = _date_iso(row[0]); 
                if not d: continue
                maps["imp"][d]   = _num(row[1])
                maps["clk"][d]   = _num(row[2])
                maps["ctrp"][d]  = _num(row[3])
                maps["vtrp"][d]  = _num(row[4])
                maps["viewp"][d] = _num(row[5])
                maps["unsafe"][d]= _num(row[6])
                maps["givt"][d]  = _num(row[7])
                maps["sivt"][d]  = _num(row[8])
                maps["meas"][d]  = _num(row[9])
            con.close()
        except Exception:
            pass
        return maps

    maps = {k:{} for k in ("imp","clk","ctrp","vtrp","viewp","unsafe","givt","sivt","meas")}
    maps_db = _db_maps()
    maps_csv= _csv_maps()
    for k in maps.keys():
        maps[k] = maps_db[k] if maps_db[k] else maps_csv[k]

    # ----- Яндекс по всем дням -----
    ymap = {}
    try:
        ydbp = os.path.join(os.getcwd(), "yandex_metrics.db")
        con_y = sqlite3.connect(ydbp); cur_y = con_y.cursor()
        for rd, vis, br, pdpth, ats in cur_y.execute(
            "SELECT report_date, visits, bounce_rate, page_depth, avg_time_sec FROM yandex_daily_metrics WHERE campaign_id=?", (cid,)
        ):
            br_pct = br * 100.0 if br is not None else None
            ymap[str(rd)] = (vis, br_pct, pdpth, ats)
        con_y.close()
    except Exception:
        pass

    overrides = _overrides_map(cid)

    # ----- тоталы базы -----
    sum_imp = sum_clk = sum_vis = 0.0
    w_vtr_num = w_vtr_den = 0.0
    br_w_num = br_w_den = 0.0

    if df is not None:
        for i in range(len(df.index)):
            raw = date_col.iloc[i] if date_col is not None else None
            if raw is None or str(raw).strip().lower() in ("nan","nat","total","итого"): continue
            dkey = _date_iso(raw)
            im = our_imp.iloc[i] if our_imp is not None else None
            ck = our_clk.iloc[i] if our_clk is not None else None
            vp = vtrp.iloc[i]     if vtrp     is not None else None
            vis = br = None
            if dkey in ymap: vis, br, _, _ = ymap[dkey]
            ov = overrides.get(dkey, {})
            if "impressions" in ov: im = ov["impressions"]
            if "clicks"      in ov: ck = ov["clicks"]
            if "visits"      in ov: vis = ov["visits"]
            if "bounce_rate" in ov: br  = ov["bounce_rate"]
            im_v = _num(im); ck_v = _num(ck); vp_v = _num(vp); vis_v = _num(vis); br_v = _num(br)
            if im_v is not None: sum_imp += im_v
            if ck_v is not None: sum_clk += ck_v
            if vp_v is not None and im_v is not None: w_vtr_num += (vp_v*im_v); w_vtr_den += im_v
            if vis_v is not None: sum_vis += vis_v
            if br_v  is not None and vis_v is not None: br_w_num += (br_v*vis_v); br_w_den += vis_v

    totals = _campaign_totals(cid) or {}
    reach_total = totals.get("uniques", None)
    ctr_total  = (100.0*sum_clk/sum_imp) if (sum_imp and sum_imp>0) else None
    vtr_total  = (w_vtr_num/w_vtr_den)   if (w_vtr_den and w_vtr_den>0) else None
    freq_total = (sum_imp/reach_total)   if (reach_total and reach_total>0) else None
    br_total   = (br_w_num/br_w_den)     if (br_w_den and br_w_den>0) else None

    # ----- вычисляемые и Δ для дня d_iso -----
    im_v = ck_v = un_v = vis_v = br_v = vp_v = None
    if df is not None:
        for i in range(len(df.index)):
            raw = date_col.iloc[i] if date_col is not None else None
            if raw is None or str(raw).strip().lower() in ("nan","nat","total","итого"): continue
            if _date_iso(raw) != d_iso: continue
            im = our_imp.iloc[i] if our_imp is not None else None
            ck = our_clk.iloc[i] if our_clk is not None else None
            un = our_uniq.iloc[i] if our_uniq is not None else None
            vp = vtrp.iloc[i] if vtrp is not None else None
            vis = br = None
            if d_iso in ymap: vis, br, _, _ = ymap[d_iso]
            ov = overrides.get(d_iso, {})
            if "impressions" in ov: im = ov["impressions"]
            if "clicks"      in ov: ck = ov["clicks"]
            if "uniques"     in ov: un = ov["uniques"]
            if "visits"      in ov: vis = ov["visits"]
            if "bounce_rate" in ov: br  = ov["bounce_rate"]
            im_v, ck_v, un_v = _num(im), _num(ck), _num(un)
            vis_v, br_v      = _num(vis), _num(br)
            vp_v             = _num(vp)
            break

    ctr_v   = (100.0*ck_v/im_v) if (im_v and im_v>0 and ck_v is not None) else None
    freq_v  = (im_v/un_v)       if (un_v and un_v>0 and im_v is not None) else None
    reach_v = (100.0*vis_v/ck_v) if (ck_v and ck_v>0 and vis_v is not None) else None

    # ----- применим оверрайды verif_* к картам -----
    for dkey, ov in overrides.items():
        if "verif_impressions"          in ov: maps["imp"][dkey]   = _num(ov["verif_impressions"])
        if "verif_clicks"               in ov: maps["clk"][dkey]   = _num(ov["verif_clicks"])
        if "verif_ctr_percent"          in ov: maps["ctrp"][dkey]  = _num(ov["verif_ctr_percent"])
        if "verif_viewability_percent"  in ov: maps["viewp"][dkey] = _num(ov["verif_viewability_percent"])
        if "verif_vtr_percent"          in ov: maps["vtrp"][dkey]  = _num(ov["verif_vtr_percent"])
        if "verif_measured_impressions" in ov: maps["meas"][dkey]  = _num(ov["verif_measured_impressions"])
        if "verif_unsafe_percent"       in ov: maps["unsafe"][dkey]= _num(ov["verif_unsafe_percent"])
        if "verif_givt_percent"         in ov: maps["givt"][dkey]  = _num(ov["verif_givt_percent"])
        if "verif_sivt_percent"         in ov: maps["sivt"][dkey]  = _num(ov["verif_sivt_percent"])

    vi_v   = maps["imp"].get(d_iso)
    vc_v   = maps["clk"].get(d_iso)
    vctr_v = maps["ctrp"].get(d_iso)
    vvtr_v = maps["vtrp"].get(d_iso)
    vview_v= maps["viewp"].get(d_iso)

    d_imp = (vi_v / im_v - 1.0) * 100.0 if (vi_v is not None and im_v and im_v>0) else None
    d_clk = (vc_v / ck_v - 1.0) * 100.0 if (vc_v is not None and ck_v and ck_v>0) else None
    d_ctr_pp  = (vctr_v - ctr_v) if (vctr_v is not None and ctr_v is not None) else None
    d_view_pp = (vview_v - 0.0) if (vview_v is not None) else None
    d_vtr_pp  = (vvtr_v - vp_v) if (vvtr_v is not None and vp_v is not None) else None

    # ----- вернуть кнопку-ячейку + OOB -----
    def _fmt_by_metric(m: str, vtxt: str):
        v = _num(vtxt)
        if m in ("impressions","clicks","uniques","visits","verif_impressions","verif_clicks","verif_measured_impressions"): return _fmt_int(v)
        if m in ("page_depth",):   return _fmt_f2(v)
        if m in ("avg_time_sec",): return _fmt_time(v)
        if m in ("bounce_rate","verif_ctr_percent","verif_viewability_percent","verif_vtr_percent","verif_unsafe_percent","verif_givt_percent","verif_sivt_percent"):  return _fmt_pct2(v)
        return vtxt or "—"

    url = f"/campaigns/{cid}/daily5/cell?date={_esc(d_iso)}&metric={_esc(metric)}"
    cell = (
        f"<button type='button' class='dcell{' is-overridden' if edited else ''}' data-url='{url}' title='Кликните для редактирования' "
        f"hx-on=\"click: htmx.ajax('GET', this.dataset.url, {{target:this, swap:'outerHTML'}})\">"
        f"{_fmt_by_metric(metric, val_clean)}</button>"
    )

    # тотальные verifier
    vrf_imp_sum = sum(v for v in maps["imp"].values() if v is not None) or 0.0
    vrf_clk_sum = sum(v for v in maps["clk"].values() if v is not None) or 0.0

    def _weighted(values_map, weight_map_primary, fallback=None):
        num = den = 0.0
        for d, val in values_map.items():
            if val is None: continue
            w = weight_map_primary.get(d)
            if w is None and fallback is not None:
                w = fallback.get(d)
            if w is None: continue
            try: num += float(val)*float(w); den += float(w)
            except Exception: pass
        return (num/den) if den>0 else None

    vrf_ctr_total  = _weighted(maps["ctrp"],  maps["imp"], {})
    vrf_vtr_total  = _weighted(maps["vtrp"],  maps["imp"], {})
    vrf_view_total = _weighted(maps["viewp"], maps["meas"] if any(maps["meas"].values()) else maps["imp"], {})

    d_imp_tot    = ((vrf_imp_sum/sum_imp - 1.0)*100.0) if (vrf_imp_sum and sum_imp>0) else None
    d_clk_tot    = ((vrf_clk_sum/sum_clk - 1.0)*100.0) if (vrf_clk_sum and sum_clk>0) else None
    d_ctr_pp_tot = (vrf_ctr_total - ctr_total) if (vrf_ctr_total is not None and ctr_total is not None) else None
    d_view_pp_tot= (vrf_view_total - 0.0) if (vrf_view_total is not None) else None
    d_vtr_pp_tot = (vrf_vtr_total - vtr_total) if (vrf_vtr_total is not None and vtr_total is not None) else None

    oob = "".join([
        # суточные вычисляемые
        f"<span id='ctr-{cid}-{d_iso}'  hx-swap-oob='true'>{_fmt_pct2(ctr_v)}</span>",
        f"<span id='freq-{cid}-{d_iso}' hx-swap-oob='true'>{_fmt_f2(freq_v)}</span>",
        f"<span id='rch-{cid}-{d_iso}'  hx-swap-oob='true'>{_fmt_pct2(reach_v)}</span>",
        # Δ по дню (включая Δ Viewability)
        f"<span id='dlt-imp-{cid}-{d_iso}'  hx-swap-oob='true'>{_fmt_pct2(d_imp)}</span>",
        f"<span id='dlt-clk-{cid}-{d_iso}'  hx-swap-oob='true'>{_fmt_pct2(d_clk)}</span>",
        f"<span id='dlt-ctr-{cid}-{d_iso}'  hx-swap-oob='true'>{_fmt_pp2(d_ctr_pp)}</span>",
        f"<span id='dlt-view-{cid}-{d_iso}' hx-swap-oob='true'>{_fmt_pp2(d_view_pp)}</span>",
        f"<span id='dlt-vtr-{cid}-{d_iso}'  hx-swap-oob='true'>{_fmt_pp2(d_vtr_pp)}</span>",
        # тоталы базы
        f"<span id='tot-impr-{cid}' hx-swap-oob='true'>{_fmt_int(sum_imp)}</span>",
        f"<span id='tot-clk-{cid}'  hx-swap-oob='true'>{_fmt_int(sum_clk)}</span>",
        f"<span id='tot-ctr-{cid}'  hx-swap-oob='true'>{_fmt_pct2(ctr_total)}</span>",
        f"<span id='tot-vtr-{cid}'  hx-swap-oob='true'>{_fmt_pct2(vtr_total)}</span>",
        f"<span id='tot-uniq-{cid}' hx-swap-oob='true'>{_fmt_int(reach_total)}</span>",
        f"<span id='tot-freq-{cid}' hx-swap-oob='true'>{_fmt_f2(freq_total)}</span>",
        f"<span id='tot-br-{cid}'   hx-swap-oob='true'>{_fmt_pct2(br_total)}</span>",
        # верхняя строка кампании
        f"<span id='imp-{cid}'  hx-swap-oob='true'>{_fmt_int(sum_imp)}</span>",
        f"<span id='clk-{cid}'  hx-swap-oob='true'>{_fmt_int(sum_clk)}</span>",
        f"<span id='uniq-{cid}' hx-swap-oob='true'>{_fmt_int(reach_total)}</span>",
        f"<span id='ctr-{cid}'  hx-swap-oob='true'>{_fmt_pct2(ctr_total)}</span>",
        f"<span id='freq-{cid}' hx-swap-oob='true'>{_fmt_f2(freq_total)}</span>",
        # тотальные Δ + тотал‑поля верификатора
        f"<span id='tot-delta-imp-{cid}'  hx-swap-oob='true'>{_fmt_pct2(d_imp_tot)}</span>",
        f"<span id='tot-delta-clk-{cid}'  hx-swap-oob='true'>{_fmt_pct2(d_clk_tot)}</span>",
        f"<span id='tot-delta-ctr-{cid}'  hx-swap-oob='true'>{_fmt_pp2(d_ctr_pp_tot)}</span>",
        f"<span id='tot-delta-view-{cid}' hx-swap-oob='true'>{_fmt_pp2(d_view_pp_tot)}</span>",
        f"<span id='tot-delta-vtr-{cid}'  hx-swap-oob='true'>{_fmt_pp2(d_vtr_pp_tot)}</span>",
        f"<span id='tot-vrf-imp-{cid}'    hx-swap-oob='true'>{_fmt_int(vrf_imp_sum)}</span>",
        f"<span id='tot-vrf-clk-{cid}'    hx-swap-oob='true'>{_fmt_int(vrf_clk_sum)}</span>",
        f"<span id='tot-vrf-ctr-{cid}'    hx-swap-oob='true'>{_fmt_pct2(vrf_ctr_total)}</span>",
        f"<span id='tot-vrf-view-{cid}'   hx-swap-oob='true'>{_fmt_pct2(vrf_view_total)}</span>",
        f"<span id='tot-vrf-vtr-{cid}'    hx-swap-oob='true'>{_fmt_pct2(vrf_vtr_total)}</span>",
    ])

    return HTMLResponse(cell + oob)


# --- overrides storage (ручные правки ячеек) ---

def _ensure_overrides_table():
    """Создаёт таблицу daily_overrides, если её ещё нет."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_overrides(
                campaign_id INTEGER NOT NULL,
                date        TEXT    NOT NULL,  -- YYYY-MM-DD
                metric      TEXT    NOT NULL,  -- impressions/clicks/...
                value       TEXT,
                PRIMARY KEY (campaign_id, date, metric)
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_daily_overrides_c ON daily_overrides(campaign_id);"))

def _overrides_map(cid: int) -> dict:
    """Возвращает { 'YYYY-MM-DD': {metric: value, ...}, ... } для кампании."""
    _ensure_overrides_table()
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT date, metric, value FROM daily_overrides WHERE campaign_id=:cid"),
            {"cid": int(cid)}
        ).fetchall()
    out = {}
    for d, m, v in rows:
        d = str(d)
        out.setdefault(d, {})[str(m)] = v
    return out

def _save_override(cid: int, d_iso: str, metric: str, value):
    """
    Сохраняет/удаляет ручную правку.
    Если value пусто/None — запись удаляется.
    """
    _ensure_overrides_table()
    with engine.begin() as conn:
        if value is None or str(value).strip() == "":
            conn.execute(text("""
                DELETE FROM daily_overrides
                 WHERE campaign_id=:cid AND date=:d AND metric=:m
            """), {"cid": int(cid), "d": d_iso, "m": metric})
        else:
            conn.execute(text("""
                INSERT INTO daily_overrides(campaign_id, date, metric, value)
                VALUES (:cid, :d, :m, :v)
                ON CONFLICT(campaign_id, date, metric) DO UPDATE SET value=excluded.value
            """), {"cid": int(cid), "d": d_iso, "m": metric, "v": str(value)})



@router.get("/campaigns/{cid}/daily5/cell", response_class=HTMLResponse)
def campaigns_daily5_cell(cid: int, date: str, metric: str):
    """
    Инпут для редактирования ячейки (с предзаполнением).
    Поддерживает базовые метрики и все verif_*.
    Глушит нативный submit, чтобы не было перезагрузки страницы.
    """
    import pandas as pd
    from pathlib import Path
    import os, sqlite3, math

    def _date_iso(x):
        s = str(x).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-": return s[:10]
        if len(s) >= 10 and s[2] == "." and s[5] == ".": d,m,y = s[:10].split("."); return f"{y}-{m}-{d}"
        return s

    def _num(x):
        if x is None: return None
        s = str(x).replace("\xa0","").replace(" ","").replace(",", ".").replace("%","").strip()
        try: return float(s)
        except Exception: return None

    d_iso = _date_iso(date)
    ov = _overrides_map(cid).get(d_iso, {})

    # 1) если есть override — используем его как предзаполнение
    if metric in ov:
        current_txt = str(ov[metric] or "")
    else:
        current_txt = ""

        # 2) Cats CSV
        p = Path("data") / "cats" / str(cid) / "latest_normalized.csv"
        df = pd.read_csv(p) if p.exists() else None

        def _col(*names):
            if df is None: return None
            for n in names:
                if n in df.columns: return df[n]
            return None

        date_col = _col("date", "День")
        imp  = _col("impressions","Показы")
        clk  = _col("clicks","Переходы")
        uni  = _col("uniques","Охват")
        vtrp = _col("vtr_percent","VTR")

        # 3) Яндекс (за конкретный день)
        y_vis = y_br = y_pd = y_ats = None
        try:
            ydbp = os.path.join(os.getcwd(), "yandex_metrics.db")
            con = sqlite3.connect(ydbp); cur = con.cursor()
            r = cur.execute("""SELECT visits, bounce_rate, page_depth, avg_time_sec
                                 FROM yandex_daily_metrics
                                WHERE campaign_id=? AND report_date=?""", (cid, d_iso)).fetchone()
            con.close()
            if r:
                y_vis, y_br, y_pd, y_ats = r
                # в БД bounce_rate как доля, переводим в проценты
                if y_br is not None:
                    y_br = float(y_br) * 100.0
        except Exception:
            pass

        # 4) Verifier из БД (приоритет) по дню, фоллбек — None
        v_imp = v_clk = v_ctrp = v_vtrp = v_viewp = v_unsafe = v_givt = v_sivt = None
        try:
            dbp = os.path.join(os.getcwd(), "campaign_hub.db")
            con = sqlite3.connect(dbp); cur = con.cursor()

            # маппинг campaign_id -> verifier_campaign_id (если есть)
            def _cols(t):
                return [c[1] for c in cur.execute(f"PRAGMA table_info({t})").fetchall()]
            def _pick(hay, cand):
                for c in cand:
                    if c in hay: return c
                return None

            vid = cid
            try:
                ctab = _cols("verifier_campaigns")
                myc = _pick(ctab, ["campaign_id","cid","campaign"])
                vrf = _pick(ctab, ["verifier_campaign_id","verifier_id","verif_campaign_id","vid"])
                if myc and vrf:
                    r = cur.execute(f"SELECT {vrf} FROM verifier_campaigns WHERE {myc}=?", (cid,)).fetchone()
                    if r and r[0] is not None:
                        vid = r[0]
            except Exception:
                pass

            cols = _cols("verifier_daily_metric")
            idc   = _pick(cols, ["verifier_campaign_id","campaign_id","cid","campaign"])
            dc    = _pick(cols, ["date","report_date","day"])
            ic    = _pick(cols, ["impressions","impr","imp"])
            kc    = _pick(cols, ["clicks","clk"])
            ctrc  = _pick(cols, ["ctr_percent","ctr"])
            vtrc  = _pick(cols, ["vtr_percent","vtr"])
            viewc = _pick(cols, ["viewability_percent","viewability","visible_percent"])
            unsfc = _pick(cols, ["unsafe_percent","unsafe"])
            givtc = _pick(cols, ["givt_percent","givt"])
            sivtc = _pick(cols, ["sivt_percent","sivt"])

            if idc and dc:
                q = f"""SELECT {ic or 'NULL'},{kc or 'NULL'},{ctrc or 'NULL'},{vtrc or 'NULL'},
                               {viewc or 'NULL'},{unsfc or 'NULL'},{givtc or 'NULL'},{sivtc or 'NULL'}
                        FROM verifier_daily_metric
                       WHERE {idc}=? AND {dc}=?"""
                r = cur.execute(q, (vid, d_iso)).fetchone()
                if r:
                    v_imp, v_clk, v_ctrp, v_vtrp, v_viewp, v_unsafe, v_givt, v_sivt = [ _num(x) for x in r ]
            con.close()
        except Exception:
            pass

        # 5) предзаполнение по типу метрики
        def _find_row_value(series):
            if df is None or series is None or date_col is None: return None
            for i in range(len(df.index)):
                if _date_iso(df[date_col.name].iloc[i]) == d_iso:
                    return _num(series.iloc[i])
            return None

        if metric == "impressions":
            current_txt = str(int(_find_row_value(imp) or 0)) if imp is not None else ""
        elif metric == "clicks":
            current_txt = str(int(_find_row_value(clk) or 0)) if clk is not None else ""
        elif metric == "uniques":
            current_txt = str(int(_find_row_value(uni) or 0)) if uni is not None else ""
        elif metric == "visits":
            current_txt = str(int(_num(y_vis) or 0)) if y_vis is not None else ""
        elif metric == "bounce_rate":
            current_txt = f"{_num(y_br) or 0:.2f}" if y_br is not None else ""
        elif metric == "page_depth":
            current_txt = f"{_num(y_pd) or 0:.2f}" if y_pd is not None else ""
        elif metric == "avg_time_sec":
            current_txt = str(int(round(_num(y_ats) or 0))) if y_ats is not None else ""

        # verif_*:
        elif metric == "verif_impressions":
            current_txt = str(int(v_imp or 0)) if v_imp is not None else ""
        elif metric == "verif_clicks":
            current_txt = str(int(v_clk or 0)) if v_clk is not None else ""
        elif metric == "verif_ctr_percent":
            current_txt = f"{v_ctrp or 0:.2f}" if v_ctrp is not None else ""
        elif metric == "verif_viewability_percent":
            current_txt = f"{v_viewp or 0:.2f}" if v_viewp is not None else ""
        elif metric == "verif_vtr_percent":
            current_txt = f"{v_vtrp or 0:.2f}" if v_vtrp is not None else ""
        elif metric == "verif_unsafe_percent":
            current_txt = f"{v_unsafe or 0:.2f}" if v_unsafe is not None else ""
        elif metric == "verif_givt_percent":
            current_txt = f"{v_givt or 0:.2f}" if v_givt is not None else ""
        elif metric == "verif_sivt_percent":
            current_txt = f"{v_sivt or 0:.2f}" if v_sivt is not None else ""
        else:
            current_txt = ""

@router.get("/campaigns/margin", response_class=HTMLResponse)
def campaigns_margin_view(request: Request):
    """
    Раздел 'Маржинальность' — сводка по брони за текущий месяц + история по дням.

    Источник данных:
      - client_price и модель закупки — из таблицы bookings
      - средний CPM/CPC — из Cats shortage
      - % закупки = (cats_avg_price / client_price) * 100

    История:
      - при каждом update_all сохраняем снимок в margin_snapshots (snapshot_date = дата апдейта)
      - в UI показываем колонки вида "% (дата)"
    """
    import json

    if not _allow_margin_access(request):
        return HTMLResponse("Forbidden", status_code=403)

    _ensure_margin_table()

    today = date.today()
    month_start = today.replace(day=1)
    month_key = month_start.strftime("%Y-%m-01")

    with engine.begin() as conn:
        base_rows = conn.execute(text("""
            SELECT
                ms.booking_id,
                ms.campaign_id,
                ms.metric_type,
                ms.client_price,
                ms.cats_avg_price,
                ms.purchase_percent,
                ms.comment,
                b.name,
                b.client_brand,
                b.agency_name,
                b.payout_model,
                b.inventory,
                b.budget_before_vat
            FROM margin_stats ms
            JOIN bookings b ON b.id = ms.booking_id
            WHERE ms.month = :month
              AND (b.name IS NULL OR b.name NOT LIKE 'Foxible%')
            ORDER BY b.name
        """), {"month": month_key}).mappings().all()

        # Какие даты снимков есть в БД за месяц (в порядке времени)
        snapshot_dates = [
            r[0] for r in conn.execute(text("""
                SELECT DISTINCT snapshot_date
                FROM margin_snapshots
                WHERE month = :month
                ORDER BY snapshot_date
            """), {"month": month_key}).fetchall()
            if r and r[0]
        ]

        # Снапшоты по всем букингам за месяц (для пивота в таблицу)
        snap_rows = conn.execute(text("""
            SELECT booking_id, snapshot_date, purchase_percent
            FROM margin_snapshots
            WHERE month = :month
        """), {"month": month_key}).fetchall()

        history_map: dict[int, dict[str, dict]] = {}
        for bid, sdate, pp in snap_rows:
            if bid is None or sdate is None:
                continue
            history_map.setdefault(int(bid), {})[str(sdate)] = {
                "value": pp,
                "class": _purchase_percent_class(pp),
            }

        latest_snapshot_date = snapshot_dates[-1] if snapshot_dates else None

        avg_purchase_percent = None
        if latest_snapshot_date:
            v = conn.execute(text("""
                SELECT AVG(purchase_percent)
                FROM margin_snapshots
                WHERE month = :month
                  AND snapshot_date = :d
                  AND purchase_percent IS NOT NULL
            """), {"month": month_key, "d": latest_snapshot_date}).fetchone()
            if v and v[0] is not None:
                try:
                    avg_purchase_percent = round(float(v[0]), 2)
                except Exception:
                    avg_purchase_percent = None

        # Данные для стартового графика (без фильтров)
        chart_rows = conn.execute(text("""
            SELECT snapshot_date, AVG(purchase_percent) AS avg_pp
            FROM margin_snapshots
            WHERE month = :month
              AND purchase_percent IS NOT NULL
            GROUP BY snapshot_date
            ORDER BY snapshot_date
        """), {"month": month_key}).fetchall()

    # Опции фильтра по РК (для графика)
    booking_options = [
        {
            "booking_id": int(r["booking_id"]),
            "name": (r.get("name") or f"Booking {int(r['booking_id'])}").strip(),
            "metric_type": (r.get("metric_type") or "").lower(),
        }
        for r in base_rows
    ]

    # Строим rows для шаблона + цепляем историю
    result_rows = []
    for r in base_rows:
        bid = int(r["booking_id"])
        result_rows.append({
            "booking_id":        bid,
            "campaign_id":       r.get("campaign_id"),
            "metric_type":       r.get("metric_type"),
            "client_price":      r.get("client_price"),
            "cats_avg_price":    r.get("cats_avg_price"),
            "purchase_percent":  r.get("purchase_percent"),  # последнее (для совместимости)
            "percent_class":     _purchase_percent_class(r.get("purchase_percent")),
            "comment":           r.get("comment") or "",
            "name":              r.get("name"),
            "client_brand":      r.get("client_brand"),
            "agency_name":       r.get("agency_name"),
            "payout_model":      r.get("payout_model"),
            "inventory":         r.get("inventory"),
            "budget_before_vat": r.get("budget_before_vat"),
            "history":           history_map.get(bid, {}),
        })

    chart_labels = [r[0] for r in chart_rows if r and r[0]]
    chart_values = []
    for r in chart_rows:
        if not r:
            continue
        v = r[1]
        if v is None:
            chart_values.append(None)
        else:
            try:
                chart_values.append(round(float(v), 2))
            except Exception:
                chart_values.append(None)

    return templates.TemplateResponse(
        "campaigns_margin.html",
        {
            "request": request,
            "rows": result_rows,
            "month_key": month_key,
            "snapshot_dates": snapshot_dates,
            "latest_snapshot_date": latest_snapshot_date,
            "avg_purchase_percent": avg_purchase_percent,
            "booking_options": booking_options,
            "chart_labels_json": json.dumps(chart_labels, ensure_ascii=False),
            "chart_values_json": json.dumps(chart_values, ensure_ascii=False),
        },
    )

def _margin_update_all_core(snapshot_date: date | None = None) -> dict:
    """
    Внутренний апдейт маржинальности (без Request).

    Делает две записи:
      - margin_stats      (upsert): последнее значение за месяц
      - margin_snapshots  (upsert): дневной снапшот (накопительная история)

    snapshot_date:
      - если None, берём date.today()
      - используется и для подписи колонок, и для защиты от повторного авто-апдейта в течение дня
    """
    from fastapi import HTTPException
    from sqlalchemy import text
    from datetime import date as _date

    _ensure_margin_table()

    snap_date = snapshot_date or _date.today()
    month_start = snap_date.replace(day=1)
    limit_begin = month_start.strftime("%d.%m.%Y")   # 01.12.2025
    month_key   = month_start.strftime("%Y-%m-01")   # 2025-12-01
    snapshot_iso = snap_date.isoformat()               # 2025-12-17

    # 1) Конфиг Cats
    syscfg = get_effective_system_config("config.yaml") or {}
    base_raw = (syscfg.get("connect_url") or syscfg.get("base_url") or "").strip().rstrip("/")
    if not base_raw:
        raise HTTPException(status_code=500, detail="No connect_url/base_url in system section")

    # Cats UI живёт под /iface. Не допускаем /iface/iface
    cats_base = base_raw
    if not cats_base.endswith("/iface"):
        cats_base = cats_base + "/iface"

    # 2) Авторизованная сессия Cats
    try:
        s = _ensure_session()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cats auth failed: {e}") from e

    # 3) Качаем и парсим CPM/CPC
    def load_map(metric_type: str, payout_models: list[str]) -> dict[int, float]:
        if metric_type not in ("cpm", "cpc"):
            raise ValueError("metric_type must be 'cpm' or 'cpc'")

        pm_qs = "&".join(
            f"payout_model%5B{i}%5D={pm}"
            for i, pm in enumerate(payout_models)
        )

        url = (
            f"{cats_base}/statistics/shortage/campaigns/{metric_type}/"
            f"?days_finish_begin=&days_finish_end="
            f"&limit_date_begin={limit_begin}&limit_date_end="
            f"&name=&campaign_id=&mediaplan_id=&agency_id=&advertiser_id="
            f"&manager_id=&organization_id=4"
            f"&status%5B0%5D=active&status%5B1%5D=blocked&status%5B2%5D=finished&"
            f"{pm_qs}&export=xls"
        )

        r = s.get(url, timeout=120, allow_redirects=False)
        loc = r.headers.get("Location", "")

        if r.status_code in (301, 302, 303, 307, 308) and "home/login" in (loc or ""):
            raise RuntimeError("Cats session expired: redirected to login")
        if r.status_code != 200:
            raise RuntimeError(f"Cats shortage export HTTP {r.status_code}")

        # иногда приходит HTML логина с кодом 200
        head = r.content[:800].decode("utf-8", errors="ignore").lower()
        if "<html" in head and "login" in head:
            raise RuntimeError("Cats returned login page for shortage export")

        parsed = _parse_shortage_spreadsheet(r.content, metric_type)
        if parsed:
            return {
                int(cid): float(row.get("avg_price"))
                for cid, row in parsed.items()
                if row and row.get("avg_price") is not None
            }

        # fallback (на случай другой шапки)
        return _parse_cats_shortage_xls(r.content, metric_type)

    try:
        cpm_map = load_map("cpm", ["cpm"])
        cpc_map = load_map("cpc", ["cpc", "cpc_ic"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка выгрузки/парсинга Cats shortage XLS: {e}") from e

    # {campaign_id: {"cpm": x, "cpc": y}}
    stats: dict[int, dict[str, float]] = {}
    for cid, v in (cpm_map or {}).items():
        stats.setdefault(int(cid), {})["cpm"] = float(v)
    for cid, v in (cpc_map or {}).items():
        stats.setdefault(int(cid), {})["cpc"] = float(v)

    updated = 0
    skipped_no_stats = 0
    skipped_unknown_model = 0

    with engine.begin() as conn:
        bookings = conn.execute(text("""
            SELECT
                id                       AS booking_id,
                campaign_id              AS cats_id,
                COALESCE(payout_model, buying_model) AS payout_model,
                client_price,
                name
            FROM bookings
            WHERE campaign_id IS NOT NULL
        """)).mappings().all()

        for b in bookings:
            name = (b.get("name") or "").strip()
            if name.lower().startswith("foxible"):
                continue  # по ТЗ Foxible не считаем

            cats_id = b.get("cats_id")
            if cats_id is None:
                continue

            pm = (b.get("payout_model") or "").lower()

            if "cpm" in pm:
                metric_type = "cpm"
            elif "cpc" in pm:
                metric_type = "cpc"
            else:
                skipped_unknown_model += 1
                continue

            cats_price = stats.get(int(cats_id), {}).get(metric_type)
            if cats_price is None:
                skipped_no_stats += 1
                continue

            client_price = b.get("client_price")
            purchase_percent = None
            if client_price is not None:
                try:
                    cp = float(client_price)
                    if cp > 0:
                        purchase_percent = (float(cats_price) / cp) * 100.0
                except Exception:
                    purchase_percent = None

            # 1) актуальное значение (для текущего UI)
            conn.execute(text("""
                INSERT INTO margin_stats(
                    booking_id, campaign_id, month, metric_type,
                    client_price, cats_avg_price, purchase_percent, updated_at
                )
                VALUES (:booking_id, :campaign_id, :month, :metric_type,
                        :client_price, :cats_avg_price, :purchase_percent,
                        datetime('now'))
                ON CONFLICT(booking_id, month) DO UPDATE SET
                    campaign_id      = excluded.campaign_id,
                    metric_type      = excluded.metric_type,
                    client_price     = excluded.client_price,
                    cats_avg_price   = excluded.cats_avg_price,
                    purchase_percent = excluded.purchase_percent,
                    updated_at       = datetime('now')
            """), {
                "booking_id": int(b["booking_id"]),
                "campaign_id": int(cats_id),
                "month": month_key,
                "metric_type": metric_type,
                "client_price": client_price,
                "cats_avg_price": float(cats_price),
                "purchase_percent": purchase_percent,
            })

            # 2) дневной снимок (накопительная история)
            conn.execute(text("""
                INSERT INTO margin_snapshots(
                    booking_id, campaign_id, month, snapshot_date, metric_type,
                    client_price, cats_avg_price, purchase_percent, updated_at
                )
                VALUES (:booking_id, :campaign_id, :month, :snapshot_date, :metric_type,
                        :client_price, :cats_avg_price, :purchase_percent,
                        datetime('now'))
                ON CONFLICT(booking_id, month, snapshot_date) DO UPDATE SET
                    campaign_id      = excluded.campaign_id,
                    metric_type      = excluded.metric_type,
                    client_price     = excluded.client_price,
                    cats_avg_price   = excluded.cats_avg_price,
                    purchase_percent = excluded.purchase_percent,
                    updated_at       = datetime('now')
            """), {
                "booking_id": int(b["booking_id"]),
                "campaign_id": int(cats_id),
                "month": month_key,
                "snapshot_date": snapshot_iso,
                "metric_type": metric_type,
                "client_price": client_price,
                "cats_avg_price": float(cats_price),
                "purchase_percent": purchase_percent,
            })

            updated += 1

    return {
        "status": "ok",
        "month": month_key,
        "snapshot_date": snapshot_iso,
        "updated": updated,
        "skipped_no_stats": skipped_no_stats,
        "skipped_unknown_model": skipped_unknown_model,
    }


@router.post("/campaigns/margin/update_all")
def campaigns_margin_update_all(request: Request):
    """
    Ручной апдейт (кнопка Update All) — сохраняет:
      - последнее значение в margin_stats
      - дневной снапшот в margin_snapshots (колонка % (дата))

    Важно:
      - Foxible-* отбрасываем
      - purchase_percent считаем в ПРОЦЕНТАХ: (cats_avg_price / client_price) * 100
    """
    from fastapi import HTTPException

    if not _allow_margin_access(request):
        return HTMLResponse("Forbidden", status_code=403)

    try:
        res = _margin_update_all_core()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    # Если вызвали через HTMX (кнопка) — вернём компактный HTML‑статус
    if (request.headers.get("HX-Request") or "").lower() == "true":
        html_status = (
            f"<span class='tag is-light'>OK · {html.escape(res.get('snapshot_date',''))} · "
            f"updated: <strong>{res.get('updated')}</strong> · "
            f"no stats: {res.get('skipped_no_stats')} · "
            f"unknown model: {res.get('skipped_unknown_model')}</span>"
        )
        return HTMLResponse(html_status)

    return JSONResponse(res)


@router.get("/campaigns/margin/chart_data", response_class=JSONResponse)
def campaigns_margin_chart_data(
    request: Request,
    month: str | None = None,
    metric_type: str | None = None,
    booking_id: int | None = None,
):
    """
    Данные для графика (line chart): средний % закупки по дням.
    Фильтры:
      - metric_type: 'cpm' / 'cpc' / None (все)
      - booking_id: конкретная РК (booking_id) / None (все)
    """
    if not _allow_margin_access(request):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)

    _ensure_margin_table()

    month_key = (month or date.today().replace(day=1).strftime("%Y-%m-01")).strip()
    mt = (metric_type or "").strip().lower()
    if mt not in ("", "cpm", "cpc"):
        mt = ""

    q = """
        SELECT snapshot_date, AVG(purchase_percent) AS avg_pp
        FROM margin_snapshots
        WHERE month = :month
          AND purchase_percent IS NOT NULL
    """
    params: dict = {"month": month_key}

    if mt:
        q += " AND metric_type = :mt"
        params["mt"] = mt

    if booking_id is not None:
        q += " AND booking_id = :bid"
        params["bid"] = int(booking_id)

    q += " GROUP BY snapshot_date ORDER BY snapshot_date"

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).fetchall()

    labels = [r[0] for r in rows if r and r[0]]
    values = []
    for r in rows:
        v = r[1] if r else None
        if v is None:
            values.append(None)
        else:
            try:
                values.append(round(float(v), 2))
            except Exception:
                values.append(None)

    return JSONResponse({
        "month": month_key,
        "metric_type": (mt or None),
        "booking_id": booking_id,
        "labels": labels,
        "values": values,
    })


# ---- Авто-апдейт 1 раз в день (по умолчанию включён; можно выключить MARGIN_DAILY_AUTO=0) ----

_MARGIN_DAILY_THREAD = None

def _start_margin_daily_autoupdate():
    """
    Авто-апдейт: раз в день сохраняет дневной снапшот (если его ещё нет).

    По умолчанию ВКЛЮЧЕНО.
    Чтобы выключить (например, локально в dev):
        export MARGIN_DAILY_AUTO=0

    Как работает:
      - раз в час проверяем, есть ли снапшот за сегодня в margin_snapshots
      - если нет — запускаем _margin_update_all_core()
    """
    import threading
    import time

    global _MARGIN_DAILY_THREAD

    flag = (os.getenv("MARGIN_DAILY_AUTO", "1") or "1").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return

    if _MARGIN_DAILY_THREAD is not None and _MARGIN_DAILY_THREAD.is_alive():
        return

    def worker():
        while True:
            try:
                _ensure_margin_table()
                today = date.today()
                month_key = today.replace(day=1).strftime("%Y-%m-01")
                snap = today.isoformat()

                with engine.begin() as conn:
                    exists = conn.execute(
                        text("SELECT 1 FROM margin_snapshots WHERE month=:m AND snapshot_date=:d LIMIT 1"),
                        {"m": month_key, "d": snap},
                    ).fetchone()

                if not exists:
                    _margin_update_all_core(snapshot_date=today)

            except Exception as e:
                # чтобы не падал процесс, просто логируем в stdout
                print(f"[margin] daily autoupdate error: {e}")

            time.sleep(60 * 60)  # 1 раз в час

    _MARGIN_DAILY_THREAD = threading.Thread(
        target=worker,
        name="margin-daily-autoupdate",
        daemon=True,
    )
    _MARGIN_DAILY_THREAD.start()


@router.on_event("startup")
def _margin_autoupdate_startup():
    _start_margin_daily_autoupdate()

@router.post("/campaigns/margin/{booking_id}/comment", response_class=HTMLResponse)
def campaigns_margin_comment(
    booking_id: int,
    request: Request,
    month: str = Form(...),
    comment: str = Form(""),
):
    """
    Обновление поля 'Комментарий' справа от каждой РК.
    Комментарий хранится в margin_stats.comment (по booking_id + month).
    """
    if not _allow_margin_access(request):
        return HTMLResponse("Forbidden", status_code=403)

    _ensure_margin_table()

    month_key = (month or "").strip()
    if not month_key:
        month_key = date.today().replace(day=1).strftime("%Y-%m-%d")

    value = comment.strip() or None

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE margin_stats
               SET comment = :comment
             WHERE booking_id = :bid AND month = :month
        """), {"comment": value, "bid": booking_id, "month": month_key})

    safe_val = html.escape(comment.strip(), quote=True)

    # Возвращаем тот же <form> (для hx-swap="outerHTML")
    html_form = (
        f"<form hx-post='/campaigns/margin/{booking_id}/comment' "
        f"      hx-target='this' hx-swap='outerHTML' "
        f"      hx-trigger='change from:input, blur from:input'>"
        f"  <input type='hidden' name='month' value='{month_key}' />"
        f"  <input class='input is-small' type='text' name='comment' value='{safe_val}' />"
        f"</form>"
    )

    return HTMLResponse(html_form)



    # ВАЖНО: глушим нативный submit (иначе возможна полная перезагрузка страницы)
    html = (
        "<form class='dcell-edit' "
        f"      hx-post='/campaigns/{cid}/daily5/save' "
        "      hx-target='this' hx-swap='outerHTML' "
        "      hx-trigger='keyup[key==\"Enter\"] from:input, blur from:input, submit' "
        "      hx-on='submit: event.preventDefault()' onsubmit='return false'>"
        f"  <input type='hidden' name='date' value='{d_iso}'/>"
        f"  <input type='hidden' name='metric' value='{metric}'/>"
        f"  <input name='value' class='input is-small' autocomplete='off' value='{current_txt}' autofocus>"
        "</form>"
    )
    return HTMLResponse(html)
