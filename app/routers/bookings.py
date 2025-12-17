from fastapi import APIRouter, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette import status
import sqlite3, io, pandas as pd, json
from pathlib import Path
import os, re, time


router = APIRouter(prefix="/bookings", tags=["bookings"])
templates = Jinja2Templates(directory="app/templates")
DB_PATH = "campaign_hub.db"
UPLOAD_DIR = Path("app/static/uploads")
ALLOWED_IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp", "image/gif"}


# ----- utils -----
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
def ensure_campaign_kpi_table(conn):
    conn.execute("""
      CREATE TABLE IF NOT EXISTS campaign_client_kpis (
        campaign_id   INTEGER PRIMARY KEY,
        ctr           REAL,
        vtr           REAL,
        reachability  REAL,
        bounce_rate   REAL,
        depth         REAL,
        time_on_site  REAL,
        viewability   REAL,
        unsafe        REAL,
        givt          REAL,
        sivt          REAL,
        delta_impr    REAL,
        delta_clicks  REAL,
        updated_at    TEXT DEFAULT (datetime('now'))
      )
    """)

def ensure_campaign_screens_table(conn):
    conn.execute("""
      CREATE TABLE IF NOT EXISTS campaign_screenshots (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id  INTEGER NOT NULL,
        file_path    TEXT    NOT NULL,  -- относительный путь в /static
        is_primary   INTEGER NOT NULL DEFAULT 0,
        uploaded_at  TEXT    NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE
      )
    """)

def get_campaign_kpi(conn, campaign_id: int):
    ensure_campaign_kpi_table(conn)
    row = conn.execute("SELECT * FROM campaign_client_kpis WHERE campaign_id=?", (campaign_id,)).fetchone()
    return dict(row) if row else {}

def get_campaign_shots(conn, campaign_id: int):
    ensure_campaign_screens_table(conn)
    cur = conn.execute("""
      SELECT id, campaign_id, file_path, is_primary, uploaded_at
      FROM campaign_screenshots
      WHERE campaign_id=? ORDER BY is_primary DESC, uploaded_at DESC
    """, (campaign_id,))
    return [dict(r) for r in cur.fetchall()]

_filename_re = re.compile(r"[^A-Za-z0-9._-]+")

def _safe_filename(name: str) -> str:
    name = name.strip().replace(" ", "_")
    name = _filename_re.sub("_", name)
    ts = int(time.time()*1000)
    base, dot, ext = name.rpartition(".")
    base = base or "file"
    return f"{base}_{ts}.{ext}" if dot else f"{name}_{ts}"

def _table_exists(conn, name: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())

def _campaign_cols(conn) -> set[str]:
    try:
        return {r[1] for r in conn.execute("PRAGMA table_info(campaigns)").fetchall()}
    except sqlite3.Error:
        return set()

def _select_fragments(cam_cols: set[str]) -> str:
    """
    Базовый SELECT для списка/строки: добавлены brand, format, buying_model,
    а также client_brand (из raw_json.'Бренды' c fallback на b.brand).
    """
    base = [
        "b.id", "b.name",
        "b.campaign_id", "b.start_date", "b.end_date", 

        # Новая колонка — клиенсткая цена
        "b.client_price",

        "b.budget_client_net", "b.inventory_total_plan", "b.inventory_fact",
        "b.vz_percent", "b.sales_manager",
        "b.brand",
        "COALESCE(NULLIF(json_extract(b.raw_json, '$.\"Бренды\"'), ''), b.brand) AS client_brand",
        "b.format", "b.buying_model",
        "a.name AS agency_name",
        # KPI (форму переделаем позже; в списке не используются, но оставим селект)
        "k.kpi_impressions", "k.kpi_clicks", "k.kpi_uniques", "k.kpi_ctr",
        "k.kpi_freq", "k.kpi_conversions", "k.kpi_spend", "k.kpi_cpm", "k.kpi_cpc", "k.kpi_cpa",
    ]
    mapping = [
        ("impressions","act_impressions"),
        ("clicks","act_clicks"),
        ("uniques","act_uniques"),
        ("conversions","act_conversions"),
        ("ctr_ext","act_ctr"),
        ("freq","act_freq"),
        ("spend","act_spend"),
        ("min_date","camp_min_date"),
        ("max_date","camp_max_date"),
    ]
    extra = [(f"cam.{src} AS {alias}") if (src in cam_cols) else (f"NULL AS {alias}") for src, alias in mapping]
    return ", ".join(base + extra)

# formatting filters (используются местами)
def _fmt_num(x, nd=0):
    try:
        if x is None: return "—"
        val = float(x)
        s = f"{val:,.{int(nd)}f}" if nd else f"{val:,.0f}"
        return s.replace(",", " ")
    except Exception:
        return "—"

def _fmt_pct(x, nd=1):
    try:
        if x is None: return "—"
        val = float(x)
        if abs(val) <= 1: val *= 100
        return f"{val:.{int(nd)}f}%".replace(",", " ")
    except Exception:
        return "—"

templates.env.filters['fmt'] = _fmt_num
templates.env.filters['pct'] = _fmt_pct

BETTER_IS_HIGHER = {"impressions","clicks","uniques","conversions","ctr","freq"}
BETTER_IS_LOWER  = {"cpm","cpc","cpa","spend"}
def judge(metric, actual, target):
    if target is None or actual is None: return ""
    try:
        a = float(actual); t = float(target)
    except Exception:
        return ""
    if metric in BETTER_IS_HIGHER: return "is-good" if a >= t else "is-bad"
    if metric in BETTER_IS_LOWER:  return "is-good" if a <= t else "is-bad"
    return ""

# ===== list page =====
@router.get("", response_class=HTMLResponse)
def list_bookings(request: Request, q: str = "", sort: str = "id", dir: str = "desc", period: str = ""):
    conn = get_db()
    cur = conn.cursor()

    has_campaigns = _table_exists(conn, "campaigns")
    cam_cols = _campaign_cols(conn) if has_campaigns else set()
    select_clause = _select_fragments(cam_cols)
    join_campaigns = "LEFT JOIN campaigns cam ON cam.id = b.campaign_id" if has_campaigns else ""

    SAFE_SORTS = {
        "id": "b.id",
        "name": "b.name",
        "start_date": "b.start_date",
        "budget_client_net": "b.budget_client_net",
        "client_brand": "client_brand",
        "agency_name": "a.name",
        "format": "b.format",
        "buying_model": "b.buying_model",
        "vz_percent": "b.vz_percent",
        "sales_manager": "b.sales_manager",
    }
    order_by = SAFE_SORTS.get((sort or "").strip(), "b.id")
    dir_sql = "ASC" if (dir or "").lower() == "asc" else "DESC"

    # период YYYY-MM
    pstart = pend = None
    if period:
        from calendar import monthrange
        try:
            year, month = map(int, period.split("-"))
            last_day = monthrange(year, month)[1]
            pstart = f"{year:04d}-{month:02d}-01"
            pend   = f"{year:04d}-{month:02d}-{last_day:02d}"
        except Exception:
            pstart = pend = None

    sql = f"""
      SELECT {select_clause}
      FROM bookings b
      LEFT JOIN clients  c ON c.id = b.client_id
      LEFT JOIN agencies a ON a.id = b.agency_id
      LEFT JOIN booking_kpis k ON k.booking_id = b.id
      {join_campaigns}
      WHERE (b.name LIKE ? OR IFNULL(b.campaign_id,'') LIKE ? OR IFNULL(a.name,'') LIKE ? OR IFNULL(b.brand,'') LIKE ?)
      {"AND (IFNULL(b.end_date, b.start_date) >= ? AND IFNULL(b.start_date, b.end_date) <= ?)" if pstart and pend else ""}
      ORDER BY {order_by} {dir_sql}
      LIMIT 1000
    """
    like = f"%{q.strip()}%" if q else "%"
    params = [like, like, like, like]
    if pstart and pend:
        params.extend([pstart, pend])

    rows = cur.execute(sql, params).fetchall()

    managers = [r[0] for r in cur.execute(
        "SELECT DISTINCT sales_manager FROM bookings WHERE sales_manager IS NOT NULL AND TRIM(sales_manager)!='' ORDER BY sales_manager"
    ).fetchall()]

    # быстрый dbg (оставь, если удобно)
    if rows:
        sample = dict(rows[0])
        print("DBG:/bookings keys:", list(sample.keys()))
        print("DBG:/bookings sample:", {k: sample.get(k) for k in ("id","client_brand","brand","agency_name","sales_manager")})

    return templates.TemplateResponse("bookings.html", {
        "request": request, "rows": rows,
        "q": q, "sort": sort, "dir": dir, "period": period,
        "judge": judge, "managers": managers
    })

# ===== add row =====
@router.post("", response_class=RedirectResponse)
def add_booking(name: str = Form(...), campaign_id: int = Form(None)):
    conn = get_db()
    conn.execute("INSERT INTO bookings(name, campaign_id) VALUES (?,?)", (name, campaign_id))
    conn.commit()
    return RedirectResponse(url="/bookings", status_code=status.HTTP_303_SEE_OTHER)

# ===== import from Excel =====
@router.post("/import_excel", response_class=HTMLResponse)
async def import_excel(request: Request, file: UploadFile = File(...), sheet: str = Form("Свод")):
    raw = await file.read()
    df = pd.read_excel(io.BytesIO(raw), sheet_name=sheet)
    conn = get_db()
    cur = conn.cursor()
    inserted, updated = 0, 0

    def as_date(x):
        if pd.isna(x): return None
        try:
            if isinstance(x, (pd.Timestamp,)):
                return x.strftime("%Y-%m-%d")
            dt = pd.to_datetime(str(x), errors='coerce', dayfirst=True)
            return None if pd.isna(dt) else dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    def coerce_float(x):
        if pd.isna(x): return None
        try:
            return float(x)
        except Exception:
            try: return float(str(x).replace(" ", "").replace(",", "."))
            except Exception: return None

    def coerce_percent(x):
        v = coerce_float(x)
        if v is None: return None
        return v/100.0 if abs(v) > 1 else v

    def get_cell(row, *alts):
        # сначала точные заголовки, затем case‑insensitive поиск
        for a in alts:
            if a in row and not pd.isna(row[a]):
                return row[a]
        lowmap = {str(c).strip().lower(): c for c in row.index}
        for a in alts:
            key = str(a).strip().lower()
            if key in lowmap and not pd.isna(row[lowmap[key]]):
                return row[lowmap[key]]
        return None

    for _, r in df.iterrows():
        name = str(r.get("Название РК") or "").strip() or f"РК {r.get('ID РК в системе') or ''}".strip()
        campaign_id = None
        try:
            campaign_id = int(r.get("ID РК в системе")) if not pd.isna(r.get("ID РК в системе")) else None
        except Exception:
            pass

        # "Агентство/Клиент"
        agency_name = client_name = None
        ac = get_cell(r, "Агентство/Клиент")
        if ac is not None:
            parts = str(ac).split("/", 1)
            if len(parts) == 2:
                agency_name, client_name = parts[0].strip(), parts[1].strip()
            else:
                client_name = str(ac).strip()

        client_id = agency_id = None
        if client_name:
            cur.execute("INSERT OR IGNORE INTO clients(name) VALUES (?)", (client_name,))
            client_id = cur.execute("SELECT id FROM clients WHERE name=?", (client_name,)).fetchone()[0]
        if agency_name:
            cur.execute("INSERT OR IGNORE INTO agencies(name) VALUES (?)", (agency_name,))
            agency_id = cur.execute("SELECT id FROM agencies WHERE name=?", (agency_name,)).fetchone()[0]

        rec = {
            "name": name,
            "campaign_id": campaign_id,
            "client_id": client_id,
            "agency_id": agency_id,
            "start_date": as_date(get_cell(r, "Дата старта", "Дата начала")),
            "end_date":   as_date(get_cell(r, "Дата завершения", "Дата конца")),
            "budget_client_net": coerce_float(get_cell(r, "Бюджет клиентский до НДС", "Бюджет до НДС")),
            "inventory_total_plan": coerce_float(get_cell(r, "Тотал инвентарь", "Инвентарь план")),
            "inventory_fact": coerce_float(get_cell(r, "Инвентарь факт", "Факт инвентарь")),
            "format": (str(get_cell(r, "Формат", "Format", "формат")).strip() or None) if get_cell(r, "Формат", "Format", "формат") is not None else None,
            "buying_model": (str(get_cell(r, "Модель закупки", "Buying model", "Модель")).strip() or None) if get_cell(r, "Модель закупки", "Buying model", "Модель") is not None else None,
            "vz_percent": coerce_percent(get_cell(r, "ВЗ%", "Б3%", "СК", "CK")),
            "month_str": str(get_cell(r, "Месяц размещения")) if get_cell(r, "Месяц размещения") is not None else None,
            "raw_json": json.dumps(
{c: (None if pd.isna(r[c]) else r[c]) for c in df.columns},
    ensure_ascii=False,
    default=str,   # <‑‑ вот эта строчка важна
),
        }

        try:
            cur.execute("""
                INSERT INTO bookings(
                  name, campaign_id, client_id, agency_id,
                  start_date, end_date,
                  budget_client_net, inventory_total_plan, inventory_fact,
                  format, buying_model, vz_percent,
                  month_str, raw_json
                ) VALUES (
                  :name, :campaign_id, :client_id, :agency_id,
                  :start_date, :end_date,
                  :budget_client_net, :inventory_total_plan, :inventory_fact,
                  :format, :buying_model, :vz_percent,
                  :month_str, :raw_json
                )
            """, rec)
            inserted += 1
        except Exception:
            cur.execute("""
                UPDATE bookings SET
                  campaign_id=:campaign_id,
                  client_id=:client_id,
                  agency_id=:agency_id,
                  start_date=:start_date,
                  end_date=:end_date,
                  budget_client_net=:budget_client_net,
                  inventory_total_plan=:inventory_total_plan,
                  inventory_fact=:inventory_fact,
                  format=:format,
                  buying_model=:buying_model,
                  vz_percent=:vz_percent,
                  month_str=:month_str,
                  raw_json=:raw_json
                WHERE name=:name AND IFNULL(start_date,'')=:start_date AND IFNULL(end_date,'')=:end_date
            """, rec)
            if cur.rowcount:
                updated += 1

    conn.commit()
    summary = f"<div class='notification is-success'>Импорт: добавлено {inserted}, обновлено {updated}</div>"
    resp = list_bookings(request)
    html = summary + resp.body.decode("utf-8")
    return HTMLResponse(html)

# ===== KPI edit (без изменений) =====
@router.get("/{booking_id}/edit_kpi", response_class=HTMLResponse)
def edit_kpi(request: Request, booking_id: int):
    conn = get_db()
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b: raise HTTPException(404)
    k = conn.execute("SELECT * FROM booking_kpis WHERE booking_id=?", (booking_id,)).fetchone()
    return templates.TemplateResponse("booking_kpi_form.html", {"request": request, "b": b, "k": k})

@router.post("/{booking_id}/save_kpi", response_class=HTMLResponse)
def save_kpi(request: Request, booking_id: int,
             kpi_impressions: float = Form(None),
             kpi_clicks: float = Form(None),
             kpi_uniques: float = Form(None),
             kpi_ctr: float = Form(None),
             kpi_freq: float = Form(None),
             kpi_conversions: float = Form(None),
             kpi_spend: float = Form(None),
             kpi_cpm: float = Form(None),
             kpi_cpc: float = Form(None),
             kpi_cpa: float = Form(None)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO booking_kpis(booking_id) VALUES (?)", (booking_id,))
    cur.execute(
      "UPDATE booking_kpis SET kpi_impressions=?, kpi_clicks=?, kpi_uniques=?, kpi_ctr=?, kpi_freq=?, kpi_conversions=?,"
      " kpi_spend=?, kpi_cpm=?, kpi_cpc=?, kpi_cpa=? WHERE booking_id=?",
      (kpi_impressions, kpi_clicks, kpi_uniques, kpi_ctr, kpi_freq, kpi_conversions, kpi_spend, kpi_cpm, kpi_cpc, kpi_cpa, booking_id)
    )
    conn.commit()
    return row_html(request, booking_id)

# ===== render single row =====
def row_html(request: Request, booking_id: int):
    conn = get_db(); cur = conn.cursor()
    has_campaigns = _table_exists(conn, "campaigns")
    cam_cols = _campaign_cols(conn) if has_campaigns else set()
    select_clause = _select_fragments(cam_cols)
    join_campaigns = "LEFT JOIN campaigns cam ON cam.id = b.campaign_id" if has_campaigns else ""
    row = cur.execute(f"""
      SELECT {select_clause}
      FROM bookings b
      LEFT JOIN clients c ON c.id = b.client_id
      LEFT JOIN agencies a ON a.id = b.agency_id
      LEFT JOIN booking_kpis k ON k.booking_id = b.id
      {join_campaigns}
      WHERE b.id=?
    """, (booking_id,)).fetchone()
    managers = [r[0] for r in cur.execute(
        "SELECT DISTINCT sales_manager FROM bookings WHERE sales_manager IS NOT NULL AND TRIM(sales_manager)!='' ORDER BY sales_manager"
    ).fetchall()]
    return templates.TemplateResponse("bookings_row.html", {"request": request, "row": row, "judge": judge, "managers": managers})

# ===== detail pages =====
@router.get("/{booking_id}/detail", response_class=HTMLResponse)
def detail(request: Request, booking_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not row: raise HTTPException(404)
    camp = None
    if row["campaign_id"]:
        camp = conn.execute("SELECT * FROM campaigns WHERE id=?", (row["campaign_id"],)).fetchone()
    return templates.TemplateResponse("booking_detail.html", {"request": request, "row": row, "camp": camp})

# ===== inline patch =====
@router.post("/{booking_id}/patch", response_class=HTMLResponse)
def patch_booking(request: Request, booking_id: int, field: str = Form(...), value: str = Form(None)):
    conn = get_db(); cur = conn.cursor()

    allowed = {
        "name","campaign_id","client_id","agency_id",
        "brand","legal_entity","format","buying_model","status","contract_type",
        "start_date","end_date","month_str","plan_payment_date","fact_payment_date",
        "budget_after_vat","budget_client_net","budget_client_gross","budget_after_sk",
        "inventory_total_plan","inventory_fact","inventory_commercial","inventory_bonus",
        "price_unit","price_unit_with_bonus","price_unit_with_vat","cpm_cpc_to_platform",
        "vz_percent","refund_amount","act_number","act_id","contract_id","initial_contract_id",
        "sales_manager","account_manager","comment_sales","comment_accounts",
        # вспомогательные алиасы для удобного ввода:
        "agency_name",   # -> обновим agency_id по названию
        "client_name",   # -> обновим client_id по названию
        "client_brand",  # алиас к brand
        "client_price",
    }

    if field not in allowed:
        raise HTTPException(400, f"field '{field}' not allowed")

    def to_float(v):
        try:
            return float(v.replace(",", ".")) if isinstance(v, str) else float(v)
        except Exception:
            return None

    def to_int(v):
        try:
            return int(v)
        except Exception:
            return None

    # спец‑случаи
    if field in ("agency_name", "client_name"):
        name = (value or "").strip()
        tbl = "agencies" if field == "agency_name" else "clients"
        col = "agency_id" if field == "agency_name" else "client_id"
        if name == "":
            cur.execute(f"UPDATE bookings SET {col}=NULL WHERE id=?", (booking_id,))
        else:
            cur.execute(f"INSERT OR IGNORE INTO {tbl}(name) VALUES (?)", (name,))
            new_id = cur.execute(f"SELECT id FROM {tbl} WHERE name=?", (name,)).fetchone()[0]
            cur.execute(f"UPDATE bookings SET {col}=? WHERE id=?", (new_id, booking_id))
        conn.commit()
        return row_html(request, booking_id)

    if field == "client_brand":
        field = "brand"  # алиас

    # типы
    if field in {"campaign_id","client_id","agency_id"}:
        coerced = to_int(value) if value not in (None,"","None") else None
    elif field in {
        "budget_after_vat","budget_client_net","budget_client_gross","budget_after_sk",
        "inventory_total_plan","inventory_fact","inventory_commercial","inventory_bonus",
        "price_unit","price_unit_with_bonus","price_unit_with_vat","cpm_cpc_to_platform",
        "vz_percent","refund_amount", "client_price",
    }:
        coerced = to_float(value)
        if field == "vz_percent" and coerced is not None and abs(coerced) > 1:
            coerced = coerced / 100.0  # в БД храним долей
    else:
        coerced = value

    cur.execute(f"UPDATE bookings SET {field}=? WHERE id=?", (coerced, booking_id))
    conn.commit()
    return row_html(request, booking_id)

@router.get("/{booking_id}", response_class=HTMLResponse)
def booking_detail_page(request: Request, booking_id: int):
    conn = get_db()
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b:
        raise HTTPException(404, "Booking not found")

    camp = None
    client_kpi = {}
    shots = []
    if b["campaign_id"]:
        camp = conn.execute("SELECT * FROM campaigns WHERE id=?", (b["campaign_id"],)).fetchone()
        client_kpi = get_campaign_kpi(conn, b["campaign_id"])
        shots = get_campaign_shots(conn, b["campaign_id"])

    return templates.TemplateResponse("booking_detail_page.html", {
        "request": request,
        "b": b,
        "camp": camp,
        "client_kpi": client_kpi,
        "shots": shots
    })



@router.post("/{booking_id}/save_client_kpi", response_class=HTMLResponse)
def save_client_kpi(
    request: Request,
    booking_id: int,
    campaign_id: int = Form(...),
    ctr: float | None = Form(None),
    vtr: float | None = Form(None),
    reachability: float | None = Form(None),
    bounce_rate: float | None = Form(None),
    depth: float | None = Form(None),
    time_on_site: float | None = Form(None),
    viewability: float | None = Form(None),
    unsafe: float | None = Form(None),
    givt: float | None = Form(None),
    sivt: float | None = Form(None),
    delta_impr: float | None = Form(None),
    delta_clicks: float | None = Form(None),
):
    conn = get_db()
    ensure_campaign_kpi_table(conn)
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b:
        raise HTTPException(status_code=404, detail="Booking not found")
    if not b["campaign_id"] or int(campaign_id) != int(b["campaign_id"]):
        raise HTTPException(status_code=400, detail="Booking is not linked with this campaign")

    sql = """
      INSERT INTO campaign_client_kpis
        (campaign_id, ctr, vtr, reachability, bounce_rate, depth, time_on_site,
         viewability, unsafe, givt, sivt, delta_impr, delta_clicks, updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
      ON CONFLICT(campaign_id) DO UPDATE SET
        ctr=excluded.ctr, vtr=excluded.vtr, reachability=excluded.reachability,
        bounce_rate=excluded.bounce_rate, depth=excluded.depth, time_on_site=excluded.time_on_site,
        viewability=excluded.viewability, unsafe=excluded.unsafe, givt=excluded.givt, sivt=excluded.sivt,
        delta_impr=excluded.delta_impr, delta_clicks=excluded.delta_clicks,
        updated_at=datetime('now')
    """
    conn.execute(sql, (
        campaign_id, ctr, vtr, reachability, bounce_rate, depth, time_on_site,
        viewability, unsafe, givt, sivt, delta_impr, delta_clicks
    ))
    conn.commit()
    # Небольшой флеш-текст для htmx
    return HTMLResponse(f"<span class='tag is-success is-light'>Сохранено {time.strftime('%H:%M:%S')}</span>")


@router.post("/{booking_id}/screenshots/upload", response_class=HTMLResponse)
async def upload_screenshots(request: Request, booking_id: int, files: list[UploadFile] = File(...)):
    conn = get_db()
    ensure_campaign_screens_table(conn)
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b:
        raise HTTPException(404, "Booking not found")
    if not b["campaign_id"]:
        raise HTTPException(400, "Booking is not linked with a campaign")

    campaign_id = b["campaign_id"]
    (UPLOAD_DIR / f"campaign_{campaign_id}").mkdir(parents=True, exist_ok=True)

    # Проверим, есть ли уже основной
    has_primary = conn.execute(
      "SELECT 1 FROM campaign_screenshots WHERE campaign_id=? AND is_primary=1", (campaign_id,)
    ).fetchone() is not None

    saved_any = False
    for uf in files or []:
      if not uf or not uf.filename:
          continue
      if uf.content_type not in ALLOWED_IMAGE_TYPES:
          continue
      data = await uf.read()
      fname = _safe_filename(uf.filename)
      rel_path = f"uploads/campaign_{campaign_id}/{fname}"
      abs_path = UPLOAD_DIR / f"campaign_{campaign_id}" / fname
      with open(abs_path, "wb") as out:
          out.write(data)
      conn.execute(
        "INSERT INTO campaign_screenshots (campaign_id, file_path, is_primary) VALUES (?,?,?)",
        (campaign_id, rel_path, 0)
      )
      saved_any = True

    if saved_any and not has_primary:
        # Первый загруженный станет основным
        row = conn.execute(
          "SELECT id FROM campaign_screenshots WHERE campaign_id=? ORDER BY uploaded_at DESC LIMIT 1",
          (campaign_id,)
        ).fetchone()
        if row:
            conn.execute(
              "UPDATE campaign_screenshots SET is_primary=1 WHERE id=?", (row["id"],)
            )

    conn.commit()

    shots = get_campaign_shots(conn, campaign_id)
    return templates.TemplateResponse("partials/_screenshots.html",
                                      {"request": request, "b": b, "camp": {"id": campaign_id}, "shots": shots})


@router.post("/{booking_id}/screenshots/{shot_id}/make_primary", response_class=HTMLResponse)
def make_primary_screenshot(request: Request, booking_id: int, shot_id: int):
    conn = get_db()
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b or not b["campaign_id"]:
        raise HTTPException(404, "Booking not found or not linked")
    campaign_id = b["campaign_id"]
    ensure_campaign_screens_table(conn)

    # Сбросить всем, включить одному
    conn.execute("UPDATE campaign_screenshots SET is_primary=0 WHERE campaign_id=?", (campaign_id,))
    conn.execute("UPDATE campaign_screenshots SET is_primary=1 WHERE id=? AND campaign_id=?", (shot_id, campaign_id))
    conn.commit()

    shots = get_campaign_shots(conn, campaign_id)
    return templates.TemplateResponse("partials/_screenshots.html",
                                      {"request": request, "b": b, "camp": {"id": campaign_id}, "shots": shots})


@router.post("/{booking_id}/screenshots/{shot_id}/delete", response_class=HTMLResponse)
def delete_screenshot(request: Request, booking_id: int, shot_id: int):
    conn = get_db()
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b or not b["campaign_id"]:
        raise HTTPException(404, "Booking not found or not linked")
    campaign_id = b["campaign_id"]
    ensure_campaign_screens_table(conn)

    row = conn.execute("SELECT file_path, is_primary FROM campaign_screenshots WHERE id=? AND campaign_id=?",
                       (shot_id, campaign_id)).fetchone()
    if row:
        # удалить файл
        try:
            abs_path = Path("app/static") / row["file_path"]
            if abs_path.exists():
                abs_path.unlink()
        except Exception:
            pass
        conn.execute("DELETE FROM campaign_screenshots WHERE id=?", (shot_id,))
        conn.commit()

        # если удалили основной — назначим новый при наличии
        if row["is_primary"]:
            new_main = conn.execute(
              "SELECT id FROM campaign_screenshots WHERE campaign_id=? ORDER BY uploaded_at DESC LIMIT 1",
              (campaign_id,)
            ).fetchone()
            if new_main:
                conn.execute("UPDATE campaign_screenshots SET is_primary=1 WHERE id=?", (new_main["id"],))
                conn.commit()

    shots = get_campaign_shots(conn, campaign_id)
    return templates.TemplateResponse("partials/_screenshots.html",
                                      {"request": request, "b": b, "camp": {"id": campaign_id}, "shots": shots})

@router.post("/{booking_id}/delete", response_class=HTMLResponse)
def delete_booking(booking_id: int):
    """
    Удаление строки из bookings (и связанных KPI, если таблица есть).
    Используется из bookings_row.html:
      hx-post="/bookings/{{ row.id }}/delete"
      hx-target="#booking-{{ row.id }}"
      hx-swap="outerHTML"
    """
    conn = get_db()
    cur = conn.cursor()

    # try: почистить KPI, если таблица существует
    try:
        cur.execute("DELETE FROM booking_kpis WHERE booking_id=?", (booking_id,))
    except sqlite3.OperationalError:
        # таблицы может не быть — просто игнорируем
        pass

    # при желании можно так же почистить margin_stats и прочие связанные таблицы
    try:
        cur.execute("DELETE FROM margin_stats WHERE booking_id=?", (booking_id,))
    except sqlite3.OperationalError:
        pass

    # основная запись
    cur.execute("DELETE FROM bookings WHERE id=?", (booking_id,))
    conn.commit()

    # для htmx hx-swap="outerHTML" достаточно вернуть пустой HTML
    return HTMLResponse("")
