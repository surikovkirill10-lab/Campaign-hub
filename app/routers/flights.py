# app/routers/flights.py
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from bs4 import BeautifulSoup
import re
from datetime import date
from urllib.parse import urljoin

# 1) общий конфиг и сессия Cats — используем то же, что в кампаниях
from app.services.config_store import get_effective_system_config
from app.services.cats_export import _ensure_session  # авторизованная requests.Session
from app.db import engine

router = APIRouter()

# ---------- helpers ----------

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ").strip())

def _lc(s: str) -> str:
    return _norm(s).lower()

def _to_int(s: str):
    if s is None:
        return None
    digits = re.sub(r"[^\d]", "", str(s))
    return int(digits) if digits else None

def _assert_authed_response(r):
    # если сессия просрочилась и нас редиректит на логин — бросаем ошибку
    if r.status_code in (301,302,303,307,308) and "login" in (r.headers.get("Location","") or "").lower():
        raise RuntimeError("Cats session expired (redirect to login)")
    head = (r.content[:800].decode("utf-8", errors="ignore") or "").lower()
    if "<html" in head and "login" in head and "password" in head:
        raise RuntimeError("Cats returned login page (auth required)")
    if r.status_code != 200:
        raise RuntimeError(f"Cats HTTP {r.status_code}")

def _find_all_link(soup, base_url):
    # «Все (N)»
    a = soup.find("a", string=re.compile(r"^\s*Все\s*\(\d+\)\s*$"))
    return urljoin(base_url, a["href"]) if a and a.has_attr("href") else None

def _collect_page_urls(soup, base_url):
    # номера страниц в пагинации
    out = []
    for a in soup.find_all("a", href=True):
        t = (_lc(a.get_text()))
        if re.fullmatch(r"\d+", t):
            out.append(urljoin(base_url, a["href"]))
    return sorted(set(out))

def _parse_creatives_table(html: bytes):
    """Парсим основную таблицу «Creatives» и вытаскиваем строки как флайты.
       Забираем: id, name, campaign_name, start, end (если есть в списке)."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    headers = [_lc(th.get_text()) for th in table.select("thead th")]
    if not headers:
        first = table.select_one("tbody tr")
        if first:
            headers = [_lc(td.get_text()) for td in first.find_all(["td","th"])]

    def idx(*names):
        for n in names:
            if n in headers:
                return headers.index(n)
        return None

    col_id    = idx("id")
    col_name  = idx("название", "name")
    col_camp  = idx("название кампании", "кампания", "campaign")
    col_start = idx("старт", "начало", "start")
    col_end   = idx("завершение", "окончание", "end")

    rows = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i):
            if i is None or i >= len(tds):
                return None
            return _norm(tds[i].get_text(" "))

        rid   = _to_int(cell(col_id))
        name  = cell(col_name)
        cname = cell(col_camp)
        start = cell(col_start)
        end   = cell(col_end)

        if not rid:
            continue

        # фильтруем Foxible
        if name and ("foxible" in name.lower()):
            continue

        rows.append({
            "id": rid,
            "name": name or "",
            "campaign_name": cname or "",
            "start": start or "",
            "end": end or "",
        })
    return rows

# ---------- DB bootstrap ----------

def _ensure_flights_table():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS flights(
            id            INTEGER PRIMARY KEY,
            name          TEXT,
            campaign_name TEXT,
            start         TEXT,
            end           TEXT,
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_flights_name ON flights(name);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_flights_campaign ON flights(campaign_name);"))

# ---------- Routes ----------

@router.get("/campaigns/flights", response_class=HTMLResponse)
def flights_index(request: Request):
    _ensure_flights_table()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, name, campaign_name, start, end
            FROM flights ORDER BY id DESC
        """)).fetchall()

    # компактный листинг + кнопка «импорт»
    html = [
        "<div class='box p-3'>",
        "<button class='button is-info is-small' "
        "hx-post='/campaigns/flights/import_cats' "
        "hx-target='#flights-status' hx-swap='innerHTML'>Import Cats</button> ",
        "<span id='flights-status' class='tag is-light'>—</span>",
        "</div>",
        "<table class='table is-striped is-fullwidth'>",
        "<thead><tr><th>ID</th><th>Name</th><th>Campaign</th><th>Start</th><th>End</th></tr></thead><tbody>"
    ]
    for r in rows:
        html.append(f"<tr><td>{r.id}</td><td>{r.name}</td><td>{r.campaign_name}</td><td>{r.start}</td><td>{r.end}</td></tr>")
    html.append("</tbody></table>")
    return HTMLResponse("".join(html))

@router.post("/campaigns/flights/import_cats", response_class=HTMLResponse)
def flights_import_cats():
    _ensure_flights_table()

    cfg = get_effective_system_config("config.yaml") or {}
    base = (cfg.get("connect_url") or cfg.get("base_url") or "").rstrip("/")
    if not base:
        return HTMLResponse("<span class='tag is-danger'>No base_url / connect_url in config</span>")

    s = _ensure_session()

    # 1-й день текущего месяца (dd.mm.YYYY)
    date_begin = date.today().replace(day=1).strftime("%d.%m.%Y")

    # URL из вашего примера (без export=xls), поле сортировки по creative_id desc
    list_url = (
        f"{base}/iface/creatives/"
        f"?limit_date_begin={date_begin}&limit_date_end=&name=&campaign_id=&creative_id="
        f"&mediaplan_id=&template_id=&show_options_on=1&field=creative_id&order=desc&page=1"
    )

    # открываем первую страницу
    r = s.get(list_url, timeout=60, allow_redirects=False)
    _assert_authed_response(r)
    soup_first = BeautifulSoup(r.content, "lxml")

    # пробуем ссылку «Все (N)»
    all_href = _find_all_link(soup_first, base)
    html_pages = []
    if all_href:
        r_all = s.get(all_href, timeout=180, allow_redirects=False)
        _assert_authed_response(r_all)
        html_pages.append(r_all.content)
    else:
        # забираем все страницы пагинации (включая первую)
        page_urls = _collect_page_urls(soup_first, base)
        html_pages.append(r.content)
        for u in page_urls:
            rp = s.get(u, timeout=90, allow_redirects=False)
            _assert_authed_response(rp)
            html_pages.append(rp.content)

    # парсим
    flights = []
    for html in html_pages:
        flights.extend(_parse_creatives_table(html))

    if not flights:
        return HTMLResponse("<span class='tag is-light'>No flights found</span>")

    # сохраняем (только новые id)
    inserted = 0
    with engine.begin() as conn:
        for f in flights:
            conn.execute(text("""
                INSERT OR IGNORE INTO flights(id, name, campaign_name, start, end)
                VALUES (:id, :name, :campaign_name, :start, :end)
            """), f)
            # посчитать реально вставленные
        inserted = conn.execute(text("SELECT changes()")).scalar()

    return HTMLResponse(f"<span class='tag is-success'>Imported: {inserted} flights</span>")
