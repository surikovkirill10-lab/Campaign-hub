from typing import Optional
import logging
from datetime import date
import json
from sqlalchemy import text
from ..database import engine

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from urllib.parse import quote


from app.services.mediaplanner import (
    Mode,
    ParsedBrief,
    PlanMeta,
    parse_brief_with_llm,
    calculate_plan,
    build_excel,
    build_campaign_name_from_parsed,
)

logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory="app/templates")

router = APIRouter(
    prefix="/sales",
    tags=["Sales"],
)


# --- базовый вход в раздел /sales ---> перенаправляем на считалку ---
@router.get("", response_class=HTMLResponse)
async def sales_root(request: Request) -> HTMLResponse:
    # сразу открываем вкладку "Считалка"
    return templates.TemplateResponse(
        "sales_calc.html",
        {
            "request": request,
            "active_section": "sales",
            "active_sales_tab": "calc",
        },
    )


# можно оставить и /sales/calc, чтобы боковое меню не 404ило
@router.get("/calc", response_class=HTMLResponse)
async def sales_calc_page(request: Request) -> HTMLResponse:
    """
    Страница с формой 'Считалка'.
    """
    return templates.TemplateResponse(
        "sales_calc.html",
        {
            "request": request,
            "active_section": "sales",
            "active_sales_tab": "calc",
        },
    )


@router.post("/calc/preview", response_class=HTMLResponse)
async def sales_calc_preview(
    request: Request,
    brief: str = Form(...),
    mode: Mode = Form("capacity"),
    budget: Optional[float] = Form(None),
) -> HTMLResponse:
    """
    Превью: бриф -> LLM -> расчёт.
    Если запрос пришёл от htmx — отдаём только кусок.
    Если обычный POST — рендерим всю страницу Sales с результатом.
    """
    context = {
        "request": request,
        "active_section": "sales",
        "active_sales_tab": "calc",
    }

    try:
        parsed: ParsedBrief = await parse_brief_with_llm(brief)
        parsed.raw_text = brief

        rows = calculate_plan(parsed, mode=mode, user_budget=budget)

        context.update(
            parsed=parsed,
            rows=rows,
            mode=mode,
            user_budget=budget,
            error=None,
        )
    except Exception as exc:
        logger.exception("Ошибка расчёта Sales/Считалка")
        context.update(
            parsed=None,
            rows=[],
            mode=mode,
            user_budget=budget,
            error=str(exc),
        )

    # если это htmx-запрос — шлём только вставку
    is_htmx = request.headers.get("HX-Request") == "true"
    template_name = "sales_calc_result.html" if is_htmx else "sales_calc.html"

    return templates.TemplateResponse(template_name, context)


@router.post("/calc/export")
async def sales_calc_export(
    brief: str = Form(...),
    mode: Mode = Form("capacity"),
    budget: Optional[float] = Form(None),

    # правки из формы нормализованного брифа
    agency: Optional[str] = Form(None),
    client: Optional[str] = Form(None),
    brand: Optional[str] = Form(None),
    campaign_name: Optional[str] = Form(None),
    manager: Optional[str] = Form(None),
    prepared_by: Optional[str] = Form(None),
    brief_date: Optional[date] = Form(None),
) -> StreamingResponse:
    """
    Скачивание Excel: бриф -> LLM -> расчёт -> xlsx по шаблону Innovation Lab.
    Учитывает правки пользователя (агентство/клиент/бренд/менеджер/подготовил/дата).
    """
    parsed: ParsedBrief = await parse_brief_with_llm(brief)

    # применяем ручные правки по клиенту/бренду, если они пришли
    if client:
        parsed.client = client
    if brand:
        parsed.brand = brand

    rows = calculate_plan(parsed, mode=mode, user_budget=budget)

    auto_campaign_name = build_campaign_name_from_parsed(parsed)
    campaign_name_final = campaign_name or auto_campaign_name

    meta = PlanMeta(
        agency=agency or None,
        advertiser=parsed.client,
        brand=parsed.brand,
        campaign_name=campaign_name_final,
        manager=manager or None,
        prepared_by=prepared_by or None,
        brief_date=brief_date,
        ages=parsed.ages,
        genders=parsed.genders,
        interests=parsed.interests,
    )

    excel_stream = build_excel(meta=meta, rows=rows)

    try:
        norm_obj = {
            "client": parsed.client,
            "brand": parsed.brand,
            "period_start": parsed.period_start,
            "period_end": parsed.period_end,
            "formats": parsed.formats,
            "geo": parsed.geo,
            "ages": parsed.ages,
            "genders": parsed.genders,
            "interests": parsed.interests,
            "goal": parsed.goal,
            "frequency": parsed.frequency,
            "budget": parsed.budget,
            "mode": mode.value if hasattr(mode, "value") else str(mode),
        }
        norm_json = json.dumps(norm_obj, ensure_ascii=False)

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO sales_calculations (
                        client, brand, campaign_name, agency, manager, prepared_by,
                        brief, normalized_brief, mode, budget,
                        period_start, period_end
                    )
                    VALUES (
                        :client, :brand, :campaign_name, :agency, :manager, :prepared_by,
                        :brief, :normalized_brief, :mode, :budget,
                        :period_start, :period_end
                    )
                """),
                {
                    "client": parsed.client,
                    "brand": parsed.brand,
                    "campaign_name": campaign_name_final,
                    "agency": agency,
                    "manager": manager,
                    "prepared_by": prepared_by,
                    "brief": brief,
                    "normalized_brief": norm_json,
                    "mode": mode.value if hasattr(mode, "value") else str(mode),
                    "budget": budget or parsed.budget,
                    "period_start": parsed.period_start,
                    "period_end": parsed.period_end,
                },
            )
    except Exception:
        logger.exception("Не удалось записать sales_calculation в БД")

    # Человеческое имя файла — как название РК, но без слэшей
    raw_name = campaign_name_final or "media_plan"
    clean_name = raw_name.replace("/", "_").strip()
    if not clean_name:
        clean_name = "media_plan"

    # ASCII‑fallback (для старых клиентов и latin-1)
    ascii_fallback = "".join(
        (ch if ch.isascii() and ch not in '/\\:*?"<>|' else "_")
        for ch in clean_name
    ).strip(" ._") or "media_plan"
    ascii_filename = f"{ascii_fallback}.xlsx"

    # Основное имя — UTF‑8 через filename* (браузер покажет русское название)
    utf8_encoded = quote(clean_name + ".xlsx", safe="")
    content_disposition = (
        f"attachment; filename=\"{ascii_filename}\"; "
        f"filename*=UTF-8''{utf8_encoded}"
    )

    return StreamingResponse(
        excel_stream,
        media_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": content_disposition
        },
    )

@router.get("/computations", response_class=HTMLResponse)
async def sales_computations(
    request: Request,
    q: Optional[str] = None,
    period: Optional[str] = None,  # YYYY-MM
    sort: str = "created_at",
    dir: str = "desc",
) -> HTMLResponse:
    """
    Список всех расчётов медиапланов.
    Фильтры и сортировка — по аналогии с bookings.
    """
    rows = []
    try:
        params = {}
        conditions = []

        if q:
            conditions.append("""
                (client ILIKE :q OR brand ILIKE :q OR campaign_name ILIKE :q)
            """)
            params["q"] = f"%{q}%"

        if period:
            try:
                year, month = map(int, period.split("-"))
                from datetime import date as _date
                start = _date(year, month, 1)
                if month == 12:
                    end = _date(year + 1, 1, 1)
                else:
                    end = _date(year, month + 1, 1)
                conditions.append("created_at >= :p_start AND created_at < :p_end")
                params["p_start"] = start
                params["p_end"] = end
            except Exception:
                pass

        # безопасный маппинг полей для сортировки
        sort_map = {
            "id": "id",
            "created_at": "created_at",
            "client": "client",
            "brand": "brand",
            "campaign_name": "campaign_name",
            "period_start": "period_start",
        }
        sort_col = sort_map.get(sort, "created_at")
        dir_sql = "asc" if dir == "asc" else "desc"

        where_sql = ""
        if conditions:
            where_sql = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                id, created_at,
                client, brand, campaign_name,
                agency, manager, prepared_by,
                mode, budget,
                period_start, period_end
            FROM sales_calculations
            {where_sql}
            ORDER BY {sort_col} {dir_sql}
            LIMIT 300
        """

        with engine.begin() as conn:
            db_rows = conn.execute(text(sql), params).fetchall()
            rows = [
                {
                    "id": r.id,
                    "created_at": r.created_at,
                    "client": r.client,
                    "brand": r.brand,
                    "campaign_name": r.campaign_name,
                    "agency": r.agency,
                    "manager": r.manager,
                    "prepared_by": r.prepared_by,
                    "mode": r.mode,
                    "budget": r.budget,
                    "period_start": r.period_start,
                    "period_end": r.period_end,
                }
                for r in db_rows
            ]
    except Exception:
        logger.exception("Ошибка чтения sales_calculations")

    return templates.TemplateResponse(
        "sales_computations.html",
        {
            "request": request,
            "rows": rows,
            "q": q or "",
            "period": period or "",
            "sort": sort,
            "dir": dir,
            "active_section": "sales",
            "active_sales_tab": "computations",
        },
    )
    


