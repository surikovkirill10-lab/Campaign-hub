# routers/data_flow.py
from pathlib import Path
import json
from fastapi import APIRouter, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

CONFIG_DIR = Path("config")
CONFIG_PATH = CONFIG_DIR / "data_flow.json"

def _default_flow_config():
    # Конфигурация по умолчанию
    return {
        "paths": {
            "cats_csv_template": "data/cats/{campaign_id}/latest_normalized.csv",
            "yandex_db_path": "yandex_metrics.db",
            "campaign_db_path": "campaign_hub.db",
            "show_raw_layer": True
        },
        "metrics": {
            # Cats
            "impressions": {"source": "cats", "field": "impressions", "desc": "Показы"},
            "clicks":      {"source": "cats", "field": "clicks",      "desc": "Клики"},
            "reach":       {"source": "cats", "field": "uniques",     "desc": "Охват"},
            "vtr":         {"source": "cats", "field": "vtr_percent", "desc": "VTR (%)"},
            "ctr":         {"source": "cats", "field": "ctr_percent", "desc": "CTR (%)"},
            "frequency":   {"source": "derived", "formula": "impressions/reach", "desc": "Частота"},
            # Yandex Update
            "visits":         {"source": "yandex", "field": "visits",         "desc": "Визиты"},
            "reachability":   {"source": "derived", "formula": "visits/clicks","desc": "Доходимость"},
            "bounce_rate":    {"source": "yandex", "field": "bounce_rate",    "desc": "Отказы (%)"},
            "avg_time_sec":   {"source": "yandex", "field": "avg_time_sec",   "desc": "Время (сек)"},
            "page_depth":     {"source": "yandex", "field": "page_depth",     "desc": "Глубина"},
            "post_click_conv":{"source": "yandex", "field": "conversions",    "desc": "Конверсии post‑click"},
            # Verificator (заготовки)
            "disp_delta_impr":  {"source": "verifier", "field": "impressions_delta", "desc": "Расхождение показы"},
            "disp_delta_clicks":{"source": "verifier", "field": "clicks_delta",      "desc": "Расхождение клики"},
            "ivt_impr":         {"source": "verifier", "field": "ivt_impressions",   "desc": "IVT показы"},
            "sivt_impr":        {"source": "verifier", "field": "sivt_impressions",  "desc": "SIVT показы"},
            "givt_impr":        {"source": "verifier", "field": "givt_impressions",  "desc": "GIVT показы"},
            "ivt_clicks":       {"source": "verifier", "field": "ivt_clicks",        "desc": "IVT клики"},
            "sivt_clicks":      {"source": "verifier", "field": "sivt_clicks",       "desc": "SIVT клики"},
            "givt_clicks":      {"source": "verifier", "field": "givt_clicks",       "desc": "GIVT клики"},
            "brand_safety":     {"source": "verifier", "field": "brand_safety",      "desc": "Brand safety"},
            "post_view_conv":   {"source": "verifier", "field": "post_view_conv",    "desc": "Конверсии post‑view"}
        }
    }

def _load_flow_config():
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return _default_flow_config()

def _save_flow_config(cfg: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

@router.get("/data-flow", response_class=HTMLResponse)
def data_flow_page(request: Request):
    cfg = _load_flow_config()
    return templates.TemplateResponse("data_flow.html", {"request": request, "cfg_json": json.dumps(cfg, ensure_ascii=False)})

@router.post("/data-flow/save")
async def data_flow_save(payload: dict = Body(...)):
    try:
        _save_flow_config(payload)
        return JSONResponse({"ok": True, "saved": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
