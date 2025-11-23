from pathlib import Path
import json

_CFG = None
_CONFIG_PATH = Path("config/data_flow.json")

def load_cfg():
    global _CFG
    if _CFG is None:
        try:
            _CFG = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            _CFG = {"paths":{}, "metrics":{}}
    return _CFG

def metric_map():
    return load_cfg().get("metrics", {})

def paths():
    p = load_cfg().get("paths", {})
    return {
        "cats_csv_template": p.get("cats_csv_template"),
        "yandex_db_path":    p.get("yandex_db_path", "yandex_metrics.db"),
        "campaign_db_path":  p.get("campaign_db_path", "campaign_hub.db"),
        "show_raw_layer":    bool(p.get("show_raw_layer", True)),
    }
