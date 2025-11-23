from __future__ import annotations
import io, re, json, time, logging, datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
import pandas as pd

from app.services.config_store import get_effective_system_config
from app.services.cats_front import cats_front_ping  # используем тот же логин через форму

log = logging.getLogger("app")

DATA_DIR = Path("data") / "cats"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ------------------ helpers: decode / filename ------------------

def _decode_rfc5987_filename(cdisp: str) -> Optional[str]:
    """
    Content-Disposition: filename*=UTF-8''%D0%A1%D1%82...
    """
    m = re.search(r"filename\*\s*=\s*([^']*)''([^;]+)", cdisp or "", flags=re.IGNORECASE)
    if not m:
        return None
    enc = (m.group(1) or "UTF-8").upper()
    try:
        from urllib.parse import unquote
        raw = unquote(m.group(2))
        if enc == "UTF-8":
            return raw
        return raw.encode("latin-1", errors="ignore").decode(enc, errors="ignore")
    except Exception:
        return None

def _fix_legacy_filename(cdisp: str, fallback: Optional[str]) -> Optional[str]:
    """
    Пробуем RFC5987, затем обычный filename="...", перекодируя latin-1 -> utf-8.
    """
    fn = _decode_rfc5987_filename(cdisp or "")
    if fn:
        return fn
    m = re.search(r'filename\s*=\s*"([^"]+)"', cdisp or "", flags=re.IGNORECASE)
    if m:
        raw = m.group(1)
        try:
            return raw.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            return raw
    return fallback

def _sanitize_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", (name or "")).strip()
    return name or "cats_export"

# ------------------ config / session ------------------

def _build_download_conf() -> Dict[str, Any]:
    sys = get_effective_system_config("config.yaml")
    cats = (sys.get("cats") or {})
    download = (cats.get("download") or {})
    return {
        "url_template": download.get("url_template") or
            "https://catsnetwork.ru/iface/campaigns/stat/uniques/{id}?&export=xlsx",
        "method": (download.get("method") or "GET").upper(),
        "format": (download.get("format") or "xlsx").lower(),
        "encoding": download.get("encoding") or "cp1251",  # для csv
        "delimiter": download.get("delimiter") or ";",     # для csv
        "filename_field": download.get("filename_field") or "Content-Disposition",
        "column_map": download.get("column_map") or {},
    }

def _ensure_session() -> requests.Session:
    """
    Авторизованная requests.Session через ту же форму логина.
    """
    res = cats_front_ping(timeout=20.0)
    if not res.get("ok"):
        raise RuntimeError(f"Cats login failed: {res.get('error') or 'unknown'}")

    sys = get_effective_system_config("config.yaml")
    auth = sys.get("auth") or {}
    form = auth.get("form") or {}

    login_url   = form.get("login_url")
    uname_field = form.get("username_field") or "username"
    pwd_field   = form.get("password_field") or "password"
    submit_name = form.get("submit_name")
    extra_fields= form.get("extra_fields") or {}

    user = auth.get("username") or ""
    pwd  = auth.get("password") or ""
    if not (login_url and user and pwd):
        raise RuntimeError("Missing login_url/username/password in config (auth.form)")

    s = requests.Session()
    try:
        s.get(login_url, timeout=15)
    except requests.RequestException:
        pass

    payload = {uname_field: user, pwd_field: pwd}
    if submit_name:
        payload[submit_name] = submit_name
    if isinstance(extra_fields, dict) and extra_fields:
        payload.update(extra_fields)

    r = s.post(login_url, data=payload, timeout=20, allow_redirects=True)
    r.raise_for_status()
    return s

# ------------------ download / parse / normalize ------------------

def download_stat_file_by_id(stat_id: str) -> Tuple[Path, bytes, Dict[str, Any]]:
    """
    Скачивает файл экспорта по ID и сохраняет RAW.
    Возвращает (путь_сохранения, байты, мета).
    """
    conf = _build_download_conf()
    url = conf["url_template"].format(id=stat_id)
    method = conf["method"]

    s = _ensure_session()
    log.info("Cats download: %s %s", method, url)
    resp = s.post(url, timeout=60) if method == "POST" else s.get(url, timeout=60)
    resp.raise_for_status()

    content = resp.content
    ctype = resp.headers.get("Content-Type", "")
    cdisp = resp.headers.get("Content-Disposition", "")

    filename: Optional[str] = None
    if cdisp:
        filename = _fix_legacy_filename(cdisp, None)
    if not filename:
        ext = ".csv" if conf["format"] == "csv" else ".xlsx"
        filename = f"cats_export_{stat_id}{ext}"
    filename = _sanitize_name(filename)

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = DATA_DIR / stat_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ts}_{filename}"
    out_path.write_bytes(content)

    meta = {"content_type": ctype, "content_disposition": cdisp, "path": str(out_path)}
    return out_path, content, meta

def parse_stat_bytes(content: bytes, fmt: str, encoding: str, delimiter: str) -> pd.DataFrame:
    """
    Парсит байты CSV/XLSX в DataFrame.
    """
    if fmt == "xlsx":
        bio = io.BytesIO(content)
        df = pd.read_excel(bio)
    else:
        text = content.decode(encoding, errors="replace")
        df = pd.read_csv(io.StringIO(text), delimiter=delimiter)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def normalize_columns(df: pd.DataFrame, column_map: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Переименовывает колонки по column_map: целевое имя -> список возможных исходных.
    """
    lower = {str(c).lower(): c for c in df.columns}
    ren: Dict[str, str] = {}
    for target, variants in (column_map or {}).items():
        for v in variants:
            src = lower.get(str(v).lower())
            if src:
                ren[src] = target
                break
    if ren:
        df = df.rename(columns=ren)
    return df

def _coerce_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(" ", "").replace(",", ".").replace("%", "")
    try:
        return float(s)
    except Exception:
        return None

def _normalize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Готовит поля под дашборд:
    date, impressions, clicks, uniques, ctr_percent/ratio, vtr_percent/ratio, freq (Показы/Охват)
    """
    # Базовая рус->канонические
    rename: Dict[str, str] = {}
    if "Переходы" in df.columns: rename["Переходы"] = "clicks"
    if "Показы"   in df.columns: rename["Показы"]   = "impressions"
    if "Охват"    in df.columns: rename["Охват"]    = "uniques"
    if "День"     in df.columns: rename["День"]     = "date"
    if rename:
        df = df.rename(columns=rename)

    # Дата -> ISO
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.date

    # CTR/VTR -> percent + ratio
    for src, base in (("CTR", "ctr"), ("VTR", "vtr")):
        if src in df.columns:
            vals = df[src].apply(_coerce_number)
            df[f"{base}_percent"] = vals
            df[f"{base}_ratio"]   = vals.apply(lambda v: (v/100.0) if v is not None else None)

    # Частота = Показы / Охват
    if "impressions" in df.columns and "uniques" in df.columns:
        imp = df["impressions"].apply(_coerce_number)
        uni = df["uniques"].apply(_coerce_number)
        df["freq"] = [round(i/u, 2) if (u and u > 0 and i is not None) else None for i, u in zip(imp, uni)]

    return df

# ------------------ ingest ------------------

def ingest_stat(df: pd.DataFrame, stat_id: str) -> Dict[str, Any]:
    """
    Сохраняет нормализованный CSV рядом с RAW.
    """
    out_dir = DATA_DIR / stat_id
    out_dir.mkdir(parents=True, exist_ok=True)
    clean_path = out_dir / "latest_normalized.csv"
    df.to_csv(clean_path, index=False)
    return {"normalized_path": str(clean_path), "rows": int(df.shape[0]), "cols": list(df.columns)}

def export_and_ingest(stat_id: str) -> Dict[str, Any]:
    """
    Полный цикл: скачать -> распарсить -> нормализовать -> сохранить.
    """
    conf = _build_download_conf()
    path, content, meta = download_stat_file_by_id(stat_id)
    df = parse_stat_bytes(content, fmt=conf["format"], encoding=conf["encoding"], delimiter=conf["delimiter"])
    df = normalize_columns(df, conf["column_map"])
    df = _normalize_metrics(df)
    ing = ingest_stat(df, stat_id)
    return {"ok": True, "raw_path": str(path), "meta": meta, "ingest": ing}
