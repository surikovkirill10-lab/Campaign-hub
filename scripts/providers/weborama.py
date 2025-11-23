
from __future__ import annotations
import io
from typing import Dict, List, Tuple
import pandas as pd

def _find_sheet(xls: pd.ExcelFile) -> str:
    names = xls.sheet_names
    for want in ["Froud Total", "Fraud Total"]:
        for n in names:
            if n.strip().lower() == want.lower():
                return n
    return names[0]

def _to_percent(v):
    # Input can be like 0.07 or "0,07%" (meaning 0.07%)
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(',', '.').replace(' ', '')
    if s.endswith('%'):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return None

def parse_weborama_xlsx(content: bytes) -> Dict[str, Dict[str, float]]:
    """Parse bytes of Weborama XLSX and return date->metrics.
    Uses 'Froud Total' sheet.
    Metrics: impressions (Imp WCM), givt_rate_pct, sivt_rate_pct, ivt_total_pct,
             givt_impr, sivt_impr (derived from rates).
    """
    xls = pd.ExcelFile(io.BytesIO(content))
    sheet = _find_sheet(xls)
    df = pd.read_excel(xls, sheet_name=sheet)

    # normalize date
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["Date"])
    df["date"] = df["Date"].dt.date.astype(str)

    # numeric conversions
    for c in ["Imp WCM", "GIVT (%)", "SIVT (%)", "Final percent"]:
        if c in df.columns:
            if c == "Imp WCM":
                df[c] = pd.to_numeric(df[c], errors="coerce")
            else:
                df[c] = df[c].map(_to_percent)

    out = {}
    for date, g in df.groupby("date"):
        impr = float(g["Imp WCM"].sum()) if "Imp WCM" in g.columns else 0.0
        givt_pct = g["GIVT (%)"].mean() if "GIVT (%)" in g.columns else None
        sivt_pct = g["SIVT (%)"].mean() if "SIVT (%)" in g.columns else None
        ivt_total_pct = g["Final percent"].mean() if "Final percent" in g.columns else None
        def cnt(pct):
            return float(round(impr * (pct or 0.0) / 100.0)) if impr else 0.0
        out[date] = {
            "impressions": impr,
            "givt_rate_pct": givt_pct,
            "sivt_rate_pct": sivt_pct,
            "ivt_total_pct": ivt_total_pct,
            "givt_impr": cnt(givt_pct) if givt_pct is not None else None,
            "sivt_impr": cnt(sivt_pct) if sivt_pct is not None else None,
            # clicks/unsafe/viewable not available here for MVP
            "clicks": None,
            "unsafe_impr": None,
            "viewable_impr": None
        }
    return out
