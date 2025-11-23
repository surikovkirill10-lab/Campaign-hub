
from __future__ import annotations
import io
from typing import Dict, List, Tuple
import pandas as pd

EXPECTED = {
    "date": "Date",
    "impr_net": "Impressions (Net)",
    "clk_net": "Clicks (Net)",
    "impr_givt": "Impressions (GIVT)",
    "givt_rate": "GIVT Rate",
    "clk_givt": "Clicks (GIVT)",
    "givt_clk_rate": "GIVT Clicks Rate",
    "impr_sivt": "Impressions (SIVT)",
    "sivt_rate": "SIVT Rate",
    "unsafe_impr": "Negative>Unsafe Impressions",
    "unsafe_rate": "Unsafe Rate",
    "recordable_impr": "Total Recordable Impressions",
    "recordable_rate": "Recordable Impressions Rate",
    "viewable_impr": "Total Viewable Impressions (IAB)",
    "viewable_rate": "Viewable Impressions Rate (IAB)",
}

def _detect_header_row(df_raw: pd.DataFrame) -> int:
    for i in range(min(len(df_raw), 30)):
        row = df_raw.iloc[i].fillna('').astype(str).str.strip().str.lower().tolist()
        if 'date' in row and ('impressions (net)' in row or 'impressions(net)' in row):
            return i
    return 10  # fallback

def parse_adserving_xlsx(content: bytes) -> Dict[str, Dict[str, float]]:
    """Parse XLSX bytes; return mapping date(YYYY-MM-DD) -> metrics dict.
    Metrics: impressions, clicks, givt_impr, givt_rate_pct, sivt_impr, sivt_rate_pct,
             unsafe_impr, unsafe_rate_pct, viewable_impr, viewable_rate_pct,
             recordable_impr, recordable_rate_pct
    All *_pct returned in percent units (e.g., 0.07 means 0.07%).
    """
    xls = pd.ExcelFile(io.BytesIO(content))
    sheet = xls.sheet_names[0]
    raw = pd.read_excel(xls, sheet_name=sheet, header=None)
    hdr = _detect_header_row(raw)
    df = pd.read_excel(xls, sheet_name=sheet, header=hdr)

    # keep only expected columns if present
    keep = [c for c in EXPECTED.values() if c in df.columns]
    df = df[keep].copy()

    # normalize date
    df[EXPECTED["date"]] = pd.to_datetime(df[EXPECTED["date"]], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=[EXPECTED["date"]])
    df["date"] = df[EXPECTED["date"]].dt.date.astype(str)

    # numeric conversions
    def _to_float(s):
        return pd.to_numeric(s, errors="coerce")

    for key, col in EXPECTED.items():
        if col in df.columns and key != "date":
            df[col] = _to_float(df[col])

    # aggregate by date (sum counts)
    agg_map = {}
    # sums
    sum_cols = ["impr_net","clk_net","impr_givt","impr_sivt","unsafe_impr",
                "recordable_impr","viewable_impr"]
    # averages by recomputation from sums (preferred)
    out = {}
    for date, g in df.groupby("date"):
        sums = {}
        for k in sum_cols:
            col = EXPECTED.get(k)
            sums[k] = float(g[col].sum()) if col in g.columns else 0.0

        # derive percent rates from sums if possible
        def pct(numer, denom):
            return float(numer * 100.0 / denom) if denom and denom > 0 else None

        impr = sums.get("impr_net", 0.0)
        viewable_impr = sums.get("viewable_impr", 0.0)
        recordable_impr = sums.get("recordable_impr", 0.0)
        givt_impr = sums.get("impr_givt", 0.0)
        sivt_impr = sums.get("impr_sivt", 0.0)
        unsafe_impr = sums.get("unsafe_impr", 0.0)

        out[date] = {
            "impressions": impr,
            "clicks": sums.get("clk_net", 0.0),
            "givt_impr": givt_impr,
            "givt_rate_pct": pct(givt_impr, impr),
            "sivt_impr": sivt_impr,
            "sivt_rate_pct": pct(sivt_impr, impr),
            "unsafe_impr": unsafe_impr if "unsafe_impr" in sums else None,
            "unsafe_rate_pct": pct(unsafe_impr, impr) if "unsafe_impr" in sums else None,
            "viewable_impr": viewable_impr if "viewable_impr" in sums else None,
            "viewable_rate_pct": pct(viewable_impr, impr) if "viewable_impr" in sums else None,
            "recordable_impr": recordable_impr if "recordable_impr" in sums else None,
            "recordable_rate_pct": pct(recordable_impr, impr) if "recordable_impr" in sums else None,
        }
    return out
