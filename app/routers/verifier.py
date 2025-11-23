"""Verifier router: update settings per campaign and trigger imports (campaigns & groups)."""
from __future__ import annotations

import re
import sys
import subprocess
from html import escape
from pathlib import Path
from typing import Dict, List

from fastapi import APIRouter, Body
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool
from sqlalchemy import text

from app.database import engine

router = APIRouter()

ALLOWED_PROVIDERS = ("weborama", "adserving", "adriver", "targetads")
ALLOWED_MATCH = ("exact","contains","startswith","endswith","regex")

DDL = [
    # campaigns
    """CREATE TABLE IF NOT EXISTS verifier_campaigns (
        campaign_id     INTEGER PRIMARY KEY,
        provider        TEXT NOT NULL CHECK (provider IN ('weborama','adserving','adriver','targetads')),
        verifier_name   TEXT NOT NULL,
        from_email      TEXT NULL,
        mailbox         TEXT NOT NULL DEFAULT 'INBOX',
        active          INTEGER NOT NULL DEFAULT 1,
        created_at      TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );""",
    # groups
    """CREATE TABLE IF NOT EXISTS verifier_group_bindings (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id          INTEGER NOT NULL,
        provider          TEXT NOT NULL CHECK (provider IN ('weborama','adserving','adriver','targetads')),
        subject_pattern   TEXT NOT NULL,
        subject_mode      TEXT NOT NULL DEFAULT 'contains' CHECK (subject_mode IN ('exact','contains','startswith','endswith','regex')),
        filename_pattern  TEXT NULL,
        filename_mode     TEXT NOT NULL DEFAULT 'contains' CHECK (filename_mode IN ('contains','startswith','endswith','regex','exact')),
        from_email        TEXT NULL,
        mailbox           TEXT NOT NULL DEFAULT 'INBOX',
        active            INTEGER NOT NULL DEFAULT 1,
        created_at        TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(group_id, provider)
    );""",
    # import logs
    """CREATE TABLE IF NOT EXISTS verifier_import_files (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id     INTEGER NOT NULL,
        provider        TEXT NOT NULL,
        message_id      TEXT NULL,
        subject         TEXT NOT NULL,
        attachment_name TEXT NOT NULL,
        report_date     TEXT NULL,
        processed_at    TEXT NOT NULL DEFAULT (datetime('now')),
        rows_parsed     INTEGER NOT NULL DEFAULT 0,
        UNIQUE (campaign_id, provider, report_date, attachment_name)
    );""",
    # daily metric
    """CREATE TABLE IF NOT EXISTS verifier_daily_metric (
        campaign_id  INTEGER NOT NULL,
        provider     TEXT    NOT NULL,
        date         TEXT    NOT NULL,
        metric       TEXT    NOT NULL,
        value        REAL    NOT NULL,
        unit         TEXT    NOT NULL,
        source       TEXT    NOT NULL,
        PRIMARY KEY (campaign_id, provider, date, metric)
    );""",
    # baseline stub (для view, опционально)
    """CREATE TABLE IF NOT EXISTS baseline_daily_metric (
        campaign_id  INTEGER NOT NULL,
        date         TEXT    NOT NULL,
        metric       TEXT    NOT NULL,
        value        REAL    NOT NULL,
        unit         TEXT    NOT NULL,
        source       TEXT    NOT NULL,
        PRIMARY KEY (campaign_id, date, metric)
    );""",
    # comparison view (оставим как есть)
    """CREATE VIEW IF NOT EXISTS verifier_vs_baseline AS
    WITH vv AS (
      SELECT campaign_id, provider, date,
             MAX(CASE WHEN metric='impressions'   AND unit='count'   THEN value END) AS v_impr,
             MAX(CASE WHEN metric='clicks'        AND unit='count'   THEN value END) AS v_clk,
             MAX(CASE WHEN metric='givt_impr'     AND unit='count'   THEN value END) AS v_givt,
             MAX(CASE WHEN metric='sivt_impr'     AND unit='count'   THEN value END) AS v_sivt,
             MAX(CASE WHEN metric='viewable_impr' AND unit='count'   THEN value END) AS v_view
      FROM verifier_daily_metric
      GROUP BY campaign_id, provider, date
    ),
    bb AS (
      SELECT campaign_id, date,
             MAX(CASE WHEN metric='impressions' AND unit='count' THEN value END) AS b_impr,
             MAX(CASE WHEN metric='clicks'      AND unit='count' THEN value END) AS b_clk
      FROM baseline_daily_metric
      GROUP BY campaign_id, date
    )
    SELECT vv.campaign_id, vv.provider, vv.date,
           vv.v_impr, vv.v_clk, vv.v_givt, vv.v_sivt, vv.v_view,
           CASE WHEN bb.b_impr IS NULL OR bb.b_impr=0 THEN NULL
                ELSE 100.0 * (bb.b_impr - vv.v_impr) / bb.b_impr END AS impr_delta_pct,
           CASE WHEN bb.b_clk  IS NULL OR bb.b_clk =0 THEN NULL
                ELSE 100.0 * (bb.b_clk  - vv.v_clk)  / bb.b_clk  END AS clk_delta_pct
    FROM vv LEFT JOIN bb USING(campaign_id, date);"""
]

MIGRATIONS = [
    "ALTER TABLE verifier_campaigns ADD COLUMN subject_mode TEXT NOT NULL DEFAULT 'contains' CHECK (subject_mode IN ('exact','contains','startswith','endswith','regex'))",
    "ALTER TABLE verifier_campaigns ADD COLUMN filename_pattern TEXT NULL",
    "ALTER TABLE verifier_campaigns ADD COLUMN filename_mode TEXT NOT NULL DEFAULT 'contains' CHECK (filename_mode IN ('contains','startswith','endswith','regex','exact'))",
]

def ensure_schema():
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
        for alt in MIGRATIONS:
            try:
                conn.execute(text(alt))
            except Exception:
                pass

@router.post("/directory/verifier/update")
def update_verifier(payload: Dict = Body(...)):
    """Upsert verifier settings for a single campaign."""
    try:
        ensure_schema()
        cid = int(payload.get("id"))
        provider = str(payload.get("provider", "")).strip().lower()
        vname = str(payload.get("verifier_name", "")).strip()
        subj_mode = str(payload.get("subject_mode", "contains")).strip().lower()
        fname_pat = (payload.get("filename_pattern") or "").strip()
        fname_mode= str(payload.get("filename_mode", "contains")).strip().lower()
        from_email = str(payload.get("from_email") or "").strip() or None

        if provider not in ALLOWED_PROVIDERS:
            return JSONResponse(status_code=400, content={"error": f"unknown provider '{provider}'"})
        if not vname:
            return JSONResponse(status_code=400, content={"error": "empty verifier_name"})
        if subj_mode not in ALLOWED_MATCH:
            return JSONResponse(status_code=400, content={"error": f"bad subject_mode '{subj_mode}'"})

        sql = text("""
            INSERT INTO verifier_campaigns(campaign_id, provider, verifier_name, subject_mode, filename_pattern, filename_mode, from_email, active)
            VALUES(:cid, :provider, :vname, :subj_mode, :fname_pat, :fname_mode, :from_email, 1)
            ON CONFLICT(campaign_id) DO UPDATE SET
                provider=excluded.provider,
                verifier_name=excluded.verifier_name,
                subject_mode=excluded.subject_mode,
                filename_pattern=excluded.filename_pattern,
                filename_mode=excluded.filename_mode,
                from_email=COALESCE(excluded.from_email, verifier_campaigns.from_email),
                updated_at=datetime('now')
        """)
        with engine.begin() as conn:
            conn.execute(sql, dict(
                cid=cid, provider=provider, vname=vname, subj_mode=subj_mode,
                fname_pat=fname_pat, fname_mode=fname_mode, from_email=from_email
            ))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/directory/verifier/group/update")
def update_verifier_group(payload: Dict = Body(...)):
    """Upsert verifier binding for a group."""
    try:
        ensure_schema()
        gid = int(payload.get("group_id"))
        provider = str(payload.get("provider", "")).strip().lower()
        subj = str(payload.get("subject_pattern", "")).strip()
        subj_mode = str(payload.get("subject_mode", "contains")).strip().lower()
        fname_pat = (payload.get("filename_pattern") or "").strip()
        fname_mode= str(payload.get("filename_mode", "contains")).strip().lower()
        from_email = str(payload.get("from_email") or "").strip() or None
        if provider not in ALLOWED_PROVIDERS:
            return JSONResponse(status_code=400, content={"error": f"unknown provider '{provider}'"})
        if not subj:
            return JSONResponse(status_code=400, content={"error": "empty subject_pattern"})
        if subj_mode not in ALLOWED_MATCH:
            return JSONResponse(status_code=400, content={"error": f"bad subject_mode '{subj_mode}'"})
        sql = text("""
            INSERT INTO verifier_group_bindings(group_id, provider, subject_pattern, subject_mode, filename_pattern, filename_mode, from_email, active)
            VALUES(:gid, :provider, :subj, :subj_mode, :fname_pat, :fname_mode, :from_email, 1)
            ON CONFLICT(group_id, provider) DO UPDATE SET
                subject_pattern=excluded.subject_pattern,
                subject_mode=excluded.subject_mode,
                filename_pattern=excluded.filename_pattern,
                filename_mode=excluded.filename_mode,
                from_email=COALESCE(excluded.from_email, verifier_group_bindings.from_email),
                updated_at=datetime('now'), active=1
        """)
        with engine.begin() as conn:
            conn.execute(sql, dict(
                gid=gid, provider=provider, subj=subj, subj_mode=subj_mode,
                fname_pat=fname_pat, fname_mode=fname_mode, from_email=from_email
            ))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/directory/verifier/import", response_class=HTMLResponse)
async def import_verifier(payload: Dict = Body(None)):
    """Run scripts/verifier_import.py for selected campaigns and/or groups; show N rows added."""
    try:
        ensure_schema()
        ids = []
        group_ids = []
        if payload:
            if isinstance(payload.get("ids"), list):
                ids = [str(int(x)) for x in payload["ids"] if str(x).strip()]
            if isinstance(payload.get("group_ids"), list):
                group_ids = [str(int(x)) for x in payload["group_ids"] if str(x).strip()]

        base_dir = Path(__file__).resolve().parents[2]
        script_path = base_dir / "scripts" / "verifier_import.py"
        if not script_path.exists():
            return HTMLResponse(
                f'<span class="tag is-danger is-light">Script not found</span>'
                f'<pre>{escape(str(script_path))}</pre>'
            )

        def _run():
            args = [sys.executable, "-u", str(script_path), "--db", "campaign_hub.db"]
            if ids:
                args += ["--campaigns", ",".join(ids)]
            if group_ids:
                args += ["--groups", ",".join(group_ids)]
            return subprocess.run(args, cwd=str(base_dir), capture_output=True, text=True, timeout=1200)

        res = await run_in_threadpool(_run)

        rows_added = None
        try:
            m = re.search(r"daily rows imported:\s*(\d+)", (res.stdout or "") + (res.stderr or ""))
            if m:
                rows_added = int(m.group(1))
        except Exception:
            rows_added = None

        if res.returncode == 0:
            if rows_added is not None:
                return HTMLResponse(f'<span class="tag is-success is-light">Verifier import completed · {rows_added} rows added</span>')
            return HTMLResponse('<span class="tag is-success is-light">Verifier import completed</span>')

        tail = (res.stderr or res.stdout or "")[-4000:]
        return HTMLResponse(
            '<span class="tag is-danger is-light">Verifier import failed</span>'
            f'<pre>{escape(tail)}</pre>'
        )
    except Exception as e:
        return HTMLResponse(
            f'<span class="tag is-danger is-light">Error</span> <small>{escape(str(e))}</small>'
        )
