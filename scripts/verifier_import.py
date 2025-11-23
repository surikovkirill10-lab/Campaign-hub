#!/usr/bin/env python3
from __future__ import annotations
import argparse
import sys
from pathlib import Path

# видеть пакет providers рядом со скриптом
sys.path.insert(0, str(Path(__file__).resolve().parent))

import email
import imaplib
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from providers import parse_adserving_xlsx, parse_weborama_xlsx, PROVIDERS_FROM_EMAIL

DEFAULT_DB = "campaign_hub.db"

DDL = [
    # биндинги на отдельные кампании (добавлены новые колонки под режимы матчинга)
    """CREATE TABLE IF NOT EXISTS verifier_campaigns (
        campaign_id     INTEGER PRIMARY KEY,
        provider        TEXT NOT NULL CHECK (provider IN ('weborama','adserving','adriver','targetads')),
        verifier_name   TEXT NOT NULL,                -- теперь это 'subject pattern'
        from_email      TEXT NULL,
        mailbox         TEXT NOT NULL DEFAULT 'INBOX',
        active          INTEGER NOT NULL DEFAULT 1,
        created_at      TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );""",
    # биндинги на группы кампаний
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
    # журнал импортированных файлов
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
    # универсальное хранилище дневных метрик
    """CREATE TABLE IF NOT EXISTS verifier_daily_metric (
        campaign_id  INTEGER NOT NULL,
        provider     TEXT    NOT NULL,
        date         TEXT    NOT NULL,
        metric       TEXT    NOT NULL,
        value        REAL    NOT NULL,
        unit         TEXT    NOT NULL,     -- 'count'|'percent'
        source       TEXT    NOT NULL,
        PRIMARY KEY (campaign_id, provider, date, metric)
    );""",
]

# добавляем новые колонки в legacy-таблицу кампаний, если их нет
MIGRATIONS = [
    "ALTER TABLE verifier_campaigns ADD COLUMN subject_mode TEXT NOT NULL DEFAULT 'contains' CHECK (subject_mode IN ('exact','contains','startswith','endswith','regex'))",
    "ALTER TABLE verifier_campaigns ADD COLUMN filename_pattern TEXT NULL",
    "ALTER TABLE verifier_campaigns ADD COLUMN filename_mode TEXT NOT NULL DEFAULT 'contains' CHECK (filename_mode IN ('contains','startswith','endswith','regex','exact'))",
]

def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    # best-effort ALTERs
    for alt in MIGRATIONS:
        try:
            cur.execute(alt)
        except Exception:
            pass
    conn.commit()

def load_config(path: str = "config.yaml") -> dict:
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def first_day_current_month_msk() -> datetime:
    # UTC-aware (без DeprecationWarning)
    now_utc = datetime.now(timezone.utc)
    msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    first = msk.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return first

def to_imap_since(d: datetime) -> str:
    return d.strftime("%d-%b-%Y")  # 01-Nov-2025

def decode_subj(raw) -> str:
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        try:
            return raw.decode("utf-8", errors="ignore")
        except Exception:
            return str(raw)

def fetch_attachments_from_message(m, allowed_ext=(".xlsx",)):
    attachments = []
    for part in m.walk():
        cd = (part.get("Content-Disposition") or "")
        if part.get_content_maintype() == 'multipart':
            continue
        fname = part.get_filename()
        if not fname:
            # иногда inline без filename — пропустим
            if "attachment" not in cd.lower():
                continue
        else:
            fname_decoded = str(make_header(decode_header(fname)))
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            if not any(fname_decoded.lower().endswith(e) for e in allowed_ext):
                continue
            attachments.append((fname_decoded, payload))
    return attachments

def match_text(text: str, pattern: str, mode: str) -> bool:
    t = (text or "").strip()
    p = (pattern or "").strip()
    m = (mode or "contains").lower()
    if not p:
        return True
    if m == "exact":
        return t == p
    if m == "contains":
        return p.lower() in t.lower()
    if m == "startswith":
        return t.lower().startswith(p.lower())
    if m == "endswith":
        return t.lower().endswith(p.lower())
    if m == "regex":
        try:
            return re.search(p, t, flags=re.IGNORECASE) is not None
        except Exception:
            return False
    return False

def upsert_metrics(conn: sqlite3.Connection, campaign_id: int, provider: str, date: str, metrics: Dict[str, Optional[float]]):
    cur = conn.cursor()
    for k, v in metrics.items():
        if v is None:
            continue
        unit = "percent" if k.endswith("_pct") else "count"
        cur.execute(
            """
            INSERT INTO verifier_daily_metric
              (campaign_id, provider, date, metric, value, unit, source)
            VALUES
              (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(campaign_id, provider, date, metric)
            DO UPDATE SET
              value  = excluded.value,
              unit   = excluded.unit,
              source = excluded.source
            """,
            (campaign_id, provider, date, k, float(v), unit, provider)
        )
    conn.commit()

def record_import_file(conn: sqlite3.Connection, campaign_id: int, provider: str, msg_id: str, subject: str, att_name: str, report_date: str):
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT OR IGNORE INTO verifier_import_files(campaign_id, provider, message_id, subject, attachment_name, report_date, rows_parsed)
               VALUES (?, ?, ?, ?, ?, ?, 1)""",
            (campaign_id, provider, msg_id, subject, att_name, report_date)
        )
        conn.commit()
    except Exception as e:
        # не фейлим импорт, просто журнал не записан
        pass

def parse_and_store(provider: str, content: bytes) -> Dict[str, Dict[str, float]]:
    if provider == "adserving":
        return parse_adserving_xlsx(content)
    elif provider == "weborama":
        return parse_weborama_xlsx(content)
    # остальные добавим позже
    return {}

def get_group_members(conn: sqlite3.Connection, group_id: int) -> List[int]:
    cur = conn.cursor()
    rows = cur.execute("SELECT campaign_id FROM campaign_group_members WHERE group_id=?", (group_id,)).fetchall()
    return [int(r[0]) for r in rows]

def main():
    ap = argparse.ArgumentParser(description="Import verifier reports from IMAP and store into campaign_hub.db")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite database path (default: campaign_hub.db)")
    ap.add_argument("--campaigns", default="", help="Comma-separated campaign IDs to import (default: all active campaign bindings)")
    ap.add_argument("--groups", default="", help="Comma-separated group IDs to import (default: all active group bindings)")
    ap.add_argument("--since", default="", help="Since date (YYYY-MM-DD). Default: 1st day of current month (MSK).")
    args = ap.parse_args()

    db_path = args.db
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    # campaign bindings
    cur = conn.cursor()
    if args.campaigns.strip():
        ids = [int(x.strip()) for x in args.campaigns.split(",") if x.strip()]
        q = """
            SELECT campaign_id, provider, verifier_name, subject_mode, COALESCE(filename_pattern,''), filename_mode, COALESCE(from_email,'')
            FROM verifier_campaigns
            WHERE active=1 AND campaign_id IN ({})
        """.format(",".join(["?"]*len(ids)))
        cur.execute(q, ids)
    else:
        cur.execute("""
            SELECT campaign_id, provider, verifier_name, subject_mode, COALESCE(filename_pattern,''), filename_mode, COALESCE(from_email,'')
            FROM verifier_campaigns
            WHERE active=1
        """)
    camp_bindings = cur.fetchall()

    # group bindings
    if args.groups.strip():
        gids = [int(x.strip()) for x in args.groups.split(",") if x.strip()]
        qg = """
            SELECT id, group_id, provider, subject_pattern, subject_mode, COALESCE(filename_pattern,''), filename_mode, COALESCE(from_email,'')
            FROM verifier_group_bindings
            WHERE active=1 AND group_id IN ({})
        """.format(",".join(["?"]*len(gids)))
        cur.execute(qg, gids)
    else:
        cur.execute("""
            SELECT id, group_id, provider, subject_pattern, subject_mode, COALESCE(filename_pattern,''), filename_mode, COALESCE(from_email,'')
            FROM verifier_group_bindings
            WHERE active=1
        """)
    group_bindings = cur.fetchall()

    cfg = load_config("config.yaml")
    imap_host = cfg.get("imap", {}).get("host")
    imap_user = cfg.get("imap", {}).get("user")
    imap_pass = cfg.get("imap", {}).get("password")
    use_ssl = bool(cfg.get("imap", {}).get("ssl", True))

    if not imap_host or not imap_user or not imap_pass:
        print("[ERROR] IMAP credentials are missing in config.yaml under 'imap'", file=sys.stderr)
        return 2

    since_dt = None
    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d")
    else:
        since_dt = first_day_current_month_msk()
    since_str = to_imap_since(since_dt)

    print(f"[INFO] Connecting IMAP {imap_host} as {imap_user}")
    M = imaplib.IMAP4_SSL(imap_host) if use_ssl else imaplib.IMAP4(imap_host)
    M.login(imap_user, imap_pass)

    SELECTED = 0
    IMPORTED = 0

    try:
        # helper to run a single search/import pass
        def run_pass(scope: str,
                     provider: str,
                     subj_pat: str,
                     subj_mode: str,
                     fname_pat: str,
                     fname_mode: str,
                     from_email: str,
                     apply_to_campaign_ids: List[int]):

            nonlocal SELECTED, IMPORTED

            from_addr = (from_email or PROVIDERS_FROM_EMAIL.get(provider))
            if not from_addr:
                print(f"[WARN] {scope}: provider {provider} has no from_email; skipped", file=sys.stderr)
                return

            mailbox = 'INBOX'
            M.select(mailbox)

            # Для перфоманса: если subj_mode != regex и subj_pat не пуст — ограничим IMAP по SUBJECT (contains)
            if subj_mode.lower() != "regex" and subj_pat and subj_pat.strip():
                typ, data = M.search(None, 'FROM', f'"{from_addr}"', 'SINCE', since_str, 'SUBJECT', f'"{subj_pat}"')
            else:
                typ, data = M.search(None, 'FROM', f'"{from_addr}"', 'SINCE', since_str)

            if typ != 'OK':
                print(f"[WARN] {scope}: IMAP search failed", file=sys.stderr)
                return

            msg_ids = data[0].split()
            for num in msg_ids:
                typ, msg_data = M.fetch(num, '(RFC822)')
                if typ != 'OK':
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
                subj = decode_subj(msg.get('Subject', ''))
                from_ = email.utils.parseaddr(msg.get('From', ''))[1].lower()
                if from_ != from_addr.lower():
                    continue
                # python-уровень: subject match по режиму
                if not match_text(subj, subj_pat, subj_mode):
                    continue

                atts = fetch_attachments_from_message(msg, allowed_ext=(".xlsx",))
                if not atts:
                    continue

                for att_name, payload in atts:
                    # фильтрация по имени файла, если задана
                    if fname_pat and not match_text(att_name, fname_pat, fname_mode):
                        continue

                    metrics_by_date = parse_and_store(provider, payload)
                    if not metrics_by_date:
                        continue

                    SELECTED += 1
                    for d, m in metrics_by_date.items():
                        for campaign_id in apply_to_campaign_ids:
                            upsert_metrics(conn, campaign_id, provider, d, m)
                            record_import_file(conn, campaign_id, provider, msg.get('Message-Id', ''), subj, att_name, d)
                            IMPORTED += 1

        # 1) покампанийные биндинги
        for campaign_id, provider, subj_pat, subj_mode, fname_pat, fname_mode, from_email in camp_bindings:
            run_pass(scope=f"campaign:{campaign_id}",
                     provider=provider,
                     subj_pat=subj_pat,
                     subj_mode=subj_mode,
                     fname_pat=fname_pat,
                     fname_mode=fname_mode,
                     from_email=from_email,
                     apply_to_campaign_ids=[int(campaign_id)])

        # 2) групповые биндинги
        for _id, group_id, provider, subj_pat, subj_mode, fname_pat, fname_mode, from_email in group_bindings:
            members = get_group_members(conn, int(group_id))
            if not members:
                continue
            run_pass(scope=f"group:{group_id}",
                     provider=provider,
                     subj_pat=subj_pat,
                     subj_mode=subj_mode,
                     fname_pat=fname_pat,
                     fname_mode=fname_mode,
                     from_email=from_email,
                     apply_to_campaign_ids=members)

        print(f"[DONE] messages matched: {SELECTED}, daily rows imported: {IMPORTED}")
        return 0
    finally:
        try:
            M.logout()
        except Exception:
            pass

if __name__ == "__main__":
    sys.exit(main())
