"""
This script connects to a Yandex mail account, searches for campaign report emails and
imports their attached XLSX metrics into a SQLite database.  It has been
restructured to work more reliably and efficiently with Cyrillic subjects and
large mailboxes.

Key improvements over the original version:

 * When supported by the server, the IMAP UTF8 extension is enabled so that
   Cyrillic search criteria are honoured on the server side.
 * Searches are based on message age rather than SUBJECT/FROM filters, to
   avoid issues with encoded subjects; filtering by subject and campaign
   name happens locally after headers are downloaded.
 * Only a limited number of the most recent UIDs are considered (controlled
   via the YANDEX_UID_CAP environment variable) to avoid scanning the entire
   mailbox on every run.
 * Message headers are fetched separately before downloading full messages;
   this minimises network traffic and avoids downloading large bodies unless
   a candidate message matches the campaign criteria.

Usage remains the same: configure connection details and campaigns in
`config.yaml`.  Running the script will update `yandex_metrics.db` with
daily metrics and record processed messages in `yandex_import_files`.
"""

import os
import sys
import imaplib
import email
import io
import yaml
import sqlite3
import datetime
import re
import unicodedata
from typing import Optional, Dict, Tuple, List
import pandas as pd
from email.header import decode_header, make_header

# Address expected to send the reports.  This remains for informational
# purposes but is no longer used in the search criteria.  Filtering by
# subject/name happens locally instead.
FROM_ADDR: str = "devnull@yandex.ru"

# Regular expression to extract campaign name and report date from the subject.
SUBJ_RE: re.Pattern = re.compile(
    r"Отч[её]т\s+[«\"“](.*?)[»\"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})",
    re.IGNORECASE,
)

def dec(s: Optional[str]) -> str:
    """Decode an RFC2047-encoded header or return the original string.

    Args:
        s: The header value (possibly encoded).

    Returns:
        The decoded string, with any errors silently ignored.
    """
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        # If decode_header fails, return the original string as a best effort.
        return s


def norm(x: str) -> str:
    """Normalize a string for case-insensitive comparison.

    The normalization uses NFKC to unify compatibility characters, replaces
    Russian "ё" with "е", strips typographic quotes and non-breaking spaces,
    lowers case and trims whitespace.  This helps match campaign names that
    vary slightly between the config and the e‑mail subject.

    Args:
        x: The input string.

    Returns:
        A normalized, lower‑cased string suitable for comparison.
    """
    if not x:
        return ""
    s = str(x)
    # Normalise compatibility characters (e.g. full width forms, etc.)
    s = unicodedata.normalize("NFKC", s)
    # Replace non‑breaking spaces with regular spaces
    s = s.replace("\xa0", " ")
    # Replace typographic quotes with straight quotes
    for old, new in (
        ("«", '"'),
        ("»", '"'),
        ("“", '"'),
        ("”", '"'),
        ("‘", "'"),
        ("’", "'"),
        ("„", '"'),
    ):
        s = s.replace(old, new)
    # Replace Russian "ё"/"Ё" with "е"/"Е"
    s = s.replace("ё", "е").replace("Ё", "Е")
    return s.casefold().strip()


def ru_date_to_date(s: str) -> datetime.date:
    """Convert a date string in DD.MM.YYYY format to a date object."""
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()


def parse_report_date_from_header(v: Optional[str]) -> Optional[datetime.date]:
    """Extract the end date of the reporting period from an XLSX header string.

    The expected format is "с YYYY-MM-DD по YYYY-MM-DD".  The function
    returns the second date if present.

    Args:
        v: The cell value from the first cell of the XLSX sheet.

    Returns:
        A date if the pattern is found, otherwise None.
    """
    if not v:
        return None
    m = re.search(r"с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})", str(v))
    if m:
        try:
            return datetime.datetime.strptime(m.group(2), "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def parse_xlsx(b: bytes) -> Tuple[Optional[datetime.date], Optional[Dict[str, float]]]:
    """Parse a Yandex.Metrica report XLSX and aggregate metrics.

    The function extracts the header and data rows, reads the reporting date
    either from the XLSX metadata (first cell) or later falls back to the
    date parsed from the subject.  Metrics are converted to numeric types
    and aggregated: visits and visitors are summed, while bounce rate,
    page depth and average time on site are computed as weighted averages
    by visits.

    Args:
        b: Bytes of the XLSX file.

    Returns:
        A tuple of (report_date, metrics_dict).  If there are no rows or
        visits, metrics_dict will be None.  report_date may be None if
        it cannot be extracted.
    """
    # Read the file into a DataFrame.  If it doesn't look like a standard
    # report (too few rows), bail early.
    df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    if len(df) < 6:
        return None, None
    # In typical Yandex.Metrica reports, row 4 (0‑indexed 3) contains
    # column names and data starts from row 6 (0‑indexed 5).
    header = df.iloc[3].tolist()
    rows = df.iloc[5:].copy()
    rows.columns = header
    report_date_file: Optional[datetime.date] = None
    # Attempt to extract report date from the first cell of the sheet.
    try:
        import openpyxl  # type: ignore
        wb = openpyxl.load_workbook(io.BytesIO(b), data_only=True)
        report_date_file = parse_report_date_from_header(wb.active.cell(1, 1).value)
    except Exception:
        report_date_file = None
    if rows.empty:
        return report_date_file, None
    # Convert textual metrics to numeric values, coercing errors to NaN then
    # filling with zero.  These columns must exist in the report; if they do
    # not, a KeyError will propagate up and cause the caller to ignore the
    # message.
    for col in ["Визиты", "Посетители", "Отказы", "Глубина просмотра"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)
    # Compute total visits; if there are no visits, there's no point in
    # computing averages.
    total_visits = float(rows["Визиты"].sum())
    if total_visits <= 0:
        return report_date_file, None
    # Total visitors
    total_visitors = float(rows["Посетители"].sum())
    # Weighted averages for bounce rate and page depth
    bounce_rate = float((rows["Отказы"] * rows["Визиты"]).sum() / total_visits)
    page_depth = float((rows["Глубина просмотра"] * rows["Визиты"]).sum() / total_visits)
    # Convert time strings to seconds.  Some reports use HH:MM:SS, others
    # may be numeric already.
    def t2s(v: object) -> float:
        s = str(v)
        if ":" in s:
            try:
                parts = [int(x) for x in s.split(":")]
                # Support both HH:MM and HH:MM:SS formats
                if len(parts) == 2:
                    h, m = parts
                    sec = 0
                else:
                    h, m, sec = parts
                return float(h * 3600 + m * 60 + sec)
            except Exception:
                return 0.0
        try:
            return float(v)
        except Exception:
            return 0.0
    avg_time_seconds = float(
        (rows["Визиты"] * rows["Время на сайте"].apply(t2s)).sum() / total_visits
    )
    metrics = {
        "visits": total_visits,
        "visitors": total_visitors,
        "bounce_rate": bounce_rate,
        "page_depth": page_depth,
        "avg_time_sec": avg_time_seconds,
    }
    return report_date_file, metrics

# Read configuration and initialise the database
cfg = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
imap_cfg = cfg.get("imap", {})
campaigns = cfg.get("yandex_campaigns") or []

# Database location relative to the project root (scripts/../yandex_metrics.db).
db_path = os.path.abspath(os.path.join("scripts", "..", "yandex_metrics.db"))
con = sqlite3.connect(db_path)
cur = con.cursor()

# Ensure required tables exist
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS yandex_daily_metrics(
      campaign_id INTEGER,
      report_date TEXT,
      visits REAL,
      visitors REAL,
      bounce_rate REAL,
      page_depth REAL,
      avg_time_sec REAL,
      PRIMARY KEY(campaign_id, report_date)
    );
    """
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS yandex_import_files(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      campaign_id INTEGER,
      message_id TEXT,
      subject TEXT,
      attachment_name TEXT,
      report_date TEXT,
      processed_at TEXT,
      UNIQUE(message_id, attachment_name)
    );
    """
)
con.commit()


def imap_date(dt: datetime.date) -> str:
    """Return a date string in IMAP's expected format (DD-Mon-YYYY).

    The month must be the English abbreviation regardless of locale.  We use
    a manual mapping instead of strftime %b to avoid locale issues.
    """
    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    return f"{dt.day:02d}-{months[dt.month - 1]}-{dt.year}"


def search_mailbox(
    M: imaplib.IMAP4_SSL, mbox: str, since_date: datetime.date, uid_cap: int
) -> List[bytes]:
    """Search the given mailbox and return a limited list of UIDs.

    The search is performed using the SINCE criterion only; further
    filtering happens locally.  The list is truncated to the most recent
    `uid_cap` results to avoid scanning the entire mailbox.

    Args:
        M: An authenticated IMAP connection.
        mbox: Mailbox name to search.
        since_date: Date from which to search (inclusive).
        uid_cap: Maximum number of UIDs to return.

    Returns:
        A list of UID bytes sorted from oldest to newest.
    """
    # Select the mailbox in readonly mode to avoid marking messages as read.
    status, _ = M.select(mbox, readonly=True)
    if status != "OK":
        return []
    # Try to perform a server-side search.  If the server supports UTF8
    # searches, we could include SUBJECT filters here, but to be robust
    # against encoding mismatches, we only search by date and do the rest
    # locally.  Format the date in English month names.
    try:
        search_criteria = ["SINCE", imap_date(since_date)]
        status, data = M.search(None, *search_criteria)
    except Exception:
        # Fallback: no results
        return []
    if status != "OK" or not data or not data[0]:
        return []
    uids = data[0].split()
    if not uids:
        return []
    # Limit to the last `uid_cap` UIDs
    if uid_cap > 0 and len(uids) > uid_cap:
        uids = uids[-uid_cap:]
    # Return ascending order so older messages are processed first
    return uids


def fetch_headers(
    M: imaplib.IMAP4_SSL, uid: bytes
) -> Optional[email.message.Message]:
    """Fetch the headers of a message by UID.

    Args:
        M: An authenticated IMAP connection.
        uid: UID of the message to fetch.

    Returns:
        An email.message.Message with the header fields, or None on error.
    """
    try:
        typ, md = M.fetch(uid, "(BODY.PEEK[HEADER.FIELDS (SUBJECT MESSAGE-ID DATE FROM)])")
        if typ != "OK" or not md or not md[0]:
            return None
        return email.message_from_bytes(md[0][1])
    except Exception:
        return None


def fetch_full_message(
    M: imaplib.IMAP4_SSL, uid: bytes
) -> Optional[email.message.Message]:
    """Fetch the full RFC822 of a message by UID.

    Args:
        M: An authenticated IMAP connection.
        uid: UID of the message to fetch.

    Returns:
        An email.message.Message, or None on error.
    """
    try:
        typ, md = M.fetch(uid, "(RFC822)")
        if typ != "OK" or not md or not md[0]:
            return None
        return email.message_from_bytes(md[0][1])
    except Exception:
        return None


def main() -> None:
    # Connect to IMAP server
    host = imap_cfg.get("host", "imap.yandex.com")
    port = int(imap_cfg.get("port", 993))
    user = imap_cfg.get("user")
    password = imap_cfg.get("password")
    if not user or not password:
        print("IMAP credentials are missing in config.yaml")
        return
    M = imaplib.IMAP4_SSL(host, port)
    M.login(user, password)
    # Attempt to enable UTF8=ACCEPT if the server supports it
    try:
        typ, caps = M.capability()
        if b"UTF8=ACCEPT" in b" ".join(caps):
            try:
                M.enable("UTF8=ACCEPT")
            except Exception:
                pass
    except Exception:
        pass
    # If the server supports COMPRESS=DEFLATE, enabling it could save
    # bandwidth.  imaplib does not provide a high‑level API, so we skip it
    # here.

    rows_total = 0
    files_total = 0
    msgs_total = 0

    # Determine the UID cap from the environment, defaulting to 2000
    uid_cap_env = os.environ.get("YANDEX_UID_CAP", "2000")
    try:
        uid_cap = int(uid_cap_env)
    except ValueError:
        uid_cap = 2000

    # Iterate over configured campaigns
    for campaign in campaigns:
        yname = str(campaign.get("yandex_name", "")).strip()
        cid = int(campaign.get("id"))
        mbox = str(campaign.get("mailbox", "INBOX")).strip() or "INBOX"
        # Compute since_date: if we already have entries for this campaign,
        # look back 30 days from the last imported report; otherwise, look
        # back 45 days from today.  This helps pick up late reports without
        # scanning the entire mailbox.
        last_date_row = cur.execute(
            "SELECT MAX(report_date) FROM yandex_daily_metrics WHERE campaign_id=?",
            (cid,),
        ).fetchone()
        since_date = None
        if last_date_row and last_date_row[0]:
            try:
                last_date = datetime.date.fromisoformat(last_date_row[0])
                since_date = last_date - datetime.timedelta(days=30)
            except Exception:
                since_date = None
        if not since_date:
            since_date = datetime.date.today() - datetime.timedelta(days=45)
        # Perform the search and get a limited set of UIDs
        uids = search_mailbox(M, mbox, since_date, uid_cap)
        print(f"[{yname}] candidates: {len(uids)} in {mbox}")
        # Normalised campaign name for comparison
        yname_norm = norm(yname)
        for uid in uids:
            # Fetch headers only
            hdr = fetch_headers(M, uid)
            if not hdr:
                continue
            subj = dec(hdr.get("Subject", ""))
            match = SUBJ_RE.search(subj)
            if not match:
                continue
            extracted_name = match.group(1).strip()
            # Compare campaign names using normalisation
            if norm(extracted_name) != yname_norm:
                continue
            # Parse report date from subject (DD.MM.YYYY)
            try:
                rdate_subj = ru_date_to_date(match.group(2))
            except Exception:
                rdate_subj = None
            # Count message matches
            msgs_total += 1
            # Check deduplication: skip if message already processed
            mid = hdr.get("Message-ID", "")
            if mid:
                exists = cur.execute(
                    "SELECT 1 FROM yandex_import_files WHERE message_id=?",
                    (mid,),
                ).fetchone()
                if exists:
                    continue
            # Fetch the full message only for candidates
            msg = fetch_full_message(M, uid)
            if not msg:
                continue
            # Decode the full subject again for storing
            full_subj = dec(msg.get("Subject", ""))
            # Collect XLSX attachments
            xlsx_parts: List[Tuple[str, int, bytes]] = []
            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                fn_raw = part.get_filename()
                if not fn_raw:
                    continue
                try:
                    fn = str(make_header(decode_header(fn_raw)))
                except Exception:
                    fn = fn_raw if isinstance(fn_raw, str) else str(fn_raw)
                if not fn.lower().endswith(".xlsx"):
                    continue
                blob = part.get_payload(decode=True) or b""
                xlsx_parts.append((fn, len(blob), blob))
            # If no XLSX attachments, record that the message was processed
            if not xlsx_parts:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO yandex_import_files
                      (campaign_id, message_id, subject, attachment_name, report_date, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cid,
                        mid,
                        full_subj,
                        None,
                        None,
                        datetime.datetime.utcnow().isoformat(),
                    ),
                )
                con.commit()
                continue
            # Prefer the attachment whose name contains 'таблиц'; otherwise
            # choose the largest by size.
            chosen = None
            for fn, sz, bl in xlsx_parts:
                if re.search(r"таблиц", fn, flags=re.IGNORECASE):
                    chosen = (fn, sz, bl)
                    break
            if not chosen:
                # Fall back to the largest by size
                chosen = max(xlsx_parts, key=lambda x: x[1])
            fn, sz, blob = chosen
            # Parse the XLSX and get metrics/date
            d_file, metrics = parse_xlsx(blob)
            report_date = d_file or rdate_subj
            # Insert metrics if available
            if metrics and report_date:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO yandex_daily_metrics
                      (campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cid,
                        str(report_date),
                        metrics["visits"],
                        metrics["visitors"],
                        metrics["bounce_rate"],
                        metrics["page_depth"],
                        metrics["avg_time_sec"],
                    ),
                )
                rows_total += 1
            # Record the processed file
            cur.execute(
                """
                INSERT OR IGNORE INTO yandex_import_files
                  (campaign_id, message_id, subject, attachment_name, report_date, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    cid,
                    mid,
                    full_subj,
                    fn,
                    str(report_date) if report_date else None,
                    datetime.datetime.utcnow().isoformat(),
                ),
            )
            con.commit()
            files_total += 1
    M.logout()
    print(
        f"SUMMARY: msgs={msgs_total}, files={files_total}, rows={rows_total}, db={db_path}"
    )
    con.close()


if __name__ == "__main__":
    main()
