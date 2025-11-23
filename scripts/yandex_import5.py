import os
import sys
import imaplib
import email
import io
import yaml
import sqlite3
import datetime
import re
import pandas as pd
from email.header import decode_header, make_header

FROM_ADDR = "devnull@yandex.ru"
SUBJ_RE = re.compile(
    r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})',
    re.IGNORECASE
)


def dec(s):
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s


def ru_date_to_date(s):
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()


def parse_report_date_from_header(v):
    m = re.search(r'с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})', str(v) if v else "")
    return datetime.datetime.strptime(m.group(2), "%Y-%m-%d").date() if m else None


def parse_xlsx(b):
    df = pd.read_excel(io.BytesIO(b))
    if len(df) < 6:
        return None, None

    header = df.iloc[3].tolist()
    rows = df.iloc[5:].copy()
    rows.columns = header

    d = None
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(b), data_only=True)
        d = parse_report_date_from_header(wb.active.cell(1, 1).value)
    except Exception:
        pass

    if rows.empty:
        return d, None

    rows['Визиты'] = pd.to_numeric(rows['Визиты'], errors='coerce').fillna(0.0)
    rows['Посетители'] = pd.to_numeric(rows['Посетители'], errors='coerce').fillna(0.0)
    rows['Отказы'] = pd.to_numeric(rows['Отказы'], errors='coerce').fillna(0.0)
    rows['Глубина просмотра'] = pd.to_numeric(rows['Глубина просмотра'], errors='coerce').fillna(0.0)

    visits = float(rows['Визиты'].sum())
    if visits <= 0:
        return d, None

    visitors = float(rows['Посетители'].sum())
    bounce = float((rows['Отказы'] * rows['Визиты']).sum() / visits)
    depth = float((rows['Глубина просмотра'] * rows['Визиты']).sum() / visits)

    def t2s(v):
        s = str(v)
        if ":" in s:
            try:
                h, m, s0 = [int(x) for x in s.split(":")]
                return float(h * 3600 + m * 60 + s0)
            except Exception:
                return 0.0
        try:
            return float(v)
        except Exception:
            return 0.0

    avg = float((rows['Время на сайте'].apply(t2s) * rows['Визиты']).sum() / visits)

    return d, dict(
        visits=visits,
        visitors=visitors,
        bounce_rate=bounce,
        page_depth=depth,
        avg_time_sec=avg
    )


with open("config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

imap = cfg["imap"]
camps = cfg.get("yandex_campaigns") or []

db = os.path.abspath(os.path.join("scripts", "..", "yandex_metrics.db"))
con = sqlite3.connect(db)
cur = con.cursor()

cur.execute(
    """CREATE TABLE IF NOT EXISTS yandex_daily_metrics(
       campaign_id INTEGER,
       report_date TEXT,
       visits REAL,
       visitors REAL,
       bounce_rate REAL,
       page_depth REAL,
       avg_time_sec REAL,
       PRIMARY KEY(campaign_id, report_date)
     );"""
)
cur.execute(
    """CREATE TABLE IF NOT EXISTS yandex_import_files(
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       campaign_id INTEGER,
       message_id TEXT,
       subject TEXT,
       attachment_name TEXT,
       report_date TEXT,
       processed_at TEXT,
       UNIQUE(message_id, attachment_name)
     );"""
)
con.commit()

M = imaplib.IMAP4_SSL(imap.get("host", "imap.yandex.com"), int(imap.get("port", 993)))
M.login(imap["user"], imap["password"])

rows_total = 0
files_total = 0
msgs_total = 0

for c in camps:
    yname = str(c["yandex_name"]).strip()
    cid = int(c["id"])
    mbox = str(c.get("mailbox", "INBOX")).strip() or "INBOX"

    t, _ = M.select(mbox, readonly=True)
    if t != "OK":
        print(f"[{yname}] mailbox FAIL:", mbox)
        continue

    safe_yname = str(yname).encode('utf-8')

    search_criteria = f'(FROM "{FROM_ADDR}" SUBJECT "{safe_yname}")'

    # Используем правильный формат вызова
    typ, data = M.search(
        'UTF-8',  # кодировка
        search_criteria.encode('utf-8')  # кодируем всю строку поиска
    )

    uids = data[0].split() if (typ == 'OK' and data and data[0]) else []
    print(f"[{yname}] matched: {len(uids)} in {mbox}")

    for uid in uids:
        t, md = M.fetch(uid, '(RFC822)')
        if t != 'OK' or not md or not md[0]:
            continue

        msg = email.message_from_bytes(md[0][1])
        subj = dec(msg.get('Subject', ''))
        m = SUBJ_RE.search(subj)
        if not m:
            continue

        name = m.group(1).strip()
        # отсекаем «Соцдем_…»
        if name.lower() != yname.lower():
            continue

        rdate_subj = ru_date_to_date(m.group(2))
        msgs_total += 1

        mid = msg.get('Message-ID')
        if mid and cur.execute(
            "SELECT 1 FROM yandex_import_files WHERE message_id=?",
            (mid,)
        ).fetchone():
            continue

        xlsx = []
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue

            fn_raw = part.get_filename()
            if not fn_raw:
                continue

            try:
                fn = str(make_header(decode_header(fn_raw)))
            except Exception:
                fn = fn_raw

            if not fn.lower().endswith(".xlsx"):
                continue

            blob = part.get_payload(decode=True) or b""
            xlsx.append((fn, len(blob), blob))

        if not xlsx:
            cur.execute(
                """INSERT OR IGNORE INTO yandex_import_files
                   (campaign_id, message_id, subject, attachment_name, report_date, processed_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    cid,
                    mid,
                    subj,
                    None,
                    None,
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                )
            )
            con.commit()
            continue

        xlsx.sort(key=lambda x: x[1], reverse=True)

        chosen = None
        for fn, sz, bl in xlsx:
            if re.search(r'таблиц', fn, flags=re.IGNORECASE):
                chosen = (fn, sz, bl)
                break
        if not chosen:
            chosen = xlsx[0]

        fn, sz, blob = chosen
        d_file, metrics = parse_xlsx(blob)
        rdate = d_file or rdate_subj

        if metrics and rdate:
            cur.execute(
                """INSERT OR REPLACE INTO yandex_daily_metrics
                   (campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    cid,
                    str(rdate),
                    metrics['visits'],
                    metrics['visitors'],
                    metrics['bounce_rate'],
                    metrics['page_depth'],
                    metrics['avg_time_sec'],
                )
            )
            rows_total += 1

        cur.execute(
            """INSERT OR IGNORE INTO yandex_import_files
               (campaign_id, message_id, subject, attachment_name, report_date, processed_at)
               VALUES (?,?,?,?,?,?)""",
            (
                cid,
                mid,
                subj,
                fn,
                str(rdate) if rdate else None,
                datetime.datetime.now(datetime.timezone.utc).isoformat(),
            )
        )
        con.commit()
        files_total += 1

M.logout()
print(f"SUMMARY: msgs={msgs_total}, files={files_total}, rows={rows_total}, db={db}")
con.close()
