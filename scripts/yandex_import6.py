# -*- coding: utf-8 -*-
"""
yandex_import6.py
-----------------
Практичное решение проблемы поиска писем с кириллицей: серверный поиск только по ASCII
(FROM + SINCE), а фильтрация по теме (кириллица) — в Python.

Основано на вашем yandex_import3.py, но с безопасным поиском.
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
import pandas as pd
from email.header import decode_header, make_header


# От кого приходят отчёты
FROM_ADDR = "devnull@yandex.ru"

# Пример темы: Отчёт «Название кампании» за 28.10.2025
SUBJ_RE = re.compile(
    r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})',
    re.IGNORECASE
)


def dec(s: str) -> str:
    """Декодируем header в читаемую строку."""
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s


def ru_date_to_date(s: str) -> datetime.date:
    """Парсим дату из формата DD.MM.YYYY в date."""
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()


def parse_report_date_from_header(v) -> datetime.date | None:
    """Пробуем вынуть дату отчёта из первой ячейки xlsx (формат: 'с YYYY-MM-DD по YYYY-MM-DD')."""
    m = re.search(r'с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})', str(v) if v else "")
    return datetime.datetime.strptime(m.group(2), "%Y-%m-%d").date() if m else None


def parse_xlsx(b: bytes) -> tuple[datetime.date | None, dict | None]:
    """Извлекаем агрегаты из xlsx: визиты, посетители, отказы, глубина, среднее время (сек)."""
    df = pd.read_excel(io.BytesIO(b))
    if len(df) < 6:
        return None, None

    header = df.iloc[3].tolist()
    rows = df.iloc[5:].copy()
    rows.columns = header

    d = None
    try:
        import openpyxl  # необязательная зависимость, только для аккуратного чтения заголовка
        wb = openpyxl.load_workbook(io.BytesIO(b), data_only=True)
        d = parse_report_date_from_header(wb.active.cell(1, 1).value)
    except Exception:
        pass

    if rows.empty:
        return d, None

    # Безопасные числовые преобразования
    rows['Визиты'] = pd.to_numeric(rows.get('Визиты'), errors='coerce').fillna(0.0)
    rows['Посетители'] = pd.to_numeric(rows.get('Посетители'), errors='coerce').fillna(0.0)
    rows['Отказы'] = pd.to_numeric(rows.get('Отказы'), errors='coerce').fillna(0.0)
    rows['Глубина просмотра'] = pd.to_numeric(rows.get('Глубина просмотра'), errors='coerce').fillna(0.0)

    visits = float(rows['Визиты'].sum())
    if visits <= 0:
        return d, None

    visitors = float(rows['Посетители'].sum())
    bounce = float((rows['Отказы'] * rows['Визиты']).sum() / visits)
    depth = float((rows['Глубина просмотра'] * rows['Визиты']).sum() / visits)

    def t2s(v) -> float:
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

    # В ряде выгрузок колонка может называться 'Время на сайте'.
    # Если её нет — пытаемся найти похожую по названию.
    time_col = None
    for cand in ['Время на сайте', 'Среднее время на сайте']:
        if cand in rows.columns:
            time_col = cand
            break
    if time_col is None:
        # Не нашли колонку времени — считаем, что 0
        avg = 0.0
    else:
        avg = float((rows[time_col].apply(t2s) * rows['Визиты']).sum() / visits)

    return d, dict(
        visits=visits,
        visitors=visitors,
        bounce_rate=bounce,
        page_depth=depth,
        avg_time_sec=avg
    )


def imap_since_date(d: datetime.date) -> str:
    """Всегда английские месяцы для IMAP (DD-Mon-YYYY), без влияния локали."""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{d.day:02d}-{months[d.month - 1]}-{d.year}"


# ---------- Конфигурация ----------

with open("config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

imap_cfg = cfg["imap"]
camps = cfg.get("yandex_campaigns") or []

# База рядом с папкой scripts (как в исходнике)
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

# ---------- IMAP ----------

M = imaplib.IMAP4_SSL(imap_cfg.get("host", "imap.yandex.com"), int(imap_cfg.get("port", 993)))
M.login(imap_cfg["user"], imap_cfg["password"])

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

    # ---- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: поиск ТОЛЬКО по ASCII ----
    # Ищем письма только по отправителю и дате (последние 35 дней).
    since_date = datetime.date.today() - datetime.timedelta(days=35)
    since_imap = imap_since_date(since_date)

    try:
        typ, data = M.search(None, 'FROM', FROM_ADDR, 'SINCE', since_imap)
    except imaplib.IMAP4.error as e:
        print(f"[{yname}] SEARCH error: {e!s}")
        continue

    uids = data[0].split() if (typ == 'OK' and data and data[0]) else []
    print(f"[{yname}] matched by FROM+SINCE: {len(uids)} in {mbox}")

    for uid in uids:
        t, md = M.fetch(uid, '(RFC822)')
        if t != 'OK' or not md or not md[0]:
            continue

        msg = email.message_from_bytes(md[0][1])
        subj = dec(msg.get('Subject', ''))
        m = SUBJ_RE.search(subj)
        if not m:
            # Тема не соответствует шаблону отчёта
            continue

        name = m.group(1).strip()
        # отсекаем «Соцдем_…» и т.п.: должно совпасть ровно с именем кампании
        if name.lower() != yname.lower():
            continue

        rdate_subj = ru_date_to_date(m.group(2))
        msgs_total += 1

        mid = msg.get('Message-ID')
        if mid and cur.execute(
            "SELECT 1 FROM yandex_import_files WHERE message_id=?",
            (mid,)
        ).fetchone():
            # Уже видели это письмо
            continue

        # Собираем xlsx-вложения
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
            # Логируем факт письма без вложений
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

        # Берём «таблиц*» если есть, иначе самое большое вложение
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
