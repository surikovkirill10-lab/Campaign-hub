# -*- coding: utf-8 -*-
"""
yandex_import6_fast.py
----------------------
«Первый вариант» (максимально быстрый и жизнеспособный):
- На сервере ищем ТОЛЬКО по ASCII (FROM + SINCE), один раз на ящик, а не на каждую кампанию.
- По теме письма (кириллица) фильтруем локально в Python.
- Для ускорения сначала тянем только заголовки (Subject, Message-ID), полный RFC822 забираем
  только для кандидатов, чья тема совпала по шаблону.

Основано на вашем yandex_import3.py.
"""

import os
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


def dec_header(s: str) -> str:
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
    def as_num(series_name):
        return pd.to_numeric(rows.get(series_name), errors='coerce').fillna(0.0)

    rows['Визиты'] = as_num('Визиты')
    rows['Посетители'] = as_num('Посетители')
    rows['Отказы'] = as_num('Отказы')
    rows['Глубина просмотра'] = as_num('Глубина просмотра')

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

    # Возможные названия колонки времени
    time_col = None
    for cand in ['Время на сайте', 'Среднее время на сайте']:
        if cand in rows.columns:
            time_col = cand
            break
    if time_col is None:
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
all_camps = cfg.get("yandex_campaigns") or []

# Сгруппируем кампании по ящикам (чтобы не делать SEARCH для каждого yandex_name)
by_mailbox: dict[str, list[dict]] = {}
for c in all_camps:
    mbox = str(c.get("mailbox", "INBOX")).strip() or "INBOX"
    by_mailbox.setdefault(mbox, []).append(c)

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

# Обход по каждому используемому ящику (обычно это один INBOX)
for mbox, camps in by_mailbox.items():
    t, _ = M.select(mbox, readonly=True)
    if t != "OK":
        print(f"[MAILBOX {mbox}] select FAIL")
        continue

    # Поисковое окно
    since_date = datetime.date.today() - datetime.timedelta(days=35)
    since_imap = imap_since_date(since_date)

    # Один раз ищем письма по FROM и дате
    try:
        typ, data = M.search(None, 'FROM', FROM_ADDR, 'SINCE', since_imap)
    except imaplib.IMAP4.error as e:
        print(f"[MAILBOX {mbox}] SEARCH error: {e!s}")
        continue

    uids = data[0].split() if (typ == 'OK' and data and data[0]) else []
    print(f"[MAILBOX {mbox}] candidates by FROM+SINCE: {len(uids)}")

    # Сопоставление: название кампании -> объект конфигурации
    by_name_lower = {str(c['yandex_name']).strip().lower(): c for c in camps}

    # Сначала: дешёвый проход по заголовкам
    for uid in uids:
        # Тянем только нужные заголовки
        t, md = M.fetch(uid, '(BODY.PEEK[HEADER.FIELDS (Subject Message-ID)])')
        if t != 'OK' or not md or not isinstance(md, list):
            continue

        # Извлекаем bytes заголовка
        header_bytes = b''
        for part in md:
            if isinstance(part, tuple) and part and isinstance(part[1], (bytes, bytearray)):
                header_bytes += part[1]

        if not header_bytes:
            continue

        hdr = email.message_from_bytes(header_bytes)
        subj = dec_header(hdr.get('Subject', ''))
        mid = hdr.get('Message-ID')

        # Парсим тему
        m = SUBJ_RE.search(subj)
        if not m:
            continue

        name = m.group(1).strip()
        yname_key = name.lower()
        if yname_key not in by_name_lower:
            # Письмо не про наши кампании
            continue

        rdate_subj = ru_date_to_date(m.group(2))
        cfg_item = by_name_lower[yname_key]
        cid = int(cfg_item['id'])

        # Дедупликация по message-id (если он есть)
        if mid and cur.execute(
            "SELECT 1 FROM yandex_import_files WHERE message_id=?",
            (mid,)
        ).fetchone():
            continue

        # Теперь забираем полный RFC822 только для совпавших
        t, md_full = M.fetch(uid, '(RFC822)')
        if t != 'OK' or not md_full or not md_full[0]:
            continue

        msg = email.message_from_bytes(md_full[0][1])
        subj_full = dec_header(msg.get('Subject', ''))
        msgs_total += 1

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
                    subj_full,
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
                subj_full,
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
