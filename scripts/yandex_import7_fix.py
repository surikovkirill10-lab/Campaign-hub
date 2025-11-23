# -*- coding: utf-8 -*-
"""
yandex_import7_fix.py
---------------------
Патч к варианту №7:
- Дедупликация перенесена ПОСЛЕ извлечения вложений и теперь идёт по паре (message_id, attachment_name),
  чтобы прежние записи с attachment_name=NULL не блокировали обработку.
- Поиск вложений стал более «цепким»: берём .xlsx по расширению ИЛИ по MIME
  (application/vnd.openxmlformats-officedocument.spreadsheetml.sheet), а также вытаскиваем .xlsx из .zip.
- Добавлены отладочные принты (включаются переменной окружения YANDEX_IMPORT_DEBUG=1).

Поиск писем сохраняет логику UTF‑8 bytes + quoted-string (FROM + SINCE + HEADER Subject <yname>).
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
import zipfile
from email.header import decode_header, make_header
from email import policy

DEBUG = os.getenv('YANDEX_IMPORT_DEBUG', '0') == '1'

# От кого приходят отчёты
FROM_ADDR = "devnull@yandex.ru"

# Пример темы: Отчёт «Название кампании» за 28.10.2025
SUBJ_RE = re.compile(
    r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})',
    re.IGNORECASE
)


def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


def dec_header(s: str) -> str:
    """Декодируем header в читаемую строку (для отображения/логов)."""
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
        import openpyxl  # только чтобы аккуратно прочитать первую ячейку с датами
        wb = openpyxl.load_workbook(io.BytesIO(b), data_only=True)
        d = parse_report_date_from_header(wb.active.cell(1, 1).value)
    except Exception:
        pass

    if rows.empty:
        return d, None

    def as_num(col):
        return pd.to_numeric(rows.get(col), errors='coerce').fillna(0.0)

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

    time_col = None
    for cand in ['Время на сайте', 'Среднее время на сайте']:
        if cand in rows.columns:
            time_col = cand
            break
    avg = float((rows[time_col].apply(t2s) * rows['Визиты']).sum() / visits) if time_col else 0.0

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


def imap_utf8_quoted_bytes(s: str) -> bytes:
    """
    Возвращаем корректный quoted-string в UTF-8 для IMAP.
    """
    s = (s or "").replace('\\', '\\\\').replace('"', '\\"')
    return f'"{s}"'.encode('utf-8')


def normalize_filename(part) -> str | None:
    """
    Получаем имя файла вложения максимально надёжно:
    - filename из Content-Disposition,
    - name из Content-Type,
    - RFC2231-параметры (filename* / name*).
    """
    fn = part.get_filename()
    if not fn:
        # Попробуем name= из Content-Type
        fn = part.get_param('name', header='content-type')
    if not fn:
        # Попробуем расширенные параметры RFC2231
        for header in ('content-disposition', 'content-type'):
            for key in ('filename*', 'name*'):
                val = part.get_param(key, header=header)
                if val:
                    # email пакет обычно уже разворачивает RFC2231, просто берём строку
                    fn = val
                    break
            if fn:
                break

    if not fn:
        return None

    try:
        return str(make_header(decode_header(fn)))
    except Exception:
        return str(fn)


def iter_xlsx_blobs(msg):
    """
    Генератор: возвращает (display_name, size, bytes) для каждого найденного .xlsx,
    в т.ч. xlsx внутри zip.
    """
    XLSX_MIMES = {
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    }

    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue

        ctype = part.get_content_type()
        dispo = part.get_content_disposition()
        fn = normalize_filename(part)
        raw = part.get_payload(decode=True) or b''

        dprint("  part:", ctype, dispo, fn, len(raw))

        # 1) Явное .xlsx по расширению
        if fn and fn.lower().endswith('.xlsx'):
            yield (fn, len(raw), raw)
            continue

        # 2) По MIME-типу — даже если нет имени
        if ctype in XLSX_MIMES and raw:
            name = fn or "attachment.xlsx"
            yield (name, len(raw), raw)
            continue

        # 3) .zip с .xlsx внутри
        if (fn and fn.lower().endswith('.zip')) or ctype in ('application/zip', 'application/x-zip-compressed'):
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    xlsx_names = [n for n in zf.namelist() if n.lower().endswith('.xlsx')]
                    for n in xlsx_names:
                        blob = zf.read(n)
                        disp_name = f"{fn}:{n}" if fn else n
                        yield (disp_name, len(blob), blob)
            except Exception as e:
                dprint("   zip parse error:", e)


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

    since_date = datetime.date.today() - datetime.timedelta(days=35)
    since_imap = imap_since_date(since_date).encode('ascii')

    from_q = imap_utf8_quoted_bytes(FROM_ADDR)
    subj_q = imap_utf8_quoted_bytes(yname)

    try:
        typ, data = M.search(
            'UTF-8',
            b'FROM', from_q,
            b'SINCE', since_imap,
            b'HEADER', b'Subject', subj_q
        )
        if typ != 'OK':
            raise imaplib.IMAP4.error('SEARCH failed')
    except imaplib.IMAP4.error as e:
        print(f"[{yname}] SEARCH UTF-8 failed ({e!s}), fallback to FROM+SINCE")
        typ, data = M.search(None, 'FROM', FROM_ADDR, 'SINCE', imap_since_date(since_date))

    uids = data[0].split() if (typ == 'OK' and data and data[0]) else []
    print(f"[{yname}] matched by UTF-8 search: {len(uids)} in {mbox}")

    for uid in uids:
        t, md = M.fetch(uid, '(RFC822)')
        if t != 'OK' or not md or not md[0]:
            continue

        # policy=default даёт EmailMessage c get_content_disposition()/iter_attachments()
        msg = email.message_from_bytes(md[0][1], policy=policy.default)
        subj = dec_header(msg.get('Subject', ''))
        m = SUBJ_RE.search(subj)
        if not m:
            continue

        name = m.group(1).strip()
        if name.lower() != yname.lower():
            continue

        rdate_subj = ru_date_to_date(m.group(2))
        msgs_total += 1

        mid = msg.get('Message-ID')

        # Находим кандидатов на xlsx (включая внутри zip)
        xlsx = list(iter_xlsx_blobs(msg))
        if DEBUG:
            print(f" [{yname}] {subj} -> xlsx candidates: {[n for n,_,__ in xlsx]}")

        if not xlsx:
            # Логируем факт письма без xlsx — НО не блокируем будущую обработку.
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

        # Берём «таблиц*» если есть, иначе самое большое
        xlsx.sort(key=lambda x: x[1], reverse=True)
        chosen = None
        for fn, sz, bl in xlsx:
            if re.search(r'таблиц', fn, flags=re.IGNORECASE):
                chosen = (fn, sz, bl)
                break
        if not chosen:
            chosen = xlsx[0]

        fn, sz, blob = chosen

        # ТОЛЬКО теперь — дедуп по паре (message_id, attachment_name)
        if mid and cur.execute(
            "SELECT 1 FROM yandex_import_files WHERE message_id=? AND attachment_name=?",
            (mid, fn)
        ).fetchone():
            dprint(f"  skip duplicate attachment for {mid} / {fn}")
            continue

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
