#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inlab_import7.py
----------------
Вариант №7 с фиксами + расширенный лог:
- Поиск писем: UTF-8 + quoted bytes (FROM + SINCE + HEADER Subject <yname>).
- Извлечение вложений «цепко»: .xlsx по имени ИЛИ по MIME, а также .xlsx из .zip.
- Дедуп перенесён ПОСЛЕ выбора вложения и идёт по паре (message_id, attachment_name),
  чтобы старые записи с attachment_name=NULL не блокировали загрузку файлов.
- Независимость от текущего каталога: config.yaml и БД ищутся относительно расположения этого скрипта.
- Подробный лог по шагам: конфиг, кампании, выбор ящика, поиск писем, тема, фильтры, вложения, парсинг.

Включить подробный лог по частям письма: YANDEX_IMPORT_DEBUG=1
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

# ------------ Настройки / константы ------------
DEBUG = os.getenv('YANDEX_IMPORT_DEBUG', '0') == '1'
FROM_ADDR = "devnull@yandex.ru"  # можно вынести в config.yaml при желании
SEARCH_DAYS = int(os.getenv('YANDEX_IMPORT_DAYS', '35'))  # окно поиска назад

# Регэксп темы: Отчёт «Название кампании» за DD.MM.YYYY
SUBJ_RE = re.compile(
    r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})',
    re.IGNORECASE
)


def dprint(*args, **kwargs):
    """Дополнительный отладочный принт, включается YANDEX_IMPORT_DEBUG=1."""
    if DEBUG:
        print(*args, **kwargs)


def dec_header(s: str) -> str:
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s


def ru_date_to_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()


def parse_report_date_from_header(v):
    m = re.search(r'с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})', str(v) if v else "")
    return datetime.datetime.strptime(m.group(2), "%Y-%m-%d").date() if m else None


def parse_xlsx(b):
    """Парсит .xlsx‑отчёт Яндекс.Метрики. Возвращает (report_date, metrics_dict|None)."""
    try:
        df = pd.read_excel(io.BytesIO(b))
    except Exception as e:
        print("parse_xlsx: failed to read excel:", e)
        return None, None

    if len(df) < 6:
        print("parse_xlsx: dataframe too short, rows:", len(df))
        return None, None

    header = df.iloc[3].tolist()
    rows = df.iloc[5:].copy()
    rows.columns = header

    d = None
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(b), data_only=True)
        d = parse_report_date_from_header(wb.active.cell(1, 1).value)
    except Exception as e:
        dprint("parse_xlsx: openpyxl header parse error:", e)

    if rows.empty:
        print("parse_xlsx: rows empty after slicing (no data)")
        return d, None

    # --- ключевой фикс: всегда возвращаем Series длиной len(rows)
    def as_num(col):
        if col in rows.columns:
            s = rows[col]
        else:
            s = pd.Series([0.0] * len(rows), index=rows.index)
        s = pd.to_numeric(s, errors='coerce')
        return s.fillna(0.0).astype(float)

    rows.loc[:, 'Визиты'] = as_num('Визиты')
    rows.loc[:, 'Посетители'] = as_num('Посетители')
    rows.loc[:, 'Отказы'] = as_num('Отказы')
    rows.loc[:, 'Глубина просмотра'] = as_num('Глубина просмотра')

    visits = float(rows['Визиты'].sum())
    if visits <= 0:
        print("parse_xlsx: visits sum <= 0, visits=", visits)
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

    # Колонка времени может называться по-разному или отсутствовать
    time_candidates = [
        'Время на сайте',
        'Среднее время на сайте',
        'Среднее время на сайте, сек'
    ]
    time_col = next((c for c in time_candidates if c in rows.columns), None)
    if time_col is None:
        avg = 0.0
    else:
        time_series = rows[time_col].apply(t2s)
        avg = float((time_series * rows['Визиты']).sum() / visits)

    print(
        "parse_xlsx: metrics ->",
        "visits=", visits,
        "visitors=", visitors,
        "bounce_rate=", bounce,
        "page_depth=", depth,
        "avg_time_sec=", avg,
        "report_date=", d,
    )

    return d, dict(
        visits=visits,
        visitors=visitors,
        bounce_rate=bounce,
        page_depth=depth,
        avg_time_sec=avg
    )


def imap_since_date(d: datetime.date) -> str:
    months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ]
    return f"{d.day:02d}-{months[d.month - 1]}-{d.year}"


def imap_utf8_quoted_bytes(s: str) -> bytes:
    """Готовим корректный quoted-string и кодируем в UTF-8."""
    s = (s or "").replace('\\', '\\\\').replace('"', '\\"')
    return f'"{s}"'.encode('utf-8')


def normalize_filename(part) -> str | None:
    """Пытаемся получить имя вложения из разных источников заголовков."""
    fn = part.get_filename()
    if not fn:
        fn = part.get_param('name', header='content-type')
    if not fn:
        for header in ('content-disposition', 'content-type'):
            for key in ('filename*', 'name*'):
                val = part.get_param(key, header=header)
                if val:
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
    Ищем .xlsx в письме:
    - по имени файла (.xlsx),
    - по MIME application/vnd.openxmlformats-officedocument.spreadsheetml.sheet (даже без имени),
    - внутри .zip архивов.
    Возвращаем кортежи (display_name, size, bytes).
    """
    XLSX_MIMES = {
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }

    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue

        ctype = part.get_content_type()
        dispo = part.get_content_disposition()
        fn = normalize_filename(part)
        raw = part.get_payload(decode=True) or b''

        dprint("  part:", ctype, dispo, fn, len(raw))

        # 1) .xlsx по имени
        if fn and fn.lower().endswith('.xlsx'):
            dprint("   -> xlsx by filename:", fn, "size=", len(raw))
            yield (fn, len(raw), raw)
            continue

        # 2) .xlsx по MIME — даже без имени
        if ctype in XLSX_MIMES and raw:
            disp_name = fn or "attachment.xlsx"
            dprint("   -> xlsx by MIME:", disp_name, "size=", len(raw))
            yield (disp_name, len(raw), raw)
            continue

        # 3) .zip с .xlsx внутри
        if (fn and fn.lower().endswith('.zip')) or ctype in (
            'application/zip',
            'application/x-zip-compressed'
        ):
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    for n in zf.namelist():
                        if n.lower().endswith('.xlsx'):
                            blob = zf.read(n)
                            disp_name = f"{fn}:{n}" if fn else n
                            dprint("   -> xlsx in zip:", disp_name, "size=", len(blob))
                            yield (disp_name, len(blob), blob)
            except Exception as e:
                dprint("   zip parse error:", e)


def main():
    # --- Определяем пути относительно файла ---
    base_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(base_dir, '..'))
    config_path = os.getenv('INLAB_CONFIG', os.path.join(root_dir, 'config.yaml'))
    db_path = os.getenv('INLAB_DB', os.path.join(root_dir, 'yandex_metrics.db'))

    print("=== YANDEX IMPORT START ===")
    print("BASE DIR:", base_dir)
    print("ROOT DIR:", root_dir)
    print("CONFIG PATH:", config_path)
    print("DB PATH:", db_path)
    print("SEARCH_DAYS:", SEARCH_DAYS, "FROM_ADDR:", FROM_ADDR)
    print("DEBUG (YANDEX_IMPORT_DEBUG):", DEBUG)

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    imap_cfg = cfg["imap"]
    camps = cfg.get("yandex_campaigns") or []

    print("IMAP CONFIG:",
          "host=", imap_cfg.get("host", "imap.yandex.com"),
          "port=", imap_cfg.get("port", 993),
          "user=", imap_cfg.get("user"))

    print("Yandex campaigns from config (yandex_campaigns):")
    if not camps:
        print("  (empty list)")
    for c in camps:
        print("  - id=", c.get("id"),
              "yandex_name=", repr(c.get("yandex_name")),
              "mailbox=", repr(c.get("mailbox", "INBOX")))

    con = sqlite3.connect(db_path)
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

    M = imaplib.IMAP4_SSL(
        imap_cfg.get("host", "imap.yandex.com"),
        int(imap_cfg.get("port", 993))
    )
    M.login(imap_cfg["user"], imap_cfg["password"])
    print("IMAP login OK")

    rows_total = 0
    files_total = 0
    msgs_total = 0

    for c in camps:
        yname = str(c["yandex_name"]).strip()
        cid = int(c["id"])
        mbox = str(c.get("mailbox", "INBOX")).strip() or "INBOX"

        print(f"\n=== Campaign id={cid} yandex_name={yname!r} mailbox={mbox!r} ===")

        try:
            t, _ = M.select(mbox, readonly=True)
        except Exception as e:
            print(f"[{yname}] ERROR on SELECT {mbox!r}: {e}")
            continue

        print(f"[{yname}] SELECT mailbox -> {t}")
        if t != "OK":
            print(f"[{yname}] mailbox FAIL: {mbox}")
            continue

        since_date = datetime.date.today() - datetime.timedelta(days=SEARCH_DAYS)
        since_imap = imap_since_date(since_date).encode('ascii')

        from_q = imap_utf8_quoted_bytes(FROM_ADDR)
        subj_q = imap_utf8_quoted_bytes(yname)

        print(f"[{yname}] SEARCH UTF-8 since={since_date} from={FROM_ADDR} subj={yname!r}")
        try:
            typ, data = M.search(
                'UTF-8',
                b'FROM', from_q,
                b'SINCE', since_imap,
                b'HEADER', b'Subject', subj_q
            )
            print(f"[{yname}] SEARCH UTF-8 result typ={typ} raw={data}")
            if typ != 'OK':
                raise imaplib.IMAP4.error('SEARCH failed')
        except imaplib.IMAP4.error as e:
            print(f"[{yname}] SEARCH UTF-8 failed ({e!s}), fallback to FROM+SINCE")
            typ, data = M.search(
                None,
                'FROM', FROM_ADDR,
                'SINCE', imap_since_date(since_date)
            )
            print(f"[{yname}] SEARCH fallback result typ={typ} raw={data}")

        uids = data[0].split() if (typ == 'OK' and data and data[0]) else []
        print(f"[{yname}] matched by search: {len(uids)} in {mbox}")

        for uid in uids:
            uid_str = uid.decode(errors='ignore') if isinstance(uid, bytes) else str(uid)
            print(f"[{yname}] FETCH uid={uid_str}")
            t, md = M.fetch(uid, '(RFC822)')
            if t != 'OK' or not md or not md[0]:
                print(f"  -> fetch FAILED: t={t}, md={md}")
                continue

            msg = email.message_from_bytes(md[0][1], policy=policy.default)
            subj = dec_header(msg.get('Subject', ''))
            print(f"  subject={subj!r}")

            m = SUBJ_RE.search(subj)
            if not m:
                print("   -> skip: subject regex SUBJ_RE not matched")
                continue

            name = m.group(1).strip()
            if name.lower() != yname.lower():
                print(
                    f"   -> skip: name mismatch in subject: "
                    f"inner={name!r} vs yandex_name={yname!r}"
                )
                continue

            rdate_subj = ru_date_to_date(m.group(2))
            print(f"   subject OK, report_date_from_subject={rdate_subj}")
            msgs_total += 1
            mid = msg.get('Message-ID')
            print(f"   Message-ID={mid!r}")

            # --- Ищем xlsx (включая внутри .zip) ---
            xlsx = list(iter_xlsx_blobs(msg))
            print(f"   xlsx candidates: {[n for (n, _, __) in xlsx]}")

            if not xlsx:
                print("   -> no xlsx attachments found, logging empty import_files row")
                # Логируем факт письма без xlsx, но не блокируем будущую обработку
                cur.execute(
                    """INSERT OR IGNORE INTO yandex_import_files
                       (campaign_id, message_id, subject, attachment_name,
                        report_date, processed_at)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        cid, mid, subj, None, None,
                        datetime.datetime.now(datetime.timezone.utc).isoformat()
                    )
                )
                con.commit()
                continue

            # «таблиц*» приоритетнее, иначе берём самый большой файл
            xlsx.sort(key=lambda x: x[1], reverse=True)
            chosen = None
            for fn, sz, bl in xlsx:
                if fn and re.search(r'таблиц', fn, flags=re.IGNORECASE):
                    chosen = (fn, sz, bl)
                    break
            if not chosen:
                chosen = xlsx[0]

            fn, sz, blob = chosen
            print(f"   chosen attachment: {fn!r}, size={sz}")

            # --- ДЕДУП СЕЙЧАС: по паре (message_id, attachment_name) ---
            if mid and cur.execute(
                "SELECT 1 FROM yandex_import_files "
                "WHERE message_id=? AND attachment_name=?",
                (mid, fn)
            ).fetchone():
                print(
                    f"   -> skip duplicate attachment "
                    f"for message_id={mid!r}, attachment_name={fn!r}"
                )
                continue

            d_file, metrics = parse_xlsx(blob)
            rdate = d_file or rdate_subj
            print(
                f"   parsed_xlsx -> report_date_from_file={d_file}, "
                f"rdate_final={rdate}, metrics={metrics}"
            )

            if metrics and rdate:
                print(
                    f"   -> saving metrics: cid={cid}, date={rdate}, "
                    f"visits={metrics['visits']}, visitors={metrics['visitors']}"
                )
                cur.execute(
                    """INSERT OR REPLACE INTO yandex_daily_metrics
                       (campaign_id, report_date, visits, visitors,
                        bounce_rate, page_depth, avg_time_sec)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        cid, str(rdate), metrics['visits'], metrics['visitors'],
                        metrics['bounce_rate'], metrics['page_depth'],
                        metrics['avg_time_sec']
                    )
                )
                rows_total += 1
            else:
                print(
                    f"   -> no metrics or no date, skip metrics insert "
                    f"(metrics={metrics}, rdate={rdate})"
                )

            cur.execute(
                """INSERT OR IGNORE INTO yandex_import_files
                   (campaign_id, message_id, subject, attachment_name,
                    report_date, processed_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    cid, mid, subj, fn,
                    str(rdate) if rdate else None,
                    datetime.datetime.now(datetime.timezone.utc).isoformat()
                )
            )
            con.commit()
            files_total += 1
            print("   -> recorded import_files row")

    M.logout()
    print(
        f"\nSUMMARY: msgs={msgs_total}, files={files_total}, "
        f"rows={rows_total}, db={db_path}"
    )
    con.close()
    print("=== YANDEX IMPORT FINISHED ===")


if __name__ == "__main__":
    main()
