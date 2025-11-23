import os, imaplib, email, io, yaml, sqlite3, datetime, re
import pandas as pd
from email.header import decode_header, make_header

FROM_ADDR = "devnull@yandex.ru"
SUBJ_RE   = re.compile(r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})', re.IGNORECASE)

def dec(s:str)->str:
    if not s: return ""
    try: return str(make_header(decode_header(s)))
    except: return s

def ru_date_to_date(s:str)->datetime.date:
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()

def parse_report_date_from_header(v):
    if not v: return None
    m=re.search(r'с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})', str(v))
    return datetime.datetime.strptime(m.group(2), "%Y-%m-%d").date() if m else None

def parse_xlsx(data:bytes):
    df = pd.read_excel(io.BytesIO(data))
    if len(df) < 6: return None, None
    header = df.iloc[3].tolist()
    rows   = df.iloc[5:].copy()
    rows.columns = header
    date_a1 = None
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
        date_a1 = parse_report_date_from_header(wb.active.cell(1,1).value)
    except Exception:
        pass
    if rows.empty: return date_a1, None
    rows['Визиты'] = pd.to_numeric(rows['Визиты'], errors='coerce').fillna(0.0)
    rows['Посетители'] = pd.to_numeric(rows['Посетители'], errors='coerce').fillna(0.0)
    rows['Отказы'] = pd.to_numeric(rows['Отказы'], errors='coerce').fillna(0.0)
    rows['Глубина просмотра'] = pd.to_numeric(rows['Глубина просмотра'], errors='coerce').fillna(0.0)
    visits = float(rows['Визиты'].sum())
    if visits <= 0: return date_a1, None
    visitors = float(rows['Посетители'].sum())
    bounce = float((rows['Отказы'] * rows['Визиты']).sum() / visits)
    depth  = float((rows['Глубина просмотра'] * rows['Визиты']).sum() / visits)
    def t2s(v):
        s=str(v)
        if ":" in s:
            try: h,m,s0=[int(x) for x in s.split(":")]; return float(h*3600+m*60+s0)
            except: return 0.0
        try: return float(v)
        except: return 0.0
    avg = float((rows['Время на сайте'].apply(t2s) * rows['Визиты']).sum() / visits)
    return date_a1, dict(visits=visits, visitors=visitors, bounce_rate=bounce, page_depth=depth, avg_time_sec=avg)

# --- config
cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
imap_cfg = cfg["imap"]
camp = (cfg.get("yandex_campaigns") or [])[0]
yname = str(camp["yandex_name"]).strip()
cid   = int(camp["id"])

# --- IMAP
M = imaplib.IMAP4_SSL(imap_cfg.get("host","imap.yandex.com"), int(imap_cfg.get("port",993)))
M.login(imap_cfg["user"], imap_cfg["password"])
M.select("INBOX", readonly=True)

typ, data = M.search(None, 'FROM', FROM_ADDR, 'SUBJECT', f'"{yname}"')
uids = data[0].split() if (typ=='OK' and data and data[0]) else []
print(f"[{yname}] matched total: {len(uids)}")

# идём с конца и берём первое письмо, где имя в теме ТОЧНО равно yname
uid_chosen = None; subj_chosen = None; rdate_subj = None
for uid in reversed(uids):
    t, md = M.fetch(uid, '(RFC822)')
    if t!='OK': continue
    msg  = email.message_from_bytes(md[0][1])
    subj = dec(msg.get('Subject',''))
    m = SUBJ_RE.search(subj)
    if not m: 
        continue
    name = m.group(1).strip()
    if name.lower() != yname.lower():
        # это "Соцдем_..." или другой отчёт — пропускаем
        continue
    rdate_subj = ru_date_to_date(m.group(2))
    uid_chosen = uid
    subj_chosen = subj
    break

if not uid_chosen:
    M.logout(); raise SystemExit("Нет писем с точным именем кампании в теме")

print("subject:", subj_chosen)

# выбираем таблицу (или самый большой xlsx)
t, md = M.fetch(uid_chosen, '(RFC822)')
msg   = email.message_from_bytes(md[0][1])
xlsx  = []
for part in msg.walk():
    if part.get_content_maintype()=="multipart": continue
    fn_raw = part.get_filename()
    if not fn_raw: continue
    fn = dec(fn_raw)
    if not fn.lower().endswith(".xlsx"): continue
    blob = part.get_payload(decode=True) or b""
    xlsx.append((fn, len(blob), blob))

if not xlsx:
    M.logout(); raise SystemExit("xlsx-вложений нет")

xlsx.sort(key=lambda x: x[1], reverse=True)
chosen = None
for fn,sz,bl in xlsx:
    if re.search(r"таблиц", fn, flags=re.IGNORECASE):
        chosen = (fn,sz,bl); break
if not chosen:
    chosen = xlsx[0]
fn, sz, blob = chosen
print(f"chosen: {fn} ({sz} bytes)")

date_file, metrics = parse_xlsx(blob)
report_date = date_file or rdate_subj
print("report_date:", report_date, "metrics:", metrics)

# --- DB write
db = os.path.abspath(os.path.join("scripts","..","yandex_metrics.db"))
con = sqlite3.connect(db); cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS yandex_daily_metrics(
  campaign_id INTEGER, report_date TEXT, visits REAL, visitors REAL,
  bounce_rate REAL, page_depth REAL, avg_time_sec REAL,
  PRIMARY KEY(campaign_id, report_date)
);""")
cur.execute("""CREATE TABLE IF NOT EXISTS yandex_import_files(
  id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id INTEGER, message_id TEXT, subject TEXT,
  attachment_name TEXT, report_date TEXT, processed_at TEXT,
  UNIQUE(message_id, attachment_name)
);""")
mid = msg.get('Message-ID')

if metrics:
    cur.execute("""INSERT OR REPLACE INTO yandex_daily_metrics
      (campaign_id, report_date, visits, visitors, bounce_rate, page_depth, avg_time_sec)
      VALUES (?,?,?,?,?,?,?)""",
      (cid, str(report_date), metrics['visits'], metrics['visitors'],
       metrics['bounce_rate'], metrics['page_depth'], metrics['avg_time_sec']))

cur.execute("""INSERT OR IGNORE INTO yandex_import_files
  (campaign_id, message_id, subject, attachment_name, report_date, processed_at)
  VALUES (?,?,?,?,?,?)""",
  (cid, mid, subj_chosen, fn, str(report_date), datetime.datetime.utcnow().isoformat()))
con.commit(); con.close(); M.logout()
print("WROTE OK →", db)
