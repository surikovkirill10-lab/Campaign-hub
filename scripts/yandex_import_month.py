
import os, yaml, imaplib, email, io, sqlite3, datetime, re
from email.header import decode_header, make_header
import pandas as pd

FROM_ADDR = "devnull@yandex.ru"
SUBJ_REX  = re.compile(r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})', re.I)

def dec(s):
    if not s: return ""
    try: return str(make_header(decode_header(s)))
    except: return s

def norm(x:str) -> str:
    x = (x or "").lower().replace("ё","е")
    return re.sub(r'[\s_«»"“”\'\-–—]+','', x)

def ru_date_to_date(s): 
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()

def parse_report_date_from_header(v):
    m=re.search(r'с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})', str(v) if v else "")
    return datetime.datetime.strptime(m.group(2), "%Y-%m-%d").date() if m else None

def parse_xlsx(b: bytes):
    df=pd.read_excel(io.BytesIO(b))
    if len(df)<6: return None,None
    header=df.iloc[3].tolist(); rows=df.iloc[5:].copy(); rows.columns=header
    d=None
    try:
        import openpyxl
        wb=openpyxl.load_workbook(io.BytesIO(b),data_only=True)
        d=parse_report_date_from_header(wb.active.cell(1,1).value)
    except Exception:
        pass
    if rows.empty: return d,None
    def numcol(col):
        if col in rows.columns:
            rows[col]=pd.to_numeric(rows[col], errors='coerce').fillna(0.0)
    for c in ['Визиты','Посетители','Отказы','Глубина просмотра']:
        numcol(c)
    visits=float(rows['Визиты'].sum())
    if visits<=0: return d,None
    visitors=float(rows['Посетители'].sum())
    bounce=float((rows['Отказы']*rows['Визиты']).sum()/visits)
    depth =float((rows['Глубина просмотра']*rows['Визиты']).sum()/visits)
    def t2s(v):
        s=str(v)
        if ":" in s:
            try: h,m,s0=[int(x) for x in s.split(":")]; return float(h*3600+m*60+s0)
            except: return 0.0
        try: return float(v)
        except: return 0.0
    avg=float((rows['Время на сайте'].apply(t2s)*rows['Визиты']).sum()/visits)
    return d, dict(visits=visits,visitors=visitors,bounce_rate=bounce,page_depth=depth,avg_time_sec=avg)

def select_box(M: imaplib.IMAP4, name: str):
    try:
        t,_=M.select(name, readonly=True)
        if t=="OK": return True
        t,_=M.select(f'"{name}"', readonly=True)
        return t=="OK"
    except Exception:
        return False

def month_window():
    m = os.environ.get("YANDEX_MONTH")  # "YYYY-MM" или None -> текущий месяц
    if m:
        y,mm = map(int, m.split("-"))
        first = datetime.date(y,mm,1)
    else:
        today = datetime.date.today()
        first = today.replace(day=1)
    nxt = datetime.date(first.year + (1 if first.month==12 else 0),
                        1 if first.month==12 else first.month+1, 1)
    return first.strftime("%d-%b-%Y"), nxt.strftime("%d-%b-%Y")

def main():
    cfg=yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
    imap=cfg["imap"]; camps=cfg.get("yandex_campaigns") or []

    db=Path("yandex_metrics.db").resolve()
    con=sqlite3.connect(str(db)); cur=con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS yandex_daily_metrics(
      campaign_id INTEGER, report_date TEXT, visits REAL, visitors REAL, bounce_rate REAL, page_depth REAL, avg_time_sec REAL,
      PRIMARY KEY(campaign_id, report_date))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS yandex_import_files(
      id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id INTEGER, message_id TEXT, subject TEXT,
      attachment_name TEXT, report_date TEXT, processed_at TEXT,
      UNIQUE(message_id, attachment_name))""")
    con.commit()

    M=imaplib.IMAP4_SSL(imap.get("host","imap.yandex.com"), int(imap.get("port",993)))
    M.login(imap["user"], imap["password"])
    try: M.enable('UTF8=ACCEPT')
    except Exception: pass
    try: M._encoding='utf-8'
    except Exception: pass

    since, before = month_window()

    rows_total=files_total=msgs_total=0
    for c in camps:
        yname=str(c["yandex_name"]).strip()
        cid  =int(c["id"])
        mbox =str(c.get("mailbox","INBOX")).strip() or "INBOX"

        if not select_box(M, mbox):
            print(f"[{yname}] mailbox FAIL:", mbox); continue

        typ,data=M.search(None,'FROM',FROM_ADDR,'SINCE', since,'BEFORE', before)
        uids=data[0].split() if (typ=='OK' and data and data[0]) else []
        uids = sorted(uids)  # от начала месяца к концу

        matched_after=0
        print(f"[{yname}] candidates_month: {len(uids)} in {mbox} window={since}..{before}")

        for i,uid in enumerate(uids):
            t,md=M.fetch(uid,'(RFC822)')
            if t!='OK' or not md or not md[0]: 
                continue
            msg = email.message_from_bytes(md[0][1])
            subj= dec(msg.get('Subject',''))
            m   = SUBJ_REX.search(subj)
            if not m: 
                continue
            name = m.group(1).strip()
            if norm(name) != norm(yname):
                continue
            rdate_subj = ru_date_to_date(m.group(2)); msgs_total+=1
            mid  = msg.get('Message-ID')
            if mid and cur.execute("SELECT 1 FROM yandex_import_files WHERE message_id=?",(mid,)).fetchone():
                continue

            xlsx=[]
            for part in msg.walk():
                if part.get_content_maintype()=="multipart": continue
                fn_raw=part.get_filename()
                if not fn_raw: continue
                try: fn=str(make_header(decode_header(fn_raw)))
                except Exception: fn=fn_raw
                if not fn.lower().endswith(".xlsx"): continue
                blob=part.get_payload(decode=True) or b""
                xlsx.append((fn,len(blob),blob))
            if not xlsx:
                cur.execute("""INSERT OR IGNORE INTO yandex_import_files
                  (campaign_id,message_id,subject,attachment_name,report_date,processed_at)
                  VALUES (?,?,?,?,?,?)""",(cid,mid,subj,None,None,datetime.datetime.now(datetime.timezone.utc).isoformat()))
                con.commit(); continue

            xlsx.sort(key=lambda x:x[1], reverse=True)
            chosen=None
            for fn,sz,bl in xlsx:
                if re.search(r'таблиц', fn, flags=re.IGNORECASE): chosen=(fn,sz,bl); break
            if not chosen: chosen=xlsx[0]
            fn,sz,blob=chosen

            d_file,metrics=parse_xlsx(blob)
            rdate=d_file or rdate_subj
            if metrics and rdate:
                cur.execute("""INSERT OR REPLACE INTO yandex_daily_metrics
                  (campaign_id,report_date,visits,visitors,bounce_rate,page_depth,avg_time_sec)
                  VALUES (?,?,?,?,?,?,?)""",(cid,str(rdate),metrics['visits'],metrics['visitors'],metrics['bounce_rate'],metrics['page_depth'],metrics['avg_time_sec']))
                rows_total+=1
            cur.execute("""INSERT OR IGNORE INTO yandex_import_files
              (campaign_id,message_id,subject,attachment_name,report_date,processed_at)
              VALUES (?,?,?,?,?,?)""",(cid,mid,subj,fn,str(rdate) if rdate else None,datetime.datetime.now(datetime.timezone.utc).isoformat()))
            con.commit(); files_total+=1; matched_after+=1

        print(f"[{yname}] matched_month: {matched_after}")

    M.logout(); con.close()
    print(f"SUMMARY: msgs={msgs_total}, files={files_total}, rows={rows_total}, db={db}")
if __name__=="__main__":
    main()
