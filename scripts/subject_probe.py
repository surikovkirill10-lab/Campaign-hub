
import yaml, imaplib, email, datetime, re, sqlite3
from email.header import decode_header, make_header

SUBJ_REX = re.compile(r'Отч[её]т\s+[«"“](.*?)[»"”]\s+за\s+(\d{2}\.\d{2}\.\d{4})', re.I)

def dec(s):
    try: return str(make_header(decode_header(s or "")))
    except: return s or ""

def norm(x):
    x=str(x or '').lower().replace('ё','е')
    return re.sub(r'[\s_«»"“”\'\-–—]+','', x)

def select_box(M, name):
    try:
        t,_ = M.select(name, readonly=True)
        if t=="OK": return True
        t,_ = M.select(f'"{name}"', readonly=True)
        return t=="OK"
    except Exception:
        return False

cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
im  = cfg["imap"]

dbp = "yandex_metrics.db"
con = sqlite3.connect(dbp)
cur = con.cursor()

for rec in (cfg.get("yandex_campaigns") or []):
    cid   = int(rec["id"])
    yname = str(rec["yandex_name"]).strip()
    mbox  = str(rec.get("mailbox","INBOX")).strip() or "INBOX"

    M=imaplib.IMAP4_SSL(im.get("host","imap.yandex.com"), int(im.get("port",993)))
    M.login(im["user"], im["password"])
    try: M.enable('UTF8=ACCEPT')
    except: pass
    M._encoding='utf-8'

    if not select_box(M, mbox):
        print(f"[{yname}] mailbox FAIL:", mbox); M.logout(); continue

    row = cur.execute("SELECT MAX(report_date) FROM yandex_daily_metrics WHERE campaign_id=?", (cid,)).fetchone()
    maxd = row[0] if row else None
    if maxd:
        try:
            d = datetime.datetime.strptime(maxd, "%Y-%m-%d").date()
            since = (d - datetime.timedelta(days=7)).strftime("%d-%b-%Y")
        except Exception:
            since = (datetime.date.today() - datetime.timedelta(days=45)).strftime("%d-%b-%Y")
    else:
        since = (datetime.date.today() - datetime.timedelta(days=45)).strftime("%d-%b-%Y")

    typ,data = M.search(None, 'FROM','devnull@yandex.ru','SINCE', since)
    uids = data[0].split() if (typ=='OK' and data and data[0]) else []
    uids = uids[-300:]

    matched = 0
    samples = []

    for uid in reversed(uids):
        t,md = M.fetch(uid,'(RFC822.HEADER)')
        if t!='OK' or not md or not md[0]: continue
        msg  = email.message_from_bytes(md[0][1])
        subj = dec(msg.get('Subject',''))
        m    = SUBJ_REX.search(subj)
        if not m: 
            continue
        name = m.group(1).strip()
        if norm(name) == norm(yname):
            matched += 1
            if len(samples) < 3:
                samples.append(subj)

    print(f"[{yname}] mailbox={mbox} since={since} candidates={len(uids)} matched={matched}")
    for s in samples:
        print("  ", s)
    M.logout()

con.close()
