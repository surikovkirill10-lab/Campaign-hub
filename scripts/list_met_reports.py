import imaplib, yaml, email, datetime, re

DAYS_BACK = 14
FROM_ADDR = "devnull@yandex.ru"

def imap_date(d): return d.strftime("%d-%b-%Y")

cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
imap_cfg = cfg.get("imap") or {}
host = imap_cfg.get("host","imap.yandex.com")
port = int(imap_cfg.get("port",993))
user = imap_cfg.get("user"); pwd = imap_cfg.get("password")
if not user or not pwd:
    print("imap.user/password не задан")
    exit(1)

since_date = datetime.date.today() - datetime.timedelta(days=DAYS_BACK)

M = imaplib.IMAP4_SSL(host, port)
M.login(user, pwd)
typ, _ = M.select("INBOX", readonly=True)
if typ != "OK":
    print("Не удалось выбрать INBOX:", typ)
    M.logout(); exit(2)

search_crit = f'(FROM "{FROM_ADDR}" SINCE {imap_date(since_date)})'
typ, data = M.search(None, search_crit)
if typ != "OK":
    print("Ошибка поиска:", typ)
    M.logout(); exit(3)

msg_ids = data[0].split() if data and data[0] else []
print(f"Found {len(msg_ids)} messages from {FROM_ADDR} since {since_date}:\\n")
for uid in msg_ids:
    typ, msg_data = M.fetch(uid, "(RFC822)")
    if typ != "OK": continue
    msg = email.message_from_bytes(msg_data[0][1])
    subj_raw = msg.get("Subject","")
    try:
        subj = str(email.header.make_header(email.header.decode_header(subj_raw)))
    except Exception:
        subj = subj_raw
    date_hdr = msg.get("Date","")
    print(f"Subject: {subj}\\nDate:    {date_hdr}")
    # attachments
    for part in msg.walk():
        if part.get_content_maintype()=="multipart": continue
        fname = part.get_filename()
        if fname:
            print(f"  Attachment: {fname}")
    print("-"*40)

M.logout()
