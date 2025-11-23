import ssl, imaplib, yaml, sys, re

def q(name:str)->str:
    name = name.strip()
    if name.startswith('"') and name.endswith('"'):
        return name
    return f'"{name}"'

cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
imap_cfg = cfg.get("imap") or {}
host = imap_cfg.get("host","imap.yandex.com")
port = int(imap_cfg.get("port",993))
user = imap_cfg.get("user"); pwd = imap_cfg.get("password")

if not user or not pwd:
    print("imap.user/password не заданы в config.yaml"); sys.exit(1)

M = imaplib.IMAP4_SSL(host, port)
typ, _ = M.login(user, pwd)
print("LOGIN:", typ)

# Выводим все папки с сырым ответом сервера
typ, boxes = M.list()
if typ != "OK":
    print("LIST failed:", typ); sys.exit(2)

print("\n=== MAILBOXES (как видит сервер) ===")
names = []
for raw in boxes or []:
    s = raw.decode("utf-8","ignore")
    print(s)
    # попытка выдрать реальное имя папки (последний токен может быть в кавычках)
    m = re.search(r'\\)\\s+"([^"]+)"\\s+"([^"]+)"\\s+(.*)$', s)
    if m:
        delim = m.group(2)
        name = m.group(3).strip()
    else:
        # запасной план: имя папки после второго перевода кавычек
        parts = s.split(') ')
        tail = parts[-1] if parts else s
        name = tail.split(' ',1)[-1].strip()
    name = name.strip()
    names.append(name)

# Пытаемся сделать SELECT по каждому имени "как есть" и "в кавычках"
print("\n=== SELECT probe ===")
for name in names:
    name_unq = name.strip()
    name_q = q(name_unq)
    # Попытка 1: как есть
    typ1, _ = M.select(name_unq, readonly=True)
    # Попытка 2: в кавычках (если первая не OK)
    if typ1 != "OK":
        typ2, _ = M.select(name_q, readonly=True)
    else:
        typ2 = None
    print(f"{name_unq}  ->  SELECT(plain)={typ1}  SELECT(quoted)={typ2}")

# Если в config.yaml задана папка — проверим её отдельно
yc = (cfg.get("yandex_campaigns") or [])
if yc:
    folder = yc[0].get("folder")
    if folder:
        print(f"\n=== Проверка из config.yaml: {folder} ===")
        t1,_ = M.select(folder, readonly=True)
        if t1 != "OK":
            t2,_ = M.select(q(folder), readonly=True)
            print("SELECT plain:", t1, " SELECT quoted:", t2)
        else:
            print("SELECT plain: OK")

M.logout()
