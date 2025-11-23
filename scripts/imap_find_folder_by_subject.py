import imaplib, yaml, sys

SUBJ = "Pesto2flight_Inlab"       # при необходимости смените паттерн
FROM = "devnull@yandex.ru"        # автоотчёты Метрики

def extract_mailbox_name(raw_line: bytes) -> str:
    """
    Разбираем ответ LIST: последний фрагмент в кавычках — имя папки.
    Декодируем из IMAP4-UTF-7 в обычную строку (если есть не-ASCII).
    """
    # ищем последние два символа кавычек
    r = raw_line
    try:
        end = r.rfind(b'"')
        beg = r.rfind(b'"', 0, end)
        name_bytes = r[beg+1:end]
        try:
            return name_bytes.decode('imap4-utf-7')  # корректная кодировка для IMAP имён
        except Exception:
            return name_bytes.decode('utf-8', 'ignore')
    except Exception:
        # запасной путь
        s = raw_line.decode('utf-8','ignore')
        parts = s.split('"')
        return parts[-2] if len(parts) >= 3 else s.strip()

def select_plain(M: imaplib.IMAP4_SSL, name: str):
    """
    ВАЖНО: Передаём в SELECT «чистое» имя папки (без добавления кавычек вручную).
    imaplib сам правильно экранирует.
    """
    return M.select(name, readonly=True)

cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
imap_cfg = cfg.get("imap") or {}
host = imap_cfg.get("host","imap.yandex.com")
port = int(imap_cfg.get("port",993))
user = imap_cfg.get("user"); pwd = imap_cfg.get("password")
if not user or not pwd:
    print("imap.user/password не заданы"); sys.exit(1)

M = imaplib.IMAP4_SSL(host, port)
typ, _ = M.login(user, pwd)
print("LOGIN:", typ)

typ, boxes = M.list()
if typ != "OK" or not boxes:
    print("LIST failed:", typ); sys.exit(2)

print("\n=== MAILBOXES (raw) ===")
folders = []
for raw in boxes:
    print(raw.decode('utf-8','ignore'))
    folders.append(extract_mailbox_name(raw))

print("\n=== SELECT probe ===")
candidates = []
for name in folders:
    t, _ = select_plain(M, name)
    print(f"{name} -> SELECT={t}")
    if t == "OK":
        # быстрый поиск писем по теме/отправителю
        typ, data = M.search(None, 'HEADER', 'Subject', SUBJ, 'FROM', FROM)
        cnt = len(data[0].split()) if (typ=='OK' and data and data[0]) else 0
        if cnt > 0:
            candidates.append((cnt, name))

M.logout()

if not candidates:
    print("\nНЕ НАЙДЕНО: папки с письмами по заданным условиям.")
    sys.exit(3)

candidates.sort(reverse=True)
print("\nКандидаты (совпадений, папка):")
for cnt, name in candidates:
    print(f"{cnt}\t{name}")
print(f"\nRECOMMENDED_FOLDER={candidates[0][1]}")
