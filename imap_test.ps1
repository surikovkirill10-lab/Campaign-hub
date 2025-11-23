@"
import imaplib, getpass
def prompt():
    host = input("IMAP host: ").strip()
    port_s = input("IMAP port [993]: ").strip() or "993"
    try: port = int(port_s)
    except: port = 993
    user = input("IMAP user: ")
    pwd  = getpass.getpass("IMAP password: ")
    tfa  = input("two_factor (off/app_password) [off]: ").strip().lower() or "off"
    return host, port, user, pwd, tfa
def main():
    host, port, user, pwd, tfa = prompt()
    print("--- INPUT DEBUG ---")
    print("user repr:", repr(user))
    print("pass repr:", repr(pwd))
    print("pass len :", len(pwd))
    if tfa == "app_password":
        cleaned = "".join(pwd.split())
        print("cleaned len:", len(cleaned), "(whitespace removed)" if cleaned != pwd else "(no change)")
        pwd = cleaned
    print("--- TRY LOGIN ---")
    try:
        M = imaplib.IMAP4_SSL(host, port)
        typ, data = M.login(user, pwd)
        print("LOGIN:", typ, data)
        typ, mailboxes = M.list()
        print("LIST:", typ, "count:", 0 if mailboxes is None else len(mailboxes))
        M.logout()
    except imaplib.IMAP4.error as e:
        print("IMAP4.error:", e)
    except Exception as e:
        print("Unexpected:", e)
if __name__ == "__main__":
    main()
"@ | python -
