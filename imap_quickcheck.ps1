@"
from imapclient import IMAPClient
import getpass
host = input("IMAP host: ").strip()
port = int(input("IMAP port [993]: ") or 993)
user = input("IMAP user: ")
pwd  = getpass.getpass("IMAP password: ")
tfa  = input("two_factor (off/app_password) [off]: ").strip().lower() or "off"
pwd2 = "".join(pwd.split()) if tfa == "app_password" else pwd
print(f"Password length: {len(pwd)} -> {len(pwd2)} after clean")
with IMAPClient(host, port=port, ssl=True) as c:
    c.login(user, pwd2)
    print("LOGIN OK")
    boxes = c.list_folders()
    print(f"Folders: {0 if boxes is None else len(boxes)}")
"@ | python -
