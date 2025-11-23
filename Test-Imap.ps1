param(
  [string]$HostName,
  [int]$Port = 993,
  [string]$UserName,
  [ValidateSet("off","app_password")][string]$TwoFactor = "off"
)

if (-not $HostName)  { $HostName  = Read-Host "IMAP host" }
if (-not $Port)      { $Port      = [int](Read-Host "IMAP port (default 993)") }
if (-not $UserName)  { $UserName  = Read-Host "IMAP user" }
if (-not $TwoFactor) { $TwoFactor = Read-Host "two_factor (off/app_password)" }

$sec = Read-Host "IMAP password" -AsSecureString
$ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
try { $Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }

$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

$code = @"
import sys, argparse, imaplib, re

def has_non_ascii(s: str) -> bool:
    try:
        s.encode('ascii')
        return False
    except UnicodeEncodeError:
        return True

def sanitize_app_password(p: str) -> str:
    # Удаляем все типы пробельных в том числе NBSP и юникодные пробелы
    p2 = re.sub(r'\s+', '', p, flags=re.UNICODE)
    p2 = p2.replace('\u00A0', '').replace('\u2009','').replace('\u202F','')
    return p2

p = argparse.ArgumentParser()
p.add_argument('--host', required=True)
p.add_argument('--port', type=int, default=993)
p.add_argument('--user', required=True)
p.add_argument('--password', required=True)
p.add_argument('--tfa', choices=['off','app_password'], default='off')
a = p.parse_args()

pwd = a.password
if a.tfa == 'app_password':
    pwd_sanitized = sanitize_app_password(pwd)
    if pwd_sanitized != pwd:
        print('sanitized: whitespace removed -> len', len(pwd_sanitized))
    pwd = pwd_sanitized

print('--- INPUT DEBUG ---')
print('user non-ascii:', has_non_ascii(a.user))
print('pass non-ascii:', has_non_ascii(pwd))
print('pass len :', len(pwd))

use_bytes = has_non_ascii(a.user) or has_non_ascii(pwd)

print('--- TRY LOGIN ---')
try:
    M = imaplib.IMAP4_SSL(a.host, a.port)
    if use_bytes:
        typ, data = M.login(a.user.encode('utf-8'), pwd.encode('utf-8'))
        print('(bytes literal login)')
    else:
        try:
            typ, data = M.login(a.user, pwd)
        except (UnicodeEncodeError, TypeError):
            typ, data = M.login(a.user.encode('utf-8'), pwd.encode('utf-8'))
            print('(fallback to bytes literal)')
    print('LOGIN:', typ, data)
    typ, boxes = M.list()
    print('LIST:', typ, 'count:', 0 if boxes is None else len(boxes))
    M.logout()
except imaplib.IMAP4.error as e:
    print('IMAP4.error:', e); sys.exit(2)
except Exception as e:
    print('Unexpected:', e); sys.exit(3)
"@

$tmp = Join-Path $env:TEMP "imap_test.py"
$code | Set-Content -Encoding UTF8 $tmp
& $py $tmp --host $HostName --port $Port --user $UserName --password $Plain --tfa $TwoFactor
