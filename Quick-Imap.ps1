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
import sys, argparse
from imapclient import IMAPClient

p = argparse.ArgumentParser()
p.add_argument('--host', required=True)
p.add_argument('--port', type=int, default=993)
p.add_argument('--user', required=True)
p.add_argument('--password', required=True)
p.add_argument('--tfa', choices=['off','app_password'], default='off')
a = p.parse_args()

pwd = a.password
if a.tfa == 'app_password':
    cleaned = ''.join(pwd.split())
    print(f'Password length: {len(pwd)} -> {len(cleaned)} after clean')
    pwd = cleaned
else:
    print(f'Password length: {len(pwd)}')

with IMAPClient(a.host, port=a.port, ssl=True) as c:
    c.login(a.user, pwd)
    print('LOGIN OK')
    boxes = c.list_folders()
    print('Folders:', 0 if boxes is None else len(boxes))
"@

$tmp = Join-Path $env:TEMP "imap_quick.py"
$code | Set-Content -Encoding UTF8 $tmp
& $py $tmp --host $HostName --port $Port --user $UserName --password $Plain --tfa $TwoFactor
