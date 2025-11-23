from __future__ import annotations
from typing import Optional, List
import logging
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.services.imap_utils import IMAPCompatClient as IMAPClient
from ..config import get_settings
from ..services import config_store

logger = logging.getLogger(__name__)
router = APIRouter(tags=["settings"])
templates = Jinja2Templates(directory="app/templates")

@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, ok: Optional[int] = None) -> HTMLResponse:
    s = get_settings()
    return templates.TemplateResponse("settings.html", {"request": request, "s": s, "saved": bool(ok)})

@router.post("/settings/save")
def settings_save(
    request: Request,
    system_base_url: str = Form(""),
    system_connect_url: str = Form(""),
    system_username: str = Form(""),
    system_password: str = Form(""),
    imap_host: str = Form("imap.yandex.ru"),
    imap_port: int = Form(993),
    imap_user: str = Form(""),
    imap_password: str = Form(""),
    imap_mailbox: str = Form("INBOX"),
    imap_two_factor: str = Form("none"),
):
    logger.info("Saving settings (System + IMAP)")
    config_store.upsert("system", {
        "base_url": system_base_url.strip(),
        "connect_url": system_connect_url.strip() or None,
        "auth": {"type": "basic", "username": system_username.strip() or None, "password": system_password.strip() or None}
    })
    config_store.upsert("imap", {
        "host": imap_host.strip(), "port": int(imap_port),
        "user": imap_user.strip(), "password": imap_password.strip(),
        "mailbox": imap_mailbox.strip() or "INBOX", "two_factor": imap_two_factor.strip()
    })
    return RedirectResponse(url="/settings?ok=1", status_code=303)

@router.get("/settings/imap/folders", response_class=HTMLResponse)
def list_imap_folders(request: Request) -> HTMLResponse:
    s = get_settings()
    folders: List[str] = []; error: Optional[str] = None
    try:
        with IMAPClient(s.imap.host, port=s.imap.port, ssl=True) as client:
            client.login(s.imap.user, s.imap.password)
            for flags, delim, name in client.list_folders():
                if isinstance(name, bytes): name = name.decode("utf-8", errors="ignore")
                folders.append(name)
        folders.sort(key=str.lower)
    except Exception as e:
        error = str(e); logger.exception("IMAP list folders failed")
    return templates.TemplateResponse("partials/imap_folders.html", {"request": request, "folders": folders, "error": error})
# --- BEGIN: IMAP folders partial (append) ---
from fastapi import Request
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
import logging

# Если в файле ещё нет templates — создадим локально
try:
    templates  # type: ignore
except NameError:
    templates = Jinja2Templates(directory="templates")

logger = logging.getLogger("app")

# ВНИМАНИЕ: этот роут регистрируется на существующий APIRouter `router`, который уже объявлен в settings.py.
# Если у вашего `router` задан prefix="/settings", итоговый путь будет /settings/imap_folders.
@router.get("/imap_folders", response_class=HTMLResponse, name="imap_folders_partial")
def imap_folders_partial(request: Request):
    """
    Partial со списком папок IMAP (через imaplib, не IMAPClient).
    - Пустые user/password => аккуратная ошибка в partial (без попытки логина)
    - two_factor == "app_password" => удаляем все пробелы/невидимые символы перед LOGIN
    - Байтовый fallback: если есть не-ASCII, логинимся через literal (UTF-8 bytes)
    - Retry на конкретный BAD Command syntax error
    """
    from app.services.config_store import get_effective_imap_config
    import imaplib, re

    logger = logging.getLogger("app")
    cfg = get_effective_imap_config("config.yaml")
    host = cfg.get("host")
    port = int(cfg.get("port") or 993)
    user = _sanitize_user_login(cfg.get("user") or "")
    password = (cfg.get("password") or "")
    two_factor = (cfg.get("two_factor") or "").lower()

    # Пустые учётки — аккуратно сообщаем пользователю
    if not user or not password:
        msg = "IMAP авторизация не настроена: заполните логин и пароль (Settings → IMAP)."
        logger.warning("IMAP folders: missing credentials (user_empty=%s, pass_empty=%s)", not bool(user), not bool(password))
        return templates.TemplateResponse(
            "partials/imap_folders.html",
            {"request": request, "folders": [], "error": msg, "hint_2fa": True},
        )

    # Пароль для входа
    pwd_for_login = _sanitize_password_general(password)
    if two_factor == "app_password":
        cleaned = _sanitize_app_password(password)
        if cleaned != password:
            logger.info("IMAP app_password: whitespace cleaned (len %d -> %d).", len(password), len(cleaned))
        pwd_for_login = cleaned

    def _login_and_list(u: str, p: str):
        M = imaplib.IMAP4_SSL(host, port)
        try:
            # Определяем, нужно ли отправлять literal (bytes)
            use_bytes = False
            try:
                u.encode("ascii"); p.encode("ascii")
            except UnicodeEncodeError:
                use_bytes = True

            if use_bytes:
                logger.debug("IMAP LOGIN (bytes) %s:%s as '%s' (pwd_len=%d, 2FA=%s)", host, port, u, len(p), two_factor)
                M.login(u.encode("utf-8"), p.encode("utf-8"))
            else:
                logger.debug("IMAP LOGIN %s:%s as '%s' (pwd_len=%d, 2FA=%s)", host, port, u, len(p), two_factor)
                try:
                    M.login(u, p)
                except (UnicodeEncodeError, TypeError):
                    M.login(u.encode("utf-8"), p.encode("utf-8"))

            typ, data = M.list()
            names: list[str] = []
            if data:
                for raw in data:  # bytes
                    line = raw.decode("utf-8", "replace")
                    # последнее "quoted" значение — имя папки
                    mq = re.findall(r'"([^"]+)"', line)
                    if mq:
                        names.append(mq[-1])
                    else:
                        # запасной путь: берём последний токен
                        parts = line.split(" ")
                        names.append(parts[-1].strip().strip('"'))
            return names
        finally:
            try:
                M.logout()
            except Exception:
                pass

    try:
        folders = sorted(_login_and_list(user, pwd_for_login), key=lambda s: s.lower())
        return templates.TemplateResponse(
            "partials/imap_folders.html",
            {"request": request, "folders": folders, "error": None, "hint_2fa": True},
        )
    except imaplib.IMAP4.error as e:
        msg = str(e)
        logger.warning("IMAP LOGIN failed: %s", msg)
        # На Яндексе это типичный кейс — попробуем «жёсткую» очистку + bytes ещё раз
        if "Command syntax error" in msg:
            try:
                logger.warning("IMAP BAD syntax: retry via imaplib literal login with full sanitize")
                folders = sorted(_login_and_list(user, _sanitize_app_password(password)), key=lambda s: s.lower())
                return templates.TemplateResponse(
                    "partials/imap_folders.html",
                    {"request": request, "folders": folders, "error": None, "hint_2fa": True},
                )
            except Exception as e2:
                logger.warning("IMAP retry after BAD failed: %s", e2)
        return templates.TemplateResponse(
            "partials/imap_folders.html",
            {"request": request, "folders": [], "error": f"IMAP ошибка входа: {e}", "hint_2fa": True},
        )
    except Exception as e:
        logger.exception("IMAP list_folders unexpected error")
        return templates.TemplateResponse(
            "partials/imap_folders.html",
            {"request": request, "folders": [], "error": f"Непредвиденная ошибка IMAP: {e}", "hint_2fa": True},
        )


