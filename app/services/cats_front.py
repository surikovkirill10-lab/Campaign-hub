# app/services/cats_front.py
from __future__ import annotations
import logging, time, re
from typing import Dict, Any
import requests
from bs4 import BeautifulSoup

from urllib.parse import urlencode
from app.services.config_store import get_effective_system_config

logger = logging.getLogger("app")

def cats_front_ping(timeout: float = 15.0, verbose: bool = False) -> Dict[str, Any]:
    """
    Пытаемся залогиниться через обычную HTML-форму (без headless браузера).
    Ожидается конфиг:
    system.auth.type = "form"
    system.auth.username / password
    system.auth.form:
      login_url: "https://catsnetwork.example/login"
      username_field: "email"
      password_field: "password"
      submit_name: "submit"       # если нужен
      success_check: {"url_contains": "/dashboard"}  # или {"css": ".dashboard"} / {"text_contains": "Dashboard"}
      csrf:
        find: {"css": "input[name=csrf_token]"}      # как найти токен
        field_name: "csrf_token"                     # имя поля в POST
    """
    cfg = get_effective_system_config("config.yaml")
    sys = cfg
    auth = (sys.get("auth") or {})
    if (auth.get("type") or "").lower() != "form":
        return {"ok": False, "error": "auth.type != form"}

    form = auth.get("form") or {}
    login_url = form.get("login_url")
    uname_field = form.get("username_field") or "username"
    pwd_field   = form.get("password_field") or "password"
    submit_name = form.get("submit_name")      # опционально
    extra_fields = form.get("extra_fields") or {}
    success     = form.get("success_check") or {}
    csrf_conf   = form.get("csrf") or {}

    user = auth.get("username") or ""
    pwd  = auth.get("password") or ""
    if not (login_url and user and pwd):
        return {"ok": False, "error": "Missing login_url/username/password in config"}

    s = requests.Session()
    t0 = time.perf_counter()
    try:
        # 1) GET логин-страницы — вытаскиваем CSRF (если описан)
        r1 = s.get(login_url, timeout=timeout)
        r1.raise_for_status()
        payload = {uname_field: user, pwd_field: pwd}
        # optional: добавить submit_name как name=value, если указан
        if submit_name:
            payload[submit_name] = submit_name
        # optional: дополнительные скрытые/константные поля из YAML
        if isinstance(extra_fields, dict) and extra_fields:
            payload.update(extra_fields)
        # --- debug (masked) ---
        _debug_payload_masked = dict(payload)
        try:
            _pv = _debug_payload_masked.get(pwd_field, "")
            _debug_payload_masked[pwd_field] = "*" * len(_pv or "")
        except Exception:
            pass
        _debug_urlencoded = urlencode(_debug_payload_masked, doseq=True)
        logger.info("Cats FRONT payload (masked): %s", _debug_urlencoded)
        # CSRF:
        token_val = None
        if csrf_conf:
            find = csrf_conf.get("find") or {}
            field_name = csrf_conf.get("field_name") or "csrf_token"
            if "css" in find:
                soup = BeautifulSoup(r1.text, "html.parser")
                node = soup.select_one(find["css"])
                if node and node.get("value"):
                    token_val = node.get("value")
            elif "regex" in find:
                m = re.search(find["regex"], r1.text)
                if m:
                    token_val = m.group(1)
            if token_val:
                payload[field_name] = token_val

        if submit_name:
            payload[submit_name] = "1"

        # 2) POST формы. Если action не указан — шлём на ту же страницу.
        post_url = login_url
        if "action_css" in form:
            soup = BeautifulSoup(r1.text, "html.parser")
            f = soup.select_one(form["action_css"])
            if f and f.get("action"):
                post_url = requests.compat.urljoin(login_url, f.get("action"))

        r2 = s.post(post_url, data=payload, timeout=timeout, allow_redirects=True)
        elapsed = int((time.perf_counter()-t0)*1000)

        ok = False
        # Критерии успеха
        if "url_contains" in success:
            ok = success["url_contains"] in str(r2.url)
        elif "css" in success:
            soup2 = BeautifulSoup(r2.text, "html.parser")
            ok = bool(soup2.select_one(success["css"]))
        elif "text_contains" in success:
            ok = success["text_contains"] in (r2.text or "")

        logger.info("Cats FRONT login url=%s -> %s ms=%s ok=%s", login_url, r2.url, elapsed, ok)
        return {
            "ok": ok, "elapsed_ms": elapsed, "final_url": str(r2.url),
            "status_code": r2.status_code, "preview": (r2.text or "")[:200]
        }
    except requests.RequestException as e:
        elapsed = int((time.perf_counter()-t0)*1000)
        logger.warning("Cats FRONT login failed %s ms=%s", e, elapsed)
        return {"ok": False, "error": str(e), "elapsed_ms": elapsed}

def cats_front_preview(timeout: float = 15.0) -> Dict[str, Any]:
    """
    Build the exact payload that will be sent to the Cats login form (without sending it).
    Returns masked values (password hidden) and urlencoded preview.
    """
    cfg = get_effective_system_config("config.yaml")
    sys = cfg
    auth = (sys.get("auth") or {})
    if (auth.get("type") or "").lower() != "form":
        return {"ok": False, "error": "auth.type != form"}

    form = auth.get("form") or {}
    login_url = form.get("login_url")
    uname_field = form.get("username_field") or "username"
    pwd_field   = form.get("password_field") or "password"
    submit_name = form.get("submit_name")
    csrf_conf   = form.get("csrf") or {}
    extra_fields = form.get("extra_fields") or {}

    user = auth.get("username") or ""
    pwd  = auth.get("password") or ""
    if not (login_url and user and pwd):
        return {"ok": False, "error": "Missing login_url/username/password in config"}

    s = requests.Session()
    csrf_info = None
    # Попробуем подтянуть CSRF, если описан в YAML
    try:
        if csrf_conf:
            r1 = s.get(login_url, timeout=timeout)
            r1.raise_for_status()
            soup = BeautifulSoup(r1.text, "html.parser")
            if "hidden_input_name" in csrf_conf:
                nm = csrf_conf["hidden_input_name"]
                el = soup.select_one(f"input[name='{nm}']")
                if el and el.get("value"):
                    extra_fields[nm] = el.get("value")
                    csrf_info = {"hidden_input_name": nm, "value_len": len(el.get("value"))}
            elif "meta_name" in csrf_conf:
                nm = csrf_conf["meta_name"]
                m = soup.select_one(f"meta[name='{nm}']")
                if m and m.get("content"):
                    extra_fields[nm] = m.get("content")
                    csrf_info = {"meta_name": nm, "value_len": len(m.get("content"))}
    except Exception as e:
        csrf_info = {"error": str(e)}

    payload = {uname_field: user, pwd_field: pwd}
    if submit_name:
        payload[submit_name] = submit_name
    if isinstance(extra_fields, dict) and extra_fields:
        payload.update(extra_fields)

    masked = dict(payload)
    try:
        masked[pwd_field] = "*" * len(str(masked.get(pwd_field) or ""))
    except Exception:
        pass
    urlenc = urlencode(masked, doseq=True)

    return {
        "ok": True,
        "login_url": login_url,
        "username_field": uname_field,
        "password_field": pwd_field,
        "submit_name": submit_name,
        "payload_masked": masked,
        "payload_urlencoded_masked": urlenc,
        "csrf": csrf_info,
    }



