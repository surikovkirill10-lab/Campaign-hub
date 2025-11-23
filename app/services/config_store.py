from pathlib import Path
from typing import Any, Dict
import os, yaml

CONFIG_PATH = Path("config.yaml")

def load_raw() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            with CONFIG_PATH.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}
    return {}

def save_raw(data: Dict[str, Any]) -> None:
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data or {}, f, allow_unicode=True, sort_keys=False)

def get_effective_imap_config(path: str | None = None) -> Dict[str, Any]:
    raw = load_raw()
    imap = (raw.get("imap") or {})
    # ENV override (пустые значения не перетирают YAML)
    env_user = os.getenv("IMAP__USER")
    env_pass = os.getenv("IMAP__PASSWORD")
    def _prefer_env(yaml_val: str | None, env_val: str | None):
        if env_val is None: return yaml_val
        if isinstance(env_val, str) and len(env_val.strip()) == 0: return yaml_val
        return env_val
    eff = {
        "host": imap.get("host"),
        "port": imap.get("port"),
        "user": _prefer_env(imap.get("user"), env_user),
        "password": _prefer_env(imap.get("password"), env_pass),
        "mailbox": imap.get("mailbox") or "INBOX",
        "two_factor": imap.get("two_factor") or "none",
    }
    # порт числом, если пришёл строкой
    try:
        if isinstance(eff["port"], str): eff["port"] = int(eff["port"])
    except Exception:
        pass
    return eff

def get_effective_system_config(path: str | None = None) -> Dict[str, Any]:
    raw = load_raw()
    system = (raw.get("system") or {})
    auth = (system.get("auth") or {})

    # ENV overrides для токена (пример)
    env_token = os.getenv("SYSTEM__AUTH__TOKEN")
    token = auth.get("token")
    if env_token is not None and len(env_token.strip()) > 0:
        token = env_token

    # ВАЖНО: вернуть form как есть (или пустой dict)
    return {
        "base_url": system.get("base_url"),
        "connect_url": system.get("connect_url") or system.get("base_url"),
        "auth": {
            "type": (auth.get("type") or "none"),
            "username": auth.get("username"),
            "password": auth.get("password"),
            "token": token,
            "headers": (auth.get("headers") or {}),
            "cookies": (auth.get("cookies") or {}),
            "form": (auth.get("form") or {}),   # <-- ключевой момент
        },
    }
