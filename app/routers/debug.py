# app/routers/debug.py
from fastapi import APIRouter
from app.services.config_store import get_effective_imap_config, get_effective_system_config

router = APIRouter()

@router.get("/debug/imap")
def debug_imap():
    cfg = get_effective_imap_config("config.yaml")
    u = cfg.get("user") or ""
    p = cfg.get("password") or ""
    return {
        "user_repr": repr(u),
        "user_len": len(u),
        "pass_len": len(p),
        "two_factor": cfg.get("two_factor"),
        "host": cfg.get("host"),
        "port": cfg.get("port"),
    }

@router.get("/debug/system")
def debug_system():
    sys = get_effective_system_config("config.yaml")
    auth = sys.get("auth") or {}
    return {
        "base_url": sys.get("base_url"),
        "connect_url": sys.get("connect_url"),
        "auth": {
            "type": auth.get("type"),
            "username": auth.get("username"),
            "password_len": len(auth.get("password") or ""),
            "has_form": bool((auth.get("form") or {})),
            "form": (auth.get("form") or {})
        },
    }

from pathlib import Path
import os

@router.get("/debug/fs")
def debug_fs():
    cwd = Path.cwd()
    here = Path(__file__).resolve()
    root = here.parents[2]  # корень проекта: .../app/routers -> app -> <root>
    p1 = root / "data" / "cats" / "13995" / "latest_normalized.csv"
    p2 = cwd  / "data" / "cats" / "13995" / "latest_normalized.csv"
    def ls(p):
        try:
            if p.exists():
                if p.is_dir():
                    return [str(x) for x in p.iterdir()]
                else:
                    return f"{p} [FILE]"
            return f"{p} [MISSING]"
        except Exception as e:
            return f"ERR: {e}"
    cats_root1 = root / "data" / "cats"
    cats_root2 = cwd  / "data" / "cats"
    return {
        "cwd": str(cwd),
        "file_here": str(here),
        "root": str(root),
        "check_p1": str(p1), "p1_exists": p1.exists(),
        "check_p2": str(p2), "p2_exists": p2.exists(),
        "ls_root_data_cats_from_root": ls(cats_root1),
        "ls_root_data_cats_from_cwd":  ls(cats_root2),
    }
