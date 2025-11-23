# app/routers/diag_bytes.py
from fastapi import APIRouter
from app.services.config_store import get_effective_imap_config

router = APIRouter()

def _ords(s: str):
    return [ord(ch) for ch in s]

@router.get("/debug/imap_bytes")
def debug_imap_bytes():
    cfg = get_effective_imap_config("config.yaml")
    u = (cfg.get("user") or "")
    p = (cfg.get("password") or "")
    return {
        "user_len": len(u),
        "user_ords": _ords(u),
        "pass_len": len(p),
        "pass_ords": _ords(p),  # только числа; реальный пароль не раскрываем
        "two_factor": cfg.get("two_factor"),
    }
