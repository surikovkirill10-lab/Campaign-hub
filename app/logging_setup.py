import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
APP_LOG = LOG_DIR / "app.log"

def setup_logging(level=logging.DEBUG) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    root = logging.getLogger()
    root.setLevel(level)

    # убрать старые хендлеры (важно при --reload)
    for h in list(root.handlers):
        root.removeHandler(h)

    fh = RotatingFileHandler(APP_LOG, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt); fh.setLevel(level); root.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt); ch.setLevel(level); root.addHandler(ch)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        logging.getLogger(name).setLevel(level)
