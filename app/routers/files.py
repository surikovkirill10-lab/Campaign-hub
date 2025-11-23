"""Routes for the processed files list."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy.orm import Session

from ..database import get_db
from ..crud import list_files

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))


@router.get("/files", response_class=HTMLResponse)
def view_files(request: Request, db: Session = Depends(get_db)):
    files = list_files(db, limit=100)
    return templates.TemplateResponse("files.html", {"request": request, "files": files})
