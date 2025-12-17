import hashlib
import secrets
import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime

from auth import get_db, require_module  # берём из auth.py

templates = Jinja2Templates(directory="app/templates")

router = APIRouter(prefix="/admin/publishers", tags=["admin-publishers"])


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


# ---------- Список издателей ----------

@router.get("/", response_class=HTMLResponse, name="admin_publishers_list")
def admin_publishers_list(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
    current_user: Any = Depends(require_module("users", "edit")),
):
    cur = db.execute(
        """
        SELECT
            p.id               AS publisher_id,
            p.name             AS publisher_name,
            p.contact_email    AS publisher_email,
            p.is_active        AS publisher_is_active,
            u.id               AS user_id,
            u.login            AS user_login,
            u.is_active        AS user_is_active
        FROM publishers p
        JOIN users u ON p.user_id = u.id
        ORDER BY p.id
        """
    )
    publishers = [dict(row) for row in cur.fetchall()]

    return templates.TemplateResponse(
        "admin/publishers_list.html",
        {
            "request": request,
            "publishers": publishers,
        },
    )


# ---------- Создать издателя ----------

@router.get("/new", response_class=HTMLResponse, name="admin_new_publisher")
def admin_new_publisher_form(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
    current_user: Any = Depends(require_module("users", "edit")),
):
    # Проверим, что роль publisher существует
    cur = db.execute("SELECT id FROM roles WHERE code = 'publisher'")
    role = cur.fetchone()

    if not role:
        return templates.TemplateResponse(
            "admin/publisher_form.html",
            {
                "request": request,
                "error": "Сначала создайте роль 'publisher' в таблице roles (code='publisher')",
                "login": "",
                "name": "",
                "contact_email": "",
            },
        )

    return templates.TemplateResponse(
        "admin/publisher_form.html",
        {
            "request": request,
            "error": None,
            "login": "",
            "name": "",
            "contact_email": "",
        },
    )


@router.post("/new", response_class=HTMLResponse, name="admin_create_publisher")
def admin_create_publisher(
    request: Request,
    login: str = Form(...),
    password: str = Form(...),
    name: str = Form(...),
    contact_email: str = Form(""),
    db: sqlite3.Connection = Depends(get_db),
    current_user: Any = Depends(require_module("users", "edit")),
):
    login = login.strip()
    name = name.strip()
    contact_email = contact_email.strip()

    # роль publisher
    cur = db.execute("SELECT id FROM roles WHERE code = 'publisher'")
    role = cur.fetchone()
    if not role:
        return templates.TemplateResponse(
            "admin/publisher_form.html",
            {
                "request": request,
                "error": "Сначала создайте роль 'publisher' в таблице roles (code='publisher')",
                "login": login,
                "name": name,
                "contact_email": contact_email,
            },
        )
    role_id = role["id"]

    # Проверка логина
    cur = db.execute("SELECT 1 FROM users WHERE login = ?", (login,))
    if cur.fetchone():
        return templates.TemplateResponse(
            "admin/publisher_form.html",
            {
                "request": request,
                "error": "Пользователь с таким логином уже существует",
                "login": login,
                "name": name,
                "contact_email": contact_email,
            },
            status_code=400,
        )

    if not password:
        return templates.TemplateResponse(
            "admin/publisher_form.html",
            {
                "request": request,
                "error": "Пароль не может быть пустым",
                "login": login,
                "name": name,
                "contact_email": contact_email,
            },
            status_code=400,
        )

    password_hash = hash_password(password)

    # 1) создаём user с ролью publisher
    cur = db.execute(
        """
        INSERT INTO users (login, password_hash, password_plain, is_active, role_id)
        VALUES (?, ?, ?, 1, ?)
        """,
        (login, password_hash, password, role_id),
    )
    user_id = cur.lastrowid

    # 2) создаём publisher, не забывая created_at / updated_at
    db.execute(
        """
        INSERT INTO publishers (name, user_id, contact_email, is_active, created_at, updated_at)
        VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))
        """,
        (name, user_id, contact_email or None),
    )

    db.commit()

    return RedirectResponse(
        url="/admin/publishers",
        status_code=303,
    )


# ---------- Сайты издателя ----------

@router.get(
    "/{publisher_id}/sites",
    response_class=HTMLResponse,
    name="admin_publisher_sites",
)
def admin_publisher_sites(
    publisher_id: int,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
    current_user: Any = Depends(require_module("users", "edit")),
):
    cur = db.execute("SELECT * FROM publishers WHERE id = ?", (publisher_id,))
    pub = cur.fetchone()
    if not pub:
        raise HTTPException(status_code=404, detail="Publisher not found")

    cur = db.execute(
        """
        SELECT id, name, domain, public_token, is_active
        FROM publisher_sites
        WHERE publisher_id = ?
        ORDER BY id
        """,
        (publisher_id,),
    )
    sites = [dict(row) for row in cur.fetchall()]

    return templates.TemplateResponse(
        "admin/publisher_sites.html",
        {
            "request": request,
            "publisher": dict(pub),
            "sites": sites,
        },
    )


@router.get(
    "/{publisher_id}/sites/new",
    response_class=HTMLResponse,
    name="admin_new_publisher_site",
)
def admin_new_publisher_site_form(
    publisher_id: int,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
    current_user: Any = Depends(require_module("users", "edit")),
):
    cur = db.execute("SELECT * FROM publishers WHERE id = ?", (publisher_id,))
    pub = cur.fetchone()
    if not pub:
        raise HTTPException(status_code=404, detail="Publisher not found")

    return templates.TemplateResponse(
        "admin/publisher_site_form.html",
        {
            "request": request,
            "publisher": dict(pub),
            "error": None,
            "name": "",
            "domain": "",
        },
    )


@router.post(
    "/{publisher_id}/sites/new",
    response_class=HTMLResponse,
    name="admin_create_publisher_site",
)
def admin_create_publisher_site(
    publisher_id: int,
    request: Request,
    name: str = Form(...),
    domain: str = Form(...),
    db: sqlite3.Connection = Depends(get_db),
    current_user: Any = Depends(require_module("users", "edit")),
):
    cur = db.execute("SELECT * FROM publishers WHERE id = ?", (publisher_id,))
    pub = cur.fetchone()
    if not pub:
        raise HTTPException(status_code=404, detail="Publisher not found")

    name = name.strip()
    domain = domain.strip()

    if not name or not domain:
        return templates.TemplateResponse(
            "admin/publisher_site_form.html",
            {
                "request": request,
                "publisher": dict(pub),
                "error": "Имя и домен обязательны",
                "name": name,
                "domain": domain,
            },
            status_code=400,
        )

    public_token = secrets.token_urlsafe(8)
    ts = now_ts()  # <--- время для created_at

    db.execute(
        """
        INSERT INTO publisher_sites (publisher_id, name, domain, public_token, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, ?)
        """,
        (publisher_id, name, domain, public_token, ts),
    )
    db.commit()

    return RedirectResponse(
        url=f"/admin/publishers/{publisher_id}/sites",
        status_code=303,
    )

def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")