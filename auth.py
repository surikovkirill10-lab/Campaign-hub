import hashlib
import sqlite3
from pathlib import Path
from typing import Generator, Literal

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status, FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

DATABASE_PATH = "campaign_hub.db"
SECRET_KEY = "change-me-please"

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

router = APIRouter()


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Dependency: даёт подключение к SQLite и аккуратно его закрывает."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def verify_password(plain: str, stored_hash: str) -> bool:
    """Проверка пароля через SHA-256 (совпадает с тем, что мы положили в БД)."""
    if stored_hash is None:
        return False
    return hashlib.sha256(plain.encode("utf-8")).hexdigest() == stored_hash


def get_current_user(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
):
    """
    Возвращает текущего пользователя как dict:
    {
        "id": ...,
        "login": ...,
        "role_code": ...,
        "role_name": ...,
        ...
    }
    Если не залогинен или юзер неактивен — кидает 302 на /auth/login.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        # не залогинен — на логин
        raise HTTPException(
            status_code=status.HTTP_302_FOUND,
            headers={"Location": "/auth/login"},
        )

    cur = db.execute(
        """
        SELECT u.*, r.code AS role_code, r.name AS role_name
        FROM users u
        JOIN roles r ON u.role_id = r.id
        WHERE u.id = ? AND u.is_active = 1
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        # юзер удалён/заблокирован — чистим и на логин
        request.session.clear()
        raise HTTPException(
            status_code=status.HTTP_302_FOUND,
            headers={"Location": "/auth/login"},
        )

    # приводим sqlite3.Row к обычному dict, чтобы
    # и Jinja, и новый код (publisher_widget) могли спокойно работать
    user = dict(row)
    return user


ModuleAction = Literal["view", "edit"]


def require_module(module_code: str, action: ModuleAction = "view"):
    """
    Dependency‑обёртка: проверяет доступ текущего пользователя к модулю.

    Пример использования в main.py:

        from fastapi import Depends
        from auth import require_module

        @app.get("/campaigns")
        def campaigns_list(user = Depends(require_module("campaigns", "view"))):
            ...

    module_code должен быть одним из:
        'campaigns', 'directories', 'bookings', 'logs', 'settings', 'dataflow', 'users'
    """

    def dependency(
        user=Depends(get_current_user),
        db: sqlite3.Connection = Depends(get_db),
    ):
        # Админ — полный доступ
        if user["role_code"] == "admin":
            return user

        cur = db.execute(
            """
            SELECT can_view, can_edit
            FROM role_module_permissions
            WHERE role_id = ? AND module_code = ?
            """,
            (user["role_id"], module_code),
        )
        perm = cur.fetchone()
        if perm is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к модулю",
            )

        if action == "view" and not perm["can_view"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет прав на просмотр",
            )
        if action == "edit" and not perm["can_edit"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет прав на редактирование",
            )

        return user

    return dependency


@router.get("/auth/login", response_class=HTMLResponse)
def login_form(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
):
    """
    Страница логина.
    Если в сессии есть ВАЛИДНЫЙ пользователь — шлём на /campaigns.
    Если пользователь уже удалён/выключен — чистим сессию и показываем форму.
    """
    user_id = request.session.get("user_id")

    if user_id:
        cur = db.execute(
            "SELECT 1 FROM users WHERE id = ? AND is_active = 1",
            (user_id,),
        )
        if cur.fetchone():
            # пользователь живой — уже залогинен
            return RedirectResponse(
                url="/campaigns",
                status_code=status.HTTP_303_SEE_OTHER,
            )
        else:
            # битый user_id в сессии — чистим
            request.session.clear()

    return templates.TemplateResponse(
        "auth/login.html",
        {"request": request, "error": None},
    )


@router.post("/auth/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    login: str = Form(...),
    password: str = Form(...),
    db: sqlite3.Connection = Depends(get_db),
):
    """Обработка формы логина."""
    cur = db.execute(
        """
        SELECT u.*, r.code AS role_code
        FROM users u
        JOIN roles r ON u.role_id = r.id
        WHERE u.login = ? AND u.is_active = 1
        """,
        (login,),
    )
    user = cur.fetchone()

    if not user or not verify_password(password, user["password_hash"]):
        # Неверный логин/пароль — просто возвращаем форму с ошибкой
        return templates.TemplateResponse(
            "auth/login.html",
            {
                "request": request,
                "error": "Неверный логин или пароль",
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    # Всё ок — запоминаем пользователя в сессии и ведём на кампании
    request.session["user_id"] = user["id"]
    return RedirectResponse(
        url="/campaigns",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/auth/logout")
def logout(request: Request):
    """Выход: чистим сессию и ведём на логин."""
    request.session.clear()
    return RedirectResponse(
        url="/auth/login",
        status_code=status.HTTP_303_SEE_OTHER,
    )


def setup_auth(app: FastAPI):
    """
    Подключение авторизации к существующему FastAPI‑приложению.

    В main.py (после app = FastAPI()):

        from auth import setup_auth
        app = FastAPI()
        setup_auth(app)
    """
    app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
    app.include_router(router)
