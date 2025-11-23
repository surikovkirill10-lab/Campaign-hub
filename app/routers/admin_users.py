import hashlib
import sqlite3
from typing import Dict, Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from auth import get_db, require_module, templates as auth_templates

router = APIRouter(prefix="/admin", tags=["admin"])

# используем тот же templates, что и в auth.py (app/templates/…)
templates = auth_templates


def load_admin_data(db: sqlite3.Connection) -> Dict[str, Any]:
    """Читаем из БД роли, модули, пользователей и матрицу прав."""
    data: Dict[str, Any] = {}

    # Роли
    cur = db.execute("SELECT id, code, name FROM roles ORDER BY id")
    data["roles"] = [dict(r) for r in cur.fetchall()]

    # Модули (страницы/разделы)
    cur = db.execute("SELECT code, name FROM modules ORDER BY code")
    data["modules"] = [dict(m) for m in cur.fetchall()]

    # Пользователи
    cur = db.execute(
        """
        SELECT u.id,
               u.login,
               u.password_plain,
               u.role_id,
               u.is_active,
               u.created_at,
               r.code AS role_code,
               r.name AS role_name
        FROM users u
        JOIN roles r ON u.role_id = r.id
        ORDER BY u.id
        """
    )
    data["users"] = [dict(u) for u in cur.fetchall()]

    # Права ролей по модулям
    cur = db.execute(
        "SELECT role_id, module_code, can_view, can_edit FROM role_module_permissions"
    )
    perms: Dict[int, Dict[str, Dict[str, bool]]] = {}
    for row in cur.fetchall():
        role_id = row["role_id"]
        module_code = row["module_code"]
        perms.setdefault(role_id, {})[module_code] = {
            "can_view": bool(row["can_view"]),
            "can_edit": bool(row["can_edit"]),
        }

    data["perms"] = perms
    return data


# === СТРАНИЦА АДМИНКИ: ПОЛЬЗОВАТЕЛИ + ПРАВА ===
@router.get("/users", response_class=HTMLResponse)
def admin_users_page(
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
    current_user=Depends(require_module("users", "view")),
):
    ctx = load_admin_data(db)
    ctx.update(
        {
            "request": request,
            "current_user": current_user,
        }
    )
    return templates.TemplateResponse("admin/users.html", ctx)


# === СОЗДАНИЕ ПОЛЬЗОВАТЕЛЯ ===
@router.post("/users", response_class=HTMLResponse)
def create_user(
    request: Request,
    login: str = Form(...),
    password: str = Form(...),
    role_id: int = Form(...),
    db: sqlite3.Connection = Depends(get_db),
    current_user=Depends(require_module("users", "edit")),
):
    password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    try:
        db.execute(
            """
            INSERT INTO users (login, password_hash, password_plain, role_id, is_active)
            VALUES (?, ?, ?, ?, 1)
            """,
            (login, password_hash, password, role_id),
        )
        db.commit()
    except sqlite3.IntegrityError:
        # логин уже существует — молча игнорируем и возвращаемся на список
        pass

    return RedirectResponse(url="/admin/users", status_code=303)


# === СМЕНА РОЛИ ПОЛЬЗОВАТЕЛЯ ===
@router.post("/users/{user_id}/role")
def change_user_role(
    user_id: int,
    role_id: int = Form(...),
    db: sqlite3.Connection = Depends(get_db),
    current_user=Depends(require_module("users", "edit")),
):
    db.execute("UPDATE users SET role_id = ? WHERE id = ?", (role_id, user_id))
    db.commit()
    return RedirectResponse(url="/admin/users", status_code=303)


# === ВКЛ/ВЫКЛ ПОЛЬЗОВАТЕЛЯ ===
@router.post("/users/{user_id}/toggle-active")
def toggle_user_active(
    user_id: int,
    db: sqlite3.Connection = Depends(get_db),
    current_user=Depends(require_module("users", "edit")),
):
    cur = db.execute("SELECT is_active FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    new_value = 0 if row["is_active"] else 1
    db.execute("UPDATE users SET is_active = ? WHERE id = ?", (new_value, user_id))
    db.commit()
    return RedirectResponse(url="/admin/users", status_code=303)


# === СМЕНА ПАРОЛЯ ===
@router.post("/users/{user_id}/password")
def change_user_password(
    user_id: int,
    password: str = Form(...),
    db: sqlite3.Connection = Depends(get_db),
    current_user=Depends(require_module("users", "edit")),
):
    password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    db.execute(
        "UPDATE users SET password_hash = ?, password_plain = ? WHERE id = ?",
        (password_hash, password, user_id),
    )
    db.commit()
    return RedirectResponse(url="/admin/users", status_code=303)


# === ПЕРЕКЛЮЧЕНИЕ ПРАВ ПО МОДУЛЯМ (view / edit) ===
@router.post("/permissions/{role_id}/{module_code}")
def toggle_permission(
    role_id: int,
    module_code: str,
    field: str = Form(...),  # 'view' или 'edit'
    db: sqlite3.Connection = Depends(get_db),
    current_user=Depends(require_module("users", "edit")),
):
    if field not in ("view", "edit"):
        raise HTTPException(status_code=400, detail="Invalid field")

    cur = db.execute(
        """
        SELECT can_view, can_edit
        FROM role_module_permissions
        WHERE role_id = ? AND module_code = ?
        """,
        (role_id, module_code),
    )
    row = cur.fetchone()

    if row is None:
        # если записи не было — создаём с 0/1 только по нужному флагу
        can_view = 1 if field == "view" else 0
        can_edit = 1 if field == "edit" else 0
        db.execute(
            """
            INSERT INTO role_module_permissions (role_id, module_code, can_view, can_edit)
            VALUES (?, ?, ?, ?)
            """,
            (role_id, module_code, can_view, can_edit),
        )
    else:
        can_view = row["can_view"]
        can_edit = row["can_edit"]

        if field == "view":
            can_view = 0 if can_view else 1
        else:
            can_edit = 0 if can_edit else 1

        db.execute(
            """
            UPDATE role_module_permissions
            SET can_view = ?, can_edit = ?
            WHERE role_id = ? AND module_code = ?
            """,
            (can_view, can_edit, role_id, module_code),
        )

    db.commit()
    # htmx сам уже переключил состояние чекбокса в DOM, разметку перерисовывать не нужно
    return Response(status_code=204)
