
from fastapi import FastAPI, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.logging_setup import setup_logging
from app.database import engine
from app import models
from app.routers import widget_public, publisher_widget
from app.routers import publishers_admin


# агрегированные роутеры
from app.routers import (
    campaigns_router,
    directory_router,
    files_router,
    cats_export_router,
    data_flow_router,
    verifier_router,
    sales_router,
)

# отдельные роутеры
from app.routers.settings import router as settings_router, imap_folders_partial
from app.routers.diag_bytes import router as diag_bytes_router
from app.routers.debug import router as debug_router
from app.routers.cats_front import router as cats_front_router
from app.routers.imap_ping import router as imap_ping_router
from app.routers.logs import router as logs_router
from app.routers.bookings import router as bookings_router
from app.routers.admin_users import router as admin_users_router



from auth import setup_auth, require_module


# === ЛОГИРОВАНИЕ ===
setup_logging()

# === ПРИЛОЖЕНИЕ ===
app = FastAPI(title="Campaign Hub", version="0.2.0")

# === АВТОРИЗАЦИЯ / СЕССИИ ===
# Подключаем SessionMiddleware и /auth/login, /auth/logout из auth.py
setup_auth(app)

# === STATIC ===
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# === БАЗА / МИГРАЦИИ ===
models.Base.metadata.create_all(bind=engine)


# === ROOT ===
@app.get("/")
def root():
    # Можно поменять на "/campaigns", если удобнее
    return RedirectResponse(url="/settings")


# === РОУТЕРЫ С РОЛЯМИ ПО МОДУЛЯМ ===
# Здесь через dependencies вешаем проверку require_module(...),
# чтобы к каждому модулю был доступ только у нужных ролей.


# --- Campaigns ---
#    Админ / Аккаунт / Трафик (согласно правам в БД)
app.include_router(
    campaigns_router,
    dependencies=[Depends(require_module("campaigns", "view"))],
)


# --- Directories ---
#    Админ / Аккаунт / Трафик
app.include_router(
    directory_router,
    dependencies=[Depends(require_module("directories", "view"))],
)


# --- Bookings ---
#    Админ / Аккаунт / Трафик / Сейлз / (клиент/партнёр, когда добавишь)
app.include_router(
    bookings_router,
    dependencies=[Depends(require_module("bookings", "view"))],
)


# --- Settings ---
#    По текущей матрице — только Админ
app.include_router(
    settings_router,
    dependencies=[Depends(require_module("settings", "view"))],
)


# --- Logs ---
#    Только Админ (в правах у остальных can_view=0)
app.include_router(
    logs_router,
    dependencies=[Depends(require_module("logs", "view"))],
)

# --- Admin_users ---
#    Только Админ (в правах у остальных can_view=0)
app.include_router(
    admin_users_router, 
    dependencies=[Depends(require_module("logs", "view"))],
    )

# --- Data Flow ---
#    Только Админ (модуль "dataflow")
app.include_router(
    data_flow_router,
    dependencies=[Depends(require_module("dataflow", "view"))],
)


# === СЕРВИСНЫЕ/ТЕХНИЧЕСКИЕ РОУТЕРЫ ===
# Ниже — моя разумная раскладка по модулям, чтобы они тоже были за логином.
# Если что-то должно быть доступно не только админу — просто поменяй module_code
# на "campaigns" / "bookings" / "directories" и т.п.

# Файлы — логично отнести к кампаниям (Аккаунт/Трафик/Админ)
app.include_router(
    files_router,
    dependencies=[Depends(require_module("campaigns", "view"))],
)

# Экспорт Cats — тоже рядом с кампаниями
app.include_router(
    cats_export_router,
    dependencies=[Depends(require_module("campaigns", "view"))],
)

# Front по Cats — туда же
app.include_router(
    cats_front_router,
    dependencies=[Depends(require_module("campaigns", "view"))],
)

# Диагностика байтов — только админ (через модуль settings)
app.include_router(
    diag_bytes_router,
    dependencies=[Depends(require_module("settings", "view"))],
)

# Debug‑роутер — тоже только админ
app.include_router(
    debug_router,
    dependencies=[Depends(require_module("settings", "view"))],
)

# IMAP‑ping — тех. история, тоже под settings
app.include_router(
    imap_ping_router,
    dependencies=[Depends(require_module("settings", "view"))],
)

# Verifier — условно тех. модуль, кладём под settings
app.include_router(
    verifier_router,
    dependencies=[Depends(require_module("settings", "view"))],
)

# --- Sales ---
app.include_router(
    sales_router,
    dependencies=[Depends(require_module("sales", "view"))],
)

# === ОТДЕЛЬНЫЕ PATH'ы ДЛЯ IMAP FOLDERS ===
# Делаем их тоже частью модуля "settings", чтобы они не были открыты наружу.
app.add_api_route(
    "/imap_folders",
    imap_folders_partial,
    name="imap_folders_partial_root",
    response_class=HTMLResponse,
    dependencies=[Depends(require_module("settings", "view"))],
)

app.add_api_route(
    "/settings/imap_folders",
    imap_folders_partial,
    name="imap_folders_partial_alias",
    response_class=HTMLResponse,
    dependencies=[Depends(require_module("settings", "view"))],
)
app.include_router(widget_public.router)
app.include_router(publisher_widget.router)
app.include_router(publishers_admin.router)

# === ЛОКАЛЬНЫЙ ЗАПУСК (по желанию) ===
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # при желании ограничишь своими доменами
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)