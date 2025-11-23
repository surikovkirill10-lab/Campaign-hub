"""Application package for Campaign Hub."""

# --- postclick blueprint ---
try:
    from .blueprints.postclick import bp as postclick_bp
    app.register_blueprint(postclick_bp)
except Exception as e:
    # не валим апп, если среда без файла/БД
    pass
# --- campaigns_add_patch router ---
try:
    from app.routers.campaigns_add_patch import router as campaigns_add_patch_router
    app.include_router(campaigns_add_patch_router)
except Exception:
    pass
# --- debug routes router ---
try:
    from app.routers.debug_routes import router as debug_router
    app.include_router(debug_router)
except Exception:
    pass

# --- admin users router ---
try:
    from app.routers.admin_users import router as admin_users_router
    app.include_router(admin_users_router)
except Exception:
    pass
