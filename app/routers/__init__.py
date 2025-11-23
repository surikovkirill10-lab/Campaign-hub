
"""Collect FastAPI routers for inclusion in the application."""

from .campaigns import router as campaigns_router
from .directory import router as directory_router
from .files import router as files_router
from .data_flow import router as data_flow_router
from .cats_export import router as cats_export_router
from .debug import router as debug_router
from .verifier import router as verifier_router
from .bookings import router as bookings_router

__all__ = [
    "campaigns_router",
    "directory_router",
    "files_router",
    "data_flow_router",
    "cats_export_router",
    "debug_router",
    "verifier_router",
    "bookings_router",
]

# --- bookings router ---
try:
    from app.routers.bookings import router as bookings_router
    app.include_router(bookings_router)
except Exception:
    pass