"""Routes for checking IMAP connectivity on demand.

This module defines a small endpoint that performs an IMAP login
attempt using the current configuration and returns an HTML snippet
containing a re‑usable "connect" button and the result of the
connection attempt.  The intent is to allow users to manually
trigger an IMAP connection test from the Settings page, rather than
attempting to connect automatically on every page load.

When the endpoint is invoked via HTMX, it replaces the contents of
the ``#imap-status`` div with a new snippet.  On each call the
function reads the effective IMAP configuration (respecting non‑empty
environment overrides) and, if credentials are present, attempts to
log in using the :class:`IMAPCompatClient`.  Success and failure are
reflected in the badge colour and accompanying text.  If credentials
are missing, a warning badge is displayed.
"""

from __future__ import annotations

import logging
import time
from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from app.services.config_store import get_effective_imap_config
from app.services.imap_utils import IMAPCompatClient


logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/imap/ping", response_class=HTMLResponse)
def imap_ping_endpoint() -> HTMLResponse:
    """
    Check connectivity to the configured IMAP server.

    Returns an HTML fragment containing a button to re‑invoke the check and
    a status indicator.  If the IMAP credentials are missing from the
    configuration the badge indicates this, otherwise the function
    attempts to log in and returns success or error information.
    """
    cfg = get_effective_imap_config("config.yaml")
    host = cfg.get("host")
    port = int(cfg.get("port") or 993)
    user = cfg.get("user") or ""
    password = cfg.get("password") or ""
    two_factor = cfg.get("two_factor") or ""

    # Build the connect button.  Clicking it will re‑trigger this
    # endpoint and replace the #imap-status div.
    button_html = (
        '<button class="button is-info" hx-get="/imap/ping" '
        'hx-target="#imap-status" hx-swap="outerHTML">Подключиться</button>'
    )

    # When user or password are empty, we don't attempt a connection.
    if not user or not password:
        msg = "IMAP не настроен"
        status_html = (
            f'<span class="badge bg-warning">IMAP: missing credentials</span> '
            f'<small class="text-muted">{msg}</small>'
        )
        html = f'<div id="imap-status">{button_html}&nbsp;{status_html}</div>'
        return HTMLResponse(html)

    # Attempt to connect and log in.  We time the operation for display.
    t0 = time.perf_counter()
    try:
        # Initialise the IMAP client and attempt to log in.  We explicitly
        # call logout() afterwards because IMAPCompatClient does not
        # implement the context manager protocol.
        client = IMAPCompatClient(host, port=port, ssl=True)
        try:
            client.login(user, password, two_factor)
        finally:
            try:
                client.logout()
            except Exception:
                pass
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        status_html = (
            f'<span class="badge bg-success">IMAP: OK</span> '
            f'<small class="text-muted">{elapsed_ms} ms</small>'
        )
    except Exception as exc:
        err = str(exc)
        # Avoid HTML injection in error messages
        err_esc = err.replace("<", "&lt;").replace(">", "&gt;")
        status_html = (
            f'<span class="badge bg-danger">IMAP: FAIL</span> '
            f'<small class="text-muted">{err_esc}</small>'
        )
        logger.warning("IMAP ping failed: %s", err_esc)

    html = f'<div id="imap-status">{button_html}&nbsp;{status_html}</div>'
    return HTMLResponse(html)