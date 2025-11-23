# Campaign Hub

## Overview

This repository contains a proof‑of‑concept “Campaign Hub” web application.  The goal
of the project is to provide a single interface where marketing campaigns can be
matched with their associated post‑click statistics and advertising metrics from
multiple external systems.  The initial version focuses on integrating two
sources:

* **System statistics** – daily metrics exported from your own advertising
  platform.  These files are retrieved from a URL of the form
  `https://catsnetwork.ru/iface/campaigns/stat/uniques/{campaign_id}?export=xlsx`.
  Each export contains day‑level impressions, clicks, spend and related
  measures.
* **Post‑click metrics** – daily statistics from Yandex Metrica delivered via
  email attachments.  The IMAP client connects to a Yandex mailbox and
  downloads XLSX/CSV attachments, which are then parsed into daily sessions,
  bounce rates and conversions.

Future versions can incorporate verification vendors (Weborama, Adriver,
Adserving, etc.) by adding additional parsers and metrics to the pipeline.

The application is built with **FastAPI** for the backend, **SQLAlchemy** for
database access, **Jinja2** and **HTMX** for server‑rendered HTML, and
**APScheduler** to schedule periodic imports.  SQLite is used as the
default database for simplicity; switching to PostgreSQL only requires
updating the connection string in `campaign_hub/app/database.py`.

## Repository layout

```
campaign_hub/
├── app/                 # FastAPI application package
│   ├── __init__.py
│   ├── config.py        # Settings loader (YAML + environment)
│   ├── crud.py          # Data access functions
│   ├── database.py      # SQLAlchemy engine/session/metadata
│   ├── models.py        # ORM models
│   ├── routers/         # API and HTML endpoints
│   │   ├── __init__.py
│   │   ├── campaigns.py
│   │   ├── directory.py
│   │   ├── files.py
│   │   └── update.py
│   ├── schemas.py       # Pydantic models for API
│   ├── services/        # Business logic
│   │   ├── __init__.py
│   │   ├── fetcher.py   # Download system files and mail attachments (placeholders)
│   │   ├── parser.py    # Parse XLSX/CSV into dataframes
│   │   └── joiner.py    # Join system and metrica metrics
│   ├── tasks.py         # Background tasks using APScheduler
│   └── templates/       # Jinja2 templates for HTML pages
│       ├── layout.html
│       ├── campaigns.html
│       ├── directory.html
│       └── files.html
├── main.py              # Entrypoint for uvicorn
├── config.yaml          # Sample configuration file
└── requirements.txt     # Python dependencies
```

## Quick start (Python 3.11 or newer)

1.  Ensure you are using **Python 3.11** or a compatible version.  All
    dependencies listed in `requirements.txt` provide Windows wheels for
    Python 3.11 and have been tested on Windows 10.  Because pre‑built wheels
    for Pandas and NumPy may be missing for certain Python versions, this
    application uses the lightweight `openpyxl` library and the standard
    library to parse Excel/CSV files.  Once you have Python 3.11 installed,
    set up a virtual environment and install the dependencies:

    ```bash
    # On Windows PowerShell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    ```

2.  Configure your IMAP and system endpoints in `config.yaml` or via
    environment variables.  At minimum, set:

    - `imap.host`, `imap.user` and `imap.password` for the Yandex mailbox
    - `system.base_url` for your campaign export endpoint

3.  Run database migrations (Alembic migrations are optional – the models
    will create tables automatically if they do not exist) and start the server:

    ```bash
    uvicorn campaign_hub.main:app --reload
    ```

4.  Open `http://localhost:8000` in your browser.  Use the **Directory**
    section to register campaigns by specifying their internal ID and mail rule.
    Once registered, click the **Update** button on a campaign to fetch and
    process new files.

5.  The **Campaigns** page lists all campaigns and their total metrics for a
    selected period.  Clicking on a row expands the daily breakdown via HTMX.

## Notes and limitations

* The current version does **not** automatically download from Yandex – the
  functions in `services/fetcher.py` contain placeholders where actual IMAP
  logic can be implemented once credentials are provided.
* Likewise, fetching system statistics relies on HTTP access to
  `catsnetwork.ru` and may need additional authentication.  This is exposed
  as a configurable base URL in `config.yaml`.
* The example schema covers a small subset of possible metrics.  Feel free
  to extend the models to include reach, frequency, viewability and other
  measures as your datasets evolve.
* The UI uses server‑rendered templates and HTMX to dynamically load daily
  breakdowns.  This approach avoids complex front‑end frameworks while still
  providing a responsive experience.
