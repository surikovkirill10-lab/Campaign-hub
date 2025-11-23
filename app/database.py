"""Database configuration for Campaign Hub.

This module defines the SQLAlchemy engine, session factory and a simple
dependency for FastAPI endpoints.  SQLite is used by default but the
connection string can be overridden via the `DATABASE_URL` environment
variable.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# Read the database URL from environment or fallback to a local SQLite file.
# The default uses the built‑in SQLite driver.  You can switch to PostgreSQL
# by setting DATABASE_URL=postgresql+psycopg2://user:pass@host/dbname.
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "sqlite:///./campaign_hub.db",
)

# Create the engine.  `check_same_thread` is set to False to allow the same
# connection to be used across threads (required by SQLite).
engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def get_db():
    """Yield a database session for FastAPI endpoints.

    The session is closed automatically when the request finishes.  Use this
    function as a dependency in your route definitions:

        from fastapi import Depends
        from campaign_hub.app.database import get_db

        @router.get("/items/")
        def list_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
