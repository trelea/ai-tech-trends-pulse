"""Turso (libsql) connection + SQLAlchemy session factory.

Remote-only, pure-Python stack (works on Python 3.14 where the Rust
``libsql-experimental`` wheel has no build):

  pip install sqlalchemy turso_serverless python-dotenv

Env (in .env):
  TURSO_DATABASE_URL=libsql://<db>.<region>.turso.io
  TURSO_AUTH_TOKEN=<jwt>

``turso_serverless`` is DB-API 2.0 over Turso SQL-over-HTTP. SQLAlchemy's
pysqlite dialect calls ``create_function`` on connect (regexp support),
which the driver lacks — ``_CompatConnection`` stubs those sqlite-only
hooks and delegates everything else.

Note: constraint violations surface as ``turso_serverless.dbapi``
exceptions (e.g. ``turso_serverless.dbapi.IntegrityError``), not
``sqlalchemy.exc`` — catch both in write paths.
"""
import os

import turso_serverless
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

load_dotenv()


def _creds():
    url = os.getenv("TURSO_DATABASE_URL")
    token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not token:
        raise SystemExit("Missing TURSO_DATABASE_URL / TURSO_AUTH_TOKEN in .env")
    return url, token


_SQLITE_ONLY_HOOKS = frozenset({
    "create_function", "create_aggregate", "create_collation",
    "set_progress_handler", "set_authorizer", "interrupt",
    "set_trace_callback", "enable_load_extension", "load_extension",
    "setlimit",
})


class _CompatConnection:
    """Wrap turso_serverless connection with sqlite-only no-op hooks."""

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        if name in _SQLITE_ONLY_HOOKS:
            return lambda *a, **k: None
        return getattr(self._raw, name)


def _creator():
    url, token = _creds()
    return _CompatConnection(turso_serverless.connect(url, auth_token=token))


_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine("sqlite://", creator=_creator, poolclass=NullPool)
    return _engine


SessionLocal = sessionmaker(bind=get_engine(), expire_on_commit=False)


def upsert_week_trending(week_label, themes_url, trends_url, dropped_url=None,
                         source_prefix=None):
    """Insert or update the week_trendings row for a weekly trend run.

    Re-running the same week replaces URLs instead of failing on the
    week_label unique constraint. Returns the row id.
    """
    url, token = _creds()
    conn = turso_serverless.connect(url, auth_token=token)
    try:
        conn.execute(
            "INSERT INTO week_trendings "
            "(week_label, themes_url, trends_url, dropped_url, source_prefix) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(week_label) DO UPDATE SET "
            "themes_url=excluded.themes_url, trends_url=excluded.trends_url, "
            "dropped_url=excluded.dropped_url, source_prefix=excluded.source_prefix, "
            "updated_at=CURRENT_TIMESTAMP",
            [week_label, themes_url, trends_url, dropped_url, source_prefix],
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM week_trendings WHERE week_label=?", [week_label]
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()
