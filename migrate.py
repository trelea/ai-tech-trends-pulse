#!/usr/bin/env python3
"""Manual migration runner for the Turso DB. No auto-migrations anywhere.

Usage:
  .venv/bin/python migrate.py status    # show applied vs pending
  .venv/bin/python migrate.py up        # apply all pending migrations in order

Migrations are plain SQL files in migrations/, named NNN_slug.sql.
Applied versions are tracked in the schema_migrations table.
"""
import os
import re
import sys

import turso_serverless
from dotenv import load_dotenv

load_dotenv()

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")


def connect():
    url = os.getenv("TURSO_DATABASE_URL")
    token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not token:
        raise SystemExit("Missing TURSO_DATABASE_URL / TURSO_AUTH_TOKEN in .env")
    return turso_serverless.connect(url, auth_token=token)


def _ensure_table(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version TEXT PRIMARY KEY, applied_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
    )


def _applied(conn):
    return {r[0] for r in conn.execute("SELECT version FROM schema_migrations").fetchall()}


def _files():
    out = []
    for f in sorted(os.listdir(MIGRATIONS_DIR)):
        if re.fullmatch(r"\d{3}_.*\.sql", f):
            out.append(f[:-4])
    return out


def _split_statements(sql):
    """Split a migration file into statements. Supports -- line comments."""
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def cmd_status():
    conn = connect()
    _ensure_table(conn)
    applied = _applied(conn)
    for v in _files():
        print(f"  [{'x' if v in applied else ' '}] {v}")
    conn.close()


def cmd_up():
    conn = connect()
    _ensure_table(conn)
    applied = _applied(conn)
    pending = [v for v in _files() if v not in applied]
    if not pending:
        print("Nothing to apply.")
        conn.close()
        return
    for v in pending:
        path = os.path.join(MIGRATIONS_DIR, v + ".sql")
        with open(path) as f:
            for stmt in _split_statements(f.read()):
                conn.execute(stmt)
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", [v])
        conn.commit()
        print(f"Applied {v}")
    conn.close()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "status":
        cmd_status()
    elif cmd == "up":
        cmd_up()
    else:
        raise SystemExit(f"Unknown command {cmd!r}; use: status | up")
