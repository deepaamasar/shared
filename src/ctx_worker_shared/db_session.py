"""
Per-Celery-task SQLAlchemy session factory.

Each worker task calls ``get_worker_session()`` once at task start, uses it for
all DB ops, and the contextmanager commits on success or rolls back on error.

Engine + sessionmaker are module-level singletons. SQLAlchemy's connection pool
(default size=5, overflow=10) handles concurrency within a Celery worker process.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests
from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_SessionLocal = None
_engine_db_url: Optional[str] = None
_dotenv_cache: Optional[Dict[str, str]] = None


def _dotenv_candidates() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[2]
    workers_root = repo_root / "workers"
    cwd = Path.cwd().resolve()

    candidates: list[Path] = []
    for candidate in (
        cwd / ".env",
        cwd.parent / ".env",
        repo_root / ".env",
        workers_root / ".env",
    ):
        if candidate not in candidates:
            candidates.append(candidate)

    if workers_root.is_dir():
        for child in sorted(workers_root.iterdir(), key=lambda p: p.name.lower()):
            if child.is_dir():
                env_path = child / ".env"
                if env_path not in candidates:
                    candidates.append(env_path)

    return candidates


def _load_dotenv_values() -> Dict[str, str]:
    global _dotenv_cache
    if _dotenv_cache is not None:
        return _dotenv_cache

    merged: Dict[str, str] = {}
    for env_path in _dotenv_candidates():
        if not env_path.is_file():
            continue
        for key, value in dotenv_values(env_path).items():
            if key and value is not None and key not in merged:
                merged[key] = str(value).strip()

    _dotenv_cache = merged
    return merged


def _get_config_value(*keys: str) -> str:
    for key in keys:
        value = os.environ.get(key)
        if value and str(value).strip():
            return str(value).strip()

    dotenv_values_map = _load_dotenv_values()
    for key in keys:
        value = dotenv_values_map.get(key)
        if value and str(value).strip():
            return str(value).strip()

    return ""


def get_config_value(*keys: str) -> str:
    """Public config lookup that checks process env first, then worker .env files."""
    return _get_config_value(*keys)


def get_blob_storage_uri() -> str:
    """Resolve blob storage root for workers.

    Reads BLOB_STORAGE_URI from the environment (or any worker .env file).
    Raises RuntimeError immediately if not set — no fallback to DATA_BACKBONE_DIR.
    Workers call this at module level so the process fails at startup if misconfigured.
    """
    uri = get_config_value("BLOB_STORAGE_URI")
    if not uri:
        raise RuntimeError(
            "BLOB_STORAGE_URI is not set. "
            "Add BLOB_STORAGE_URI to .env or the environment before starting any worker."
        )
    return uri


def _clean_db_url(db_url: str) -> str:
    return db_url.replace("postgresql+asyncpg://", "postgresql://")


def _resolve_profile_decrypt_url() -> str:
    url = _get_config_value("PROFILE_DECRYPT_URL")
    if url:
        return url

    worker_results_url = _get_config_value("WORKER_RESULTS_URL")
    if worker_results_url:
        base = worker_results_url.split("/", 3)
        if len(base) >= 3:
            return f"{base[0]}//{base[2]}/rbac/decryptConnectionProfile"

    raise RuntimeError(
        "PROFILE_DECRYPT_URL not set and could not be derived from WORKER_RESULTS_URL"
    )


def _decrypt_postgres_profile(profile_name: str) -> str:
    url = _resolve_profile_decrypt_url()
    try:
        # Require the profile to be categorized as the system database. The
        # backend rejects (403) a profile of any other category (e.g. a
        # chunk_store postgres profile), so the system DB can't be resolved from
        # the wrong connection.
        resp = requests.post(
            url,
            json={"profile_name": profile_name, "expected_category": "system_database"},
            timeout=30,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to call decryptConnectionProfile at {url}: {exc}") from exc

    if resp.status_code != 200:
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        raise RuntimeError(
            f"decryptConnectionProfile for Postgres returned HTTP {resp.status_code}: {body}"
        )

    data = resp.json() or {}
    config: Dict[str, Any] = data.get("config") or {}
    host = config.get("host")
    port = config.get("port") or 5432
    database = config.get("database")
    username = config.get("username")
    password = config.get("password") or ""

    missing = [
        name for name, val in (
            ("host", host),
            ("database", database),
            ("username", username),
            ("password", password),
        ) if not val
    ]
    if missing:
        raise RuntimeError(
            f"Postgres connection profile '{profile_name}' is missing required fields: {', '.join(missing)}"
        )

    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


def _resolve_system_db_profile_name() -> str:
    name = _get_config_value("DB_CONNECTION_PROFILE_NAME")
    if name:
        return name
    raise RuntimeError(
        "DATABASE_URL not set and DB_CONNECTION_PROFILE_NAME is not configured. "
        "Set DB_CONNECTION_PROFILE_NAME in the environment."
    )


def _resolve_db_url() -> str:
    db_url = _get_config_value("DATABASE_URL")
    if db_url:
        return _clean_db_url(db_url)

    profile_name = _resolve_system_db_profile_name()
    return _clean_db_url(_decrypt_postgres_profile(profile_name))


def _init() -> None:
    """Lazy initialisation; runs once per worker process."""
    global _engine, _SessionLocal, _engine_db_url

    db_url = _resolve_db_url()
    if _engine is not None and _engine_db_url == db_url:
        return

    if _engine is not None:
        try:
            _engine.dispose()
        except Exception:
            pass

    _engine = create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        future=True,
    )
    _SessionLocal = sessionmaker(bind=_engine, autoflush=False, expire_on_commit=False)
    _engine_db_url = db_url


@contextmanager
def get_worker_session() -> Iterator[Session]:
    """Yield a SQLAlchemy session; commit on clean exit, roll back on exception.

    Usage:
        with get_worker_session() as db:
            storage = StorageClient(blob_uri=..., db=db)
            ...
    """
    _init()
    assert _SessionLocal is not None  # for type checkers
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_read_session() -> Iterator[Session]:
    """Short-lived read session.  Yields a session, commits (no-op for reads),
    and closes immediately.  Use this for the read phase of a worker task so
    the connection is returned to the pool before the heavy compute begins.

    Usage:
        with get_read_session() as db:
            storage = StorageClient(blob_uri=..., db=db)
            data = storage.read_parser_elements(folder_id)
        # connection is released here; do compute without holding a connection
    """
    _init()
    assert _SessionLocal is not None
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_write_session() -> Iterator[Session]:
    """Short-lived write session.  Identical semantics to get_worker_session
    but named explicitly for the write phase of a worker task.

    Usage:
        # after all heavy compute is done:
        with get_write_session() as db:
            storage = StorageClient(blob_uri=..., db=db)
            storage.write_chunks(folder_id, chunks)
    """
    _init()
    assert _SessionLocal is not None
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db_url() -> str:
    """Resolve and return the worker database URL.

    Use this to call StorageClient.bootstrap_tables() in workers that connect
    via DB_CONNECTION_PROFILE_NAME (profile-based) rather than DATABASE_URL.
    """
    return _resolve_db_url()


def get_session_factory():
    """Return the module-level sessionmaker after initialising the engine.

    Used by StorageClient.from_factory() so each storage method can open and
    close its own short-lived session rather than sharing one long-lived
    session across minutes of compute.
    """
    _init()
    assert _SessionLocal is not None
    return _SessionLocal
