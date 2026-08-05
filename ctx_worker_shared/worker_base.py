"""
Shared decorator wiring CAPABILITY_SCHEMA validation around each worker task.

Workers declare a ``CAPABILITY_SCHEMA`` dict at module level and decorate their
Celery task with ``@with_capability(CAPABILITY_SCHEMA)``. The decorator:

  1. Validates the incoming payload against ``input_fields`` (strict mode —
     raises ContractViolationError on missing always-required fields).
  2. Acquires a SQLAlchemy session via ``get_worker_session()``.
  3. Instantiates a ``StorageClient`` pointing at ``BLOB_STORAGE_URI``.
  4. Invokes the task function with ``storage`` and ``db`` injected as kwargs.
  5. Validates the result dict against ``output_fields`` (always_present check).
  6. Commits the session on success, rolls back on exception.

Usage:

    from ctx_worker_shared.worker_base import with_capability

    CAPABILITY_SCHEMA = {
        "worker": "tesseract",
        "input_fields": {
            "folder_id": {"type": "string", "required": "always"},
            "dpi": {"type": "integer", "required": "optional", "default": 300},
        },
        "output_fields": {
            "folder_id": {"type": "string", "always_present": True},
            "parser_output_count": {"type": "integer", "always_present": True},
        },
    }

    @celery_app.task(name="tesseract_worker.tesseract_task")
    @with_capability(CAPABILITY_SCHEMA)
    def tesseract_task(payload, storage, db):
        folder_id = payload["folder_id"]
        pdf_bytes = storage.read_blob(folder_id, "source.pdf")
        # ... process ...
        storage.write_parser_elements(folder_id, rows)
        return {"folder_id": folder_id, "parser_output_count": len(rows)}
"""
from __future__ import annotations

import logging
from functools import wraps
from typing import Any, Callable, Dict

from .contract_validator import (
    validate_capability_input,
    validate_capability_output,
)
from .db_session import get_blob_storage_uri, get_db_url, get_session_factory, get_worker_session
from .storage import StorageClient

logger = logging.getLogger(__name__)

# Bootstrap DDL once per worker process (no-op on subsequent calls).
_BOOTSTRAP_DONE: bool = False


def _ensure_bootstrap() -> None:
    global _BOOTSTRAP_DONE
    if _BOOTSTRAP_DONE:
        return
    try:
        db_url = get_db_url()
    except Exception:
        db_url = ""
    if db_url:
        StorageClient.bootstrap_tables(db_url)
    _BOOTSTRAP_DONE = True


def with_capability(capability_schema: Dict[str, Any]) -> Callable:
    """Decorator factory. See module docstring for usage."""

    def decorator(task_fn: Callable) -> Callable:
        @wraps(task_fn)
        def inner(payload: Dict[str, Any], *args, **kwargs):
            # Defensive: tasks dispatched by Celery may pass payload as the
            # first positional arg. We support either shape.
            if not isinstance(payload, dict):
                raise TypeError(
                    f"Worker {capability_schema.get('worker', '?')} expected dict payload, "
                    f"got {type(payload).__name__}"
                )

            # 1. Input validation (raises ContractViolationError on missing required)
            validate_capability_input(capability_schema, payload, mode="strict")

            blob_uri = get_blob_storage_uri()
            if not blob_uri:
                raise RuntimeError(
                    "BLOB_STORAGE_URI not set; workers require a blob backend URI"
                )

            # 2. Bootstrap DDL once per process (no-op after first call).
            _ensure_bootstrap()

            # 3. Acquire session + factory-backed storage and run the task.
            # Storage opens its own short-lived sessions per method call; `db`
            # is kept for direct repository use inside the task.
            factory = get_session_factory()
            storage = StorageClient.from_factory(blob_uri=blob_uri, session_factory=factory)
            with get_worker_session() as db:
                result = task_fn(payload, *args, storage=storage, db=db, **kwargs)

                # 4. Output validation
                if not isinstance(result, dict):
                    raise TypeError(
                        f"Worker {capability_schema.get('worker', '?')} must return a dict, "
                        f"got {type(result).__name__}"
                    )
                validate_capability_output(capability_schema, result, mode="strict")
                return result

        return inner

    return decorator
