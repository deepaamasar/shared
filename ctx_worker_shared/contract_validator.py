"""
Data Contract Runtime Validator
================================
Shared module for enforcing Data Contract compliance in ContextBuilder workers.

Each Celery worker CAN validate its task input and output payloads against the
central ``data_contract_registry`` in real-time.

Enforcement modes (set via ``CONTRACT_ENFORCEMENT_MODE`` env var):
  * ``off``    – skip contract validation entirely (fastest; no network calls)
  * ``warn``   – validate and emit WARNING logs on failure, never raise (default)
  * ``strict`` – validate and raise ``ContractViolationError`` on failure

Backend URL discovery (first match wins):
  1. ``BACKEND_API_URL`` env var
  2. Base URL derived from ``WORKER_RESULTS_URL`` (strips path)

Usage
-----
.. code-block:: python

    from contract_validator import validate_contract

    # Validate an ingest result before posting back
    ok, errors = validate_contract("raw_document", {"folder_id": "abc", "document_name": "invoice.pdf"})

    # Strict mode one-off
    validate_contract("parsed_document", output_dict, mode="strict")
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import requests as _requests  # type: ignore
    _REQUESTS_AVAILABLE = True
except ImportError:
    _requests = None  # type: ignore
    _REQUESTS_AVAILABLE = False

try:
    import jsonschema  # type: ignore
    _JSONSCHEMA_AVAILABLE = True
except ImportError:
    jsonschema = None  # type: ignore
    _JSONSCHEMA_AVAILABLE = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: Global enforcement mode; override per-call via the ``mode`` kwarg.
ENFORCEMENT_MODE: str = os.environ.get("CONTRACT_ENFORCEMENT_MODE", "warn").lower()

#: Request timeout (seconds) when fetching contracts from the backend API.
_FETCH_TIMEOUT: int = int(os.environ.get("CONTRACT_FETCH_TIMEOUT", "5"))

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ContractViolationError(Exception):
    """Raised when ``mode='strict'`` and a contract violation is found."""


# ---------------------------------------------------------------------------
# Backend URL helpers
# ---------------------------------------------------------------------------


def _get_backend_url() -> str:
    """Return the backend API base URL, or empty string if not determinable."""
    url = os.environ.get("BACKEND_API_URL", "").strip()
    if url:
        return url.rstrip("/")

    # Fall back: derive from WORKER_RESULTS_URL (strips path component)
    worker_url = os.environ.get("WORKER_RESULTS_URL", "").strip()
    if worker_url:
        try:
            parsed = urlparse(worker_url)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            pass

    return ""


# ---------------------------------------------------------------------------
# Contract fetch (cached per (key, api_url) pair)
# ---------------------------------------------------------------------------


@lru_cache(maxsize=64)
def _fetch_contract(contract_key: str, api_url: str) -> Optional[Dict[str, Any]]:
    """Fetch and cache a contract definition from the backend registry.

    Returns the contract dict on success, or ``None`` if unreachable / missing.
    Cached at process level so repeated task invocations do not re-fetch.
    """
    if not _REQUESTS_AVAILABLE:
        logger.debug(
            "CONTRACT_VALIDATION: 'requests' library not available; "
            "install it to enable contract fetching."
        )
        return None

    endpoint = f"{api_url}/contracts/{contract_key}"
    try:
        resp = _requests.get(endpoint, timeout=_FETCH_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404:
            logger.debug(
                "CONTRACT_VALIDATION: contract '%s' not found in registry (404)", contract_key
            )
        else:
            logger.debug(
                "CONTRACT_VALIDATION: backend returned %s for contract '%s'",
                resp.status_code,
                contract_key,
            )
    except Exception as exc:
        logger.debug(
            "CONTRACT_VALIDATION: could not reach backend at '%s': %s", endpoint, exc
        )
    return None


def clear_contract_cache() -> None:
    """Invalidate the in-process contract cache (useful in tests)."""
    _fetch_contract.cache_clear()


# ---------------------------------------------------------------------------
# Core validation
# ---------------------------------------------------------------------------


def validate_contract(
    contract_key: str,
    payload: Dict[str, Any],
    *,
    mode: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """Validate *payload* against the named data contract.

    Parameters
    ----------
    contract_key:
        Key of the contract to validate against (e.g. ``'raw_document'``).
    payload:
        The data dictionary to validate (must be a JSON-serialisable dict).
    mode:
        Enforcement mode override; one of ``'off'``, ``'warn'``, ``'strict'``.
        Defaults to the ``CONTRACT_ENFORCEMENT_MODE`` environment variable.

    Returns
    -------
    (is_valid, errors):
        ``is_valid`` is ``True`` when no violations were found.
        ``errors`` is a list of human-readable violation messages.

    Raises
    ------
    ContractViolationError
        Only when ``mode='strict'`` and at least one violation is found.
    """
    effective_mode = (mode or ENFORCEMENT_MODE).lower()

    if effective_mode == "off":
        return True, []

    api_url = _get_backend_url()
    if not api_url:
        logger.debug(
            "CONTRACT_VALIDATION: BACKEND_API_URL / WORKER_RESULTS_URL not set; "
            "skipping validation of '%s'",
            contract_key,
        )
        return True, []

    contract = _fetch_contract(contract_key, api_url)
    if contract is None:
        # Silently skip — contract might not exist yet or backend unreachable
        return True, []

    errors: List[str] = []

    # 1. Required-fields check (fields_json)
    fields_json: List[Dict[str, Any]] = contract.get("fields_json") or []
    for field in fields_json:
        fname = field.get("name", "")
        if not fname:
            continue
        is_required = field.get("required", False)
        if is_required and fname not in payload:
            errors.append(
                f"Missing required field '{fname}' for contract '{contract_key}'"
            )

    # 2. JSON Schema validation (schema_json)
    schema: Dict[str, Any] = contract.get("schema_json") or {}
    if schema and _JSONSCHEMA_AVAILABLE:
        try:
            jsonschema.validate(instance=payload, schema=schema)
        except jsonschema.ValidationError as exc:  # type: ignore[union-attr]
            errors.append(
                f"Schema violation in '{contract_key}': {exc.message} "
                f"(path: {' > '.join(str(p) for p in exc.absolute_path) or 'root'})"
            )
        except jsonschema.SchemaError as exc:  # type: ignore[union-attr]
            # Malformed contract schema — log and continue
            logger.warning(
                "CONTRACT_VALIDATION: schema_json for '%s' is invalid: %s",
                contract_key,
                exc.message,
            )

    is_valid = len(errors) == 0

    if not is_valid:
        summary = f"CONTRACT_VALIDATION FAILED [{contract_key}]: " + "; ".join(errors)
        if effective_mode == "strict":
            raise ContractViolationError(summary)
        logger.warning(summary)
    else:
        logger.debug("CONTRACT_VALIDATION OK [%s]", contract_key)

    return is_valid, errors


# ---------------------------------------------------------------------------
# Capability-schema validation (P7)
# ---------------------------------------------------------------------------


def validate_capability_input(
    capability_schema: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    mode: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """Validate worker input payload against CAPABILITY_SCHEMA.input_fields.

    Checks fields where required == "always".  Fields with required == "optional"
    are silently skipped when absent.  Returns (is_valid, errors).

    Raises ContractViolationError when mode='strict' and violations are found.
    """
    effective_mode = (mode or ENFORCEMENT_MODE).lower()
    if effective_mode == "off":
        return True, []

    worker_name = capability_schema.get("worker", "unknown")
    input_fields: Dict[str, Any] = capability_schema.get("input_fields") or {}
    errors: List[str] = []

    for field_name, field_spec in input_fields.items():
        if field_spec.get("required") == "always" and field_name not in payload:
            errors.append(
                f"Worker '{worker_name}' requires field '{field_name}' (always required) — not present in payload"
            )

    is_valid = len(errors) == 0
    if not is_valid:
        summary = f"CAPABILITY_INPUT FAILED [{worker_name}]: " + "; ".join(errors)
        if effective_mode == "strict":
            raise ContractViolationError(summary)
        logger.warning(summary)
    else:
        logger.debug("CAPABILITY_INPUT OK [%s]", worker_name)

    return is_valid, errors


def validate_capability_output(
    capability_schema: Dict[str, Any],
    output: Dict[str, Any],
    *,
    mode: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """Validate worker output against CAPABILITY_SCHEMA.output_fields.

    Checks fields where always_present == True.  Returns (is_valid, errors).

    Raises ContractViolationError when mode='strict' and violations are found.
    """
    effective_mode = (mode or ENFORCEMENT_MODE).lower()
    if effective_mode == "off":
        return True, []

    worker_name = capability_schema.get("worker", "unknown")
    output_fields: Dict[str, Any] = capability_schema.get("output_fields") or {}
    errors: List[str] = []

    for field_name, field_spec in output_fields.items():
        if field_spec.get("always_present") is True and field_name not in output:
            errors.append(
                f"Worker '{worker_name}' must always produce field '{field_name}' — missing from output"
            )

    is_valid = len(errors) == 0
    if not is_valid:
        summary = f"CAPABILITY_OUTPUT FAILED [{worker_name}]: " + "; ".join(errors)
        if effective_mode == "strict":
            raise ContractViolationError(summary)
        logger.warning(summary)
    else:
        logger.debug("CAPABILITY_OUTPUT OK [%s]", worker_name)

    return is_valid, errors
