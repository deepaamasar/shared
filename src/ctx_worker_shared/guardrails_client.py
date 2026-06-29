"""Sync guardrails client for Celery workers.

Hard contract (Guardrails Plan §"Worker skew" + absent-policy contract):
  - **Never raises** into worker/task code.
  - No-op when ``GUARDRAILS_URL`` is unset *or* no ``guardrail_policy`` is in the
    payload — but NOT silently: emits one ``guardrails.skipped`` (reason=
    not_configured) event per call so "this run had no guardrail coverage" is
    visible in Loki. Callers should invoke this ONCE per task (e.g. per
    ``_run_embedding`` batch), not per chunk.
  - On service unavailability applies the policy ``fail_mode``: ``open`` ⇒ treat
    as allowed (+ guardrails.fail_open); ``closed`` ⇒ mark every item blocked so
    the worker skips them (+ guardrails.fail_closed) — still without raising.

The worker passes the batch of texts; the result exposes per-item verdicts so the
worker can skip/redact individual chunks and count them.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:  # log_event is best-effort; workers always have it via the shared wheel.
    from .telemetry import log_event as _log_event
except Exception:  # noqa: BLE001
    _log_event = None


def _emit(logger, level, event, **fields):
    """Structured event when a logger is present; no-op otherwise (the worker
    client must never raise, even if called without a logger)."""
    if logger is None:
        return
    try:
        if _log_event is not None:
            _log_event(logger, level, event, **fields)
        else:
            logger.log(level, "%s %s", event, fields)
    except Exception:  # noqa: BLE001
        pass


try:  # resolve GUARDRAILS_URL the same way as other worker config (env → .env).
    from .db_session import get_config_value as _get_config_value
except Exception:  # noqa: BLE001
    _get_config_value = None


def _guardrails_url() -> Optional[str]:
    """Process env first, then the worker .env files — same convention as
    BLOB_STORAGE_URI / PROFILE_DECRYPT_URL (ctx_worker_shared.db_session), so
    operators can set GUARDRAILS_URL in a worker's .env, not only the OS env."""
    if _get_config_value is not None:
        try:
            return _get_config_value("GUARDRAILS_URL") or None
        except Exception:  # noqa: BLE001
            pass
    return os.getenv("GUARDRAILS_URL")


_DEFAULT_TIMEOUT_MS = 5000


@dataclass
class WorkerGuardrailResult:
    verdict: str = "allowed"
    results: List[Dict[str, Any]] = field(default_factory=list)
    skipped: bool = True  # True ⇒ no-op (no policy/url); worker behaves as today

    def is_blocked(self, i: int) -> bool:
        return i < len(self.results) and self.results[i].get("verdict") == "blocked"

    def text(self, i: int, original: str) -> str:
        if i < len(self.results):
            t = self.results[i].get("transformed_text")
            if t is not None:
                return t
        return original

    @property
    def blocked_count(self) -> int:
        return sum(1 for r in self.results if r.get("verdict") == "blocked")


def guardrails_check(
    *,
    texts: List[str],
    policy: Optional[Dict[str, Any]],
    direction: str = "input",
    enforcement_point: Optional[str] = None,
    run_id: Optional[str] = None,
    folder_id: Optional[str] = None,
    step_name: Optional[str] = None,
    caller: str = "worker",
    logger: Optional[logging.Logger] = None,
) -> WorkerGuardrailResult:
    url = _guardrails_url()
    if not url or not policy or not policy.get("checks"):
        _emit(
            logger, logging.INFO, "guardrails.skipped",
            reason="not_configured", enforcement_point=enforcement_point,
            run_id=run_id, folder_id=folder_id, step_name=step_name,
        )
        return WorkerGuardrailResult(skipped=True)

    timeout = float(policy.get("timeout_ms", _DEFAULT_TIMEOUT_MS)) / 1000.0
    content = {"text": texts[0]} if len(texts) == 1 else {"texts": list(texts)}
    payload = {
        "policy": policy,
        "direction": direction,
        "content": content,
        "context": {"run_id": run_id, "enforcement_point": enforcement_point, "caller": caller},
    }
    try:
        import requests

        resp = requests.post(url.rstrip("/") + "/v1/check", json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        return WorkerGuardrailResult(
            verdict=data.get("verdict", "allowed"), results=data.get("results", []), skipped=False
        )
    except Exception as exc:  # noqa: BLE001 — never raise into worker code
        fail_mode = policy.get("fail_mode", "closed")
        if fail_mode == "open":
            _emit(
                logger, logging.WARNING, "guardrails.fail_open",
                reason=str(exc)[:200], enforcement_point=enforcement_point,
                run_id=run_id, folder_id=folder_id,
            )
            return WorkerGuardrailResult(verdict="allowed", results=[], skipped=False)
        _emit(
            logger, logging.ERROR, "guardrails.fail_closed",
            reason=str(exc)[:200], enforcement_point=enforcement_point,
            run_id=run_id, folder_id=folder_id,
        )
        return WorkerGuardrailResult(
            verdict="blocked",
            results=[{"verdict": "blocked"} for _ in texts],
            skipped=False,
        )
