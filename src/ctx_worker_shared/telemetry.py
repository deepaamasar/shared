"""OTEL telemetry bootstrap for ContextBuilder workers (traces + logs).

Single entry point: :func:`init_telemetry`. It replaces the per-worker
``_init_worker_tracing()`` copies and adds the OTLP **logs** bridge.

Design contract (see ``docs/observability_implementation_plan.md``):

* **Never halts the worker.** Every step is guarded — if the collector is
  unreachable at startup or dies mid-run, we log one WARNING and continue. The
  app keeps processing documents; telemetry buffers and drops, never raises.
* **Reads configuration from the environment**, not from a worker-local
  ``settings`` object, so it is identical across every worker.
* **service.name is passed in** by the caller (``contextbuilder-worker-<id>``)
  — never hardcoded here.
* **Feedback-loop guard**: the OTLP log handler excludes the SDK's own
  ``opentelemetry.*`` / ``grpc`` / ``urllib3`` loggers, so a failed log export
  cannot log an error that re-exports and amplifies while the collector is down.

Env read:
  OTEL_EXPORTER_OTLP_ENDPOINT   collector gRPC endpoint (default localhost:4317)
  OTEL_EXPORTER_OTLP_TIMEOUT    per-export timeout seconds (default 5)
  OTEL_TRACES_SAMPLER_ARG       root sample ratio 0..1 (default 1.0)
  DEPLOYMENT_ENVIRONMENT / ENVIRONMENT
  BUILD_SHA                     -> service.version
  OTEL_LOG_LEVEL                min level bridged to OTLP (default INFO)
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import re
import socket
import threading
import traceback
from contextvars import ContextVar
from typing import Any, Optional

logger = logging.getLogger("ctx_worker_shared.telemetry")

# Run-ids look like manual__… / quick_run__… / scheduled__… — used to pull run_id
# out of a worker's POSITIONAL task args (workers pass task_id/config/dag_id/run_id
# positionally, not a single payload dict, so we can't key on a fixed slot).
_RUN_ID_RE = re.compile(r"^(manual__|quick_run|scheduled__|backfill__|dataset_triggered__)")

# --- per-record correlation context (set at task entry, read by the log filter)
_run_id: ContextVar[Optional[str]] = ContextVar("otel_run_id", default=None)
_folder_id: ContextVar[Optional[str]] = ContextVar("otel_folder_id", default=None)
_step_name: ContextVar[Optional[str]] = ContextVar("otel_step_name", default=None)

# Logger-name prefixes whose records must NOT be bridged to OTLP (feedback loop).
_BRIDGE_EXCLUDE_PREFIXES = ("opentelemetry", "grpc", "urllib3", "httpcore", "httpx")

_initialized = False
_lock = threading.Lock()
_providers: dict[str, Any] = {}


# --------------------------------------------------------------------------- #
# Correlation-context API (used by Celery signals and call sites)
# --------------------------------------------------------------------------- #
def bind_log_context(*, run_id: Optional[str] = None, folder_id: Optional[str] = None,
                     step_name: Optional[str] = None) -> None:
    """Bind correlation ids onto the current context so every subsequent log
    record (and the log filter) carries them. No-op for ``None`` values."""
    if run_id is not None:
        _run_id.set(run_id)
    if folder_id is not None:
        _folder_id.set(folder_id)
    if step_name is not None:
        _step_name.set(step_name)


def clear_log_context() -> None:
    _run_id.set(None)
    _folder_id.set(None)
    _step_name.set(None)


class _ContextEnrichmentFilter(logging.Filter):
    """Stamp run_id/folder_id/step_name from the contextvars onto each record.

    Always returns True (never drops). Only sets a field when present and not
    already explicitly provided on the record (e.g. via ``log_event`` extra)."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        for attr, var in (("run_id", _run_id), ("folder_id", _folder_id),
                          ("step_name", _step_name)):
            if getattr(record, attr, None) is None:
                val = var.get()
                if val is not None:
                    setattr(record, attr, val)
        return True


class _BridgeExcludeFilter(logging.Filter):
    """Drop SDK/export-path loggers from the OTLP handler to avoid the
    log-export feedback loop when the collector is unreachable."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        name = record.name or ""
        return not name.startswith(_BRIDGE_EXCLUDE_PREFIXES)


# Standard LogRecord attribute names — everything beyond these is a caller
# "extra" that the OTLP LoggingHandler maps into log attributes.
_LOGREC_STANDARD = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()
) | {"message", "asctime", "taskName"}

_ATTR_PRIMITIVES = (bool, str, bytes, int, float)
_ATTR_MAX_LEN = 2048


class _AttributeSanitizerFilter(logging.Filter):
    """Serialize non-primitive log extras before the OTLP handler maps them.

    OTEL log attributes accept only bool/str/bytes/int/float (or sequences of
    those). Third-party libraries attach richer extras — most notably Celery,
    whose task-completion logs carry ``extra={'data': <context dict>}``
    (celery/app/trace.py), producing an "Invalid type dict for attribute
    'data'" warning on EVERY task completion. Rewrite offending values to
    compact JSON strings so the data survives and the noise stops.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        for key, value in list(record.__dict__.items()):
            if key in _LOGREC_STANDARD or value is None:
                continue
            if isinstance(value, _ATTR_PRIMITIVES):
                continue
            if isinstance(value, (list, tuple)) and all(
                isinstance(v, _ATTR_PRIMITIVES) for v in value
            ):
                continue
            try:
                record.__dict__[key] = json.dumps(value, default=str)[:_ATTR_MAX_LEN]
            except Exception:  # noqa: BLE001 — sanitizing must never break logging
                record.__dict__[key] = str(value)[:_ATTR_MAX_LEN]
        return True


# --------------------------------------------------------------------------- #
# Structured-event helper (P2.5)
# --------------------------------------------------------------------------- #
def log_event(lgr: logging.Logger, level: int, event: str, *,
              exc: Optional[BaseException] = None, **fields: Any) -> None:
    """Emit a structured event log.

    ``event`` is a stable dotted name (e.g. ``parser.docling.completed``).
    ``fields`` become structured log attributes (and a readable suffix on the
    console body). trace_id/span_id are auto-attached by the OTLP LoggingHandler
    when emitted inside a span; run_id/folder_id/step_name by the context filter.
    """
    extra: dict[str, Any] = {"event": event}
    for key, value in fields.items():
        # Never clobber reserved LogRecord attributes.
        if key in ("name", "msg", "args", "levelname", "levelno", "message"):
            key = f"f_{key}"
        extra[key] = value
    if exc is not None:
        extra["exception.type"] = type(exc).__name__
        extra["exception.message"] = str(exc)
        extra["exception.stacktrace"] = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
    suffix = " ".join(f"{k}={v}" for k, v in fields.items())
    body = f"{event} {suffix}".rstrip()
    lgr.log(level, body, extra=extra)


# --------------------------------------------------------------------------- #
# Bootstrap
# --------------------------------------------------------------------------- #
def _build_resource(service_name: str, service_version: Optional[str],
                    env: Optional[str]):
    from opentelemetry.sdk.resources import Resource

    attrs: dict[str, Any] = {"service.name": service_name}
    version = service_version or os.getenv("BUILD_SHA")
    if version:
        attrs["service.version"] = version
    environment = env or os.getenv("DEPLOYMENT_ENVIRONMENT") or os.getenv("ENVIRONMENT")
    if environment:
        attrs["deployment.environment"] = environment
    try:
        attrs["service.instance.id"] = f"{socket.gethostname()}:{os.getpid()}"
    except Exception:  # noqa: BLE001
        pass
    return Resource.create(attrs)


def _setup_tracing(resource, endpoint: str, insecure: bool, timeout: int) -> None:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    try:
        ratio = float(os.getenv("OTEL_TRACES_SAMPLER_ARG") or "1.0")
    except (TypeError, ValueError):
        ratio = 1.0
    ratio = min(max(ratio, 0.0), 1.0)

    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(ratio)),
    )
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure, timeout=timeout)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _providers["tracer"] = provider


def _setup_logging(resource, endpoint: str, insecure: bool, timeout: int) -> None:
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

    provider = LoggerProvider(resource=resource)
    exporter = OTLPLogExporter(endpoint=endpoint, insecure=insecure, timeout=timeout)
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    set_logger_provider(provider)

    level_name = (os.getenv("OTEL_LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = LoggingHandler(level=level, logger_provider=provider)
    handler.addFilter(_ContextEnrichmentFilter())
    handler.addFilter(_BridgeExcludeFilter())  # feedback-loop guard
    handler.addFilter(_AttributeSanitizerFilter())  # celery 'data' dict etc.

    _providers["logger"] = provider
    _providers["log_handler"] = handler
    _providers["log_level"] = level
    _attach_log_handler_to_root()


def _attach_log_handler_to_root() -> None:
    """(Re)attach the OTLP log handler to the root logger. Called at setup AND
    again from Celery's after_setup_logger signals: Celery hijacks the root
    logger on worker start and strips handlers added at import time, so without
    this the worker's TASK logs never reach the collector (only the import-time
    startup log does)."""
    handler = _providers.get("log_handler")
    if handler is None:
        return
    root = logging.getLogger()
    if handler not in root.handlers:
        root.addHandler(handler)
    level = _providers.get("log_level", logging.INFO)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)


def _setup_propagators() -> None:
    from opentelemetry.propagators.composite import CompositePropagator
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagate import set_global_textmap

    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )


def _instrument(kind: str) -> None:
    if kind == "worker":
        try:
            from opentelemetry.instrumentation.requests import RequestsInstrumentor
            RequestsInstrumentor().instrument()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Requests instrumentation skipped: %s", exc)
        try:
            from opentelemetry.instrumentation.celery import CeleryInstrumentor
            CeleryInstrumentor().instrument()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Celery instrumentation skipped: %s", exc)
        _wire_celery_context_signals()


def _wire_celery_context_signals() -> None:
    """Bind correlation ids from the task payload on task_prerun; clear on
    task_postrun. Best-effort: the payload is conventionally the first arg."""
    try:
        from celery.signals import task_prerun, task_postrun
    except Exception:  # noqa: BLE001
        return

    @task_prerun.connect(weak=False)
    def _on_prerun(args=None, kwargs=None, **_):  # noqa: ANN001
        # Workers pass positional args (task_id, …, dag_id, run_id), not one payload
        # dict — so pull run_id by pattern from the args, and folder_id/step_name
        # from kwargs or any dict arg. trace_id/span_id are stamped separately by
        # the LoggingHandler when inside a span.
        run_id = folder_id = step_name = None
        kwargs = kwargs or {}
        run_id = kwargs.get("run_id")
        folder_id = kwargs.get("folder_id")
        step_name = kwargs.get("step_name")
        for a in (args or []):
            if isinstance(a, str) and run_id is None and _RUN_ID_RE.match(a):
                run_id = a
            elif isinstance(a, dict):
                run_id = run_id or a.get("run_id")
                folder_id = folder_id or a.get("folder_id")
                step_name = step_name or a.get("step_name")
        if run_id or folder_id or step_name:
            bind_log_context(run_id=run_id, folder_id=folder_id, step_name=step_name)

    @task_postrun.connect(weak=False)
    def _on_postrun(**_):  # noqa: ANN001
        clear_log_context()

    # Celery hijacks the root logger on worker start and strips the OTLP handler
    # added at import time; re-attach it after Celery configures logging so worker
    # TASK logs reach the collector (not just the import-time startup log).
    try:
        from celery.signals import after_setup_logger, after_setup_task_logger

        @after_setup_logger.connect(weak=False)
        def _reattach_root_logger(**_):  # noqa: ANN001
            _attach_log_handler_to_root()

        @after_setup_task_logger.connect(weak=False)
        def _reattach_task_logger(**_):  # noqa: ANN001
            _attach_log_handler_to_root()
    except Exception:  # noqa: BLE001
        pass


def init_telemetry(service_name: str, *, service_version: Optional[str] = None,
                   env: Optional[str] = None, instrument: str = "worker") -> None:
    """Initialise OTEL traces + logs for this process. Idempotent and fully
    guarded — never raises, never halts the worker if the collector is down."""
    global _initialized
    with _lock:
        if _initialized:
            return
        _initialized = True

    endpoint = (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or "http://localhost:4317").strip()
    insecure = not endpoint.startswith("https://")
    try:
        timeout = int(float(os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT") or "5"))
    except (TypeError, ValueError):
        timeout = 5

    try:
        import opentelemetry  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        logger.warning("OpenTelemetry SDK unavailable; telemetry disabled: %s", exc)
        return

    try:
        sdk_version = _otel_version()
        resource = _build_resource(service_name, service_version, env)

        try:
            _setup_tracing(resource, endpoint, insecure, timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OTEL tracing init failed (continuing without traces): %s", exc)

        try:
            _setup_logging(resource, endpoint, insecure, timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OTEL logging init failed (continuing without OTLP logs): %s", exc)

        try:
            _setup_propagators()
        except Exception as exc:  # noqa: BLE001
            logger.warning("OTEL propagator init failed: %s", exc)

        try:
            _instrument(instrument)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OTEL instrumentation init failed: %s", exc)

        atexit.register(shutdown)
        logger.info(
            "OTEL telemetry initialised service.name=%s endpoint=%s sdk=%s",
            service_name, endpoint, sdk_version,
        )
    except Exception as exc:  # noqa: BLE001
        # Absolute backstop: nothing in telemetry setup may halt the worker.
        logger.warning("OTEL telemetry init failed; continuing without telemetry: %s", exc)


def _otel_version() -> str:
    try:
        from importlib.metadata import version
        return version("opentelemetry-sdk")
    except Exception:  # noqa: BLE001
        return "unknown"


def shutdown(timeout_millis: int = 2000) -> None:
    """Hard time-boxed flush + shutdown so a dead collector can't hang teardown.

    Runs in a daemon thread joined with a hard timeout: ``provider.shutdown()``
    on its own blocks on the exporter's retry backoff (observed ~60s against a
    dead collector), so force_flush's timeout alone is not enough. If the drain
    doesn't finish within the budget we abandon it — the daemon thread dies with
    the process.
    """
    def _drain() -> None:
        for key in ("tracer", "logger"):
            provider = _providers.get(key)
            if provider is None:
                continue
            try:
                flush = getattr(provider, "force_flush", None)
                if callable(flush):
                    flush(timeout_millis)
            except Exception:  # noqa: BLE001
                pass
            try:
                provider.shutdown()
            except Exception:  # noqa: BLE001
                pass

    t = threading.Thread(target=_drain, name="otel-shutdown", daemon=True)
    t.start()
    t.join(timeout_millis / 1000.0)
