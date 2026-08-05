"""log_event console-body rendering (Exempt bugfix, RCA 2026-08-03).

Regression guard: when ``exc`` is passed, the exception type + message must appear in
the human-readable console body — not only in the structured ``extra`` (which feeds the
OTLP sink). The RCA incident showed a worker logging a bare
``entity_extraction.runtime_failed action=fail`` while the real ``OSError [E050]`` was
buried in ``extra`` and invisible on stdout.

Pure logging objects, no OTEL/DB. Run: pytest workers/shared/tests/
"""

import logging

from ctx_worker_shared.telemetry import log_event


def _capture(level, event, **kwargs):
    logger = logging.getLogger("test_log_event_body")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)
    try:
        log_event(logger, level, event, **kwargs)
    finally:
        logger.removeHandler(handler)
    assert len(records) == 1
    return records[0]


def test_exception_type_and_message_in_console_body():
    exc = OSError("[E050] Can't find model './models/en_core_web_lg'")
    record = _capture(logging.ERROR, "entity_extraction.runtime_failed", action="fail", exc=exc)
    body = record.getMessage()
    # the event + fields the console already carried
    assert "entity_extraction.runtime_failed" in body
    assert "action=fail" in body
    # the regression: type + message must be visible in the body, not only in extra
    assert "OSError" in body
    assert "[E050]" in body
    # structured extra still carries the full detail (unchanged)
    assert record.__dict__["exception.type"] == "OSError"
    assert "[E050]" in record.__dict__["exception.message"]
    assert "Traceback" in record.__dict__["exception.stacktrace"] or record.__dict__["exception.stacktrace"]


def test_no_exception_body_unchanged():
    record = _capture(logging.INFO, "parser.docling.completed", folder_id="f1", elements=42)
    body = record.getMessage()
    assert body == "parser.docling.completed folder_id=f1 elements=42"
    assert "exception" not in body
