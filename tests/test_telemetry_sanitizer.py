"""Log-attribute sanitizer: the OTLP bridge must never receive non-primitive
extras. Pins the exact Celery case — celery/app/trace.py logs every task
completion with ``extra={'data': <context dict>}``, which produced an
"Invalid type dict for attribute 'data'" warning per task.

Pure logging objects, no OTEL/DB. Run: pytest workers/shared/tests/
"""

import json
import logging

from ctx_worker_shared.telemetry import _ATTR_PRIMITIVES, _AttributeSanitizerFilter


def _record(**extra):
    record = logging.LogRecord("celery.app.trace", logging.INFO, __file__, 1,
                               "Task %s succeeded", ("t",), None)
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def _assert_otel_safe(record: logging.LogRecord):
    baseline = vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()
    for key, value in record.__dict__.items():
        if key in baseline or value is None:
            continue
        ok = isinstance(value, _ATTR_PRIMITIVES) or (
            isinstance(value, (list, tuple))
            and all(isinstance(v, _ATTR_PRIMITIVES) for v in value)
        )
        assert ok, f"attribute {key!r} still non-primitive: {type(value)}"


def test_celery_data_dict_is_serialized():
    ctx = {"id": "abc", "name": "semantic_chunking_task", "retval": {"chunk_count": 0}}
    record = _record(data=ctx)
    assert _AttributeSanitizerFilter().filter(record) is True  # never drops records
    _assert_otel_safe(record)
    assert json.loads(record.data)["name"] == "semantic_chunking_task"  # data survives


def test_primitives_and_primitive_sequences_untouched():
    record = _record(event="parser.docling.completed", folder_id="f1",
                     failed_pages=[1, 2, 3], elements=42, ratio=0.8, ok=True)
    _AttributeSanitizerFilter().filter(record)
    assert record.failed_pages == [1, 2, 3]
    assert record.event == "parser.docling.completed"
    _assert_otel_safe(record)


def test_unserializable_value_falls_back_to_str():
    class Weird:
        def __repr__(self):
            return "<weird>"

    record = _record(data={"obj": Weird()}, direct=Weird())
    _AttributeSanitizerFilter().filter(record)
    _assert_otel_safe(record)
    assert "<weird>" in record.direct


def test_standard_logrecord_fields_never_touched():
    record = _record(data={"x": 1})
    args_before = record.args
    _AttributeSanitizerFilter().filter(record)
    assert record.args is args_before          # formatting must keep working
    assert record.getMessage() == "Task t succeeded"
