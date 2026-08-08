"""specs/capability-schema v1 (CAPSCH-AC-1..3) — declared `allowed` values.

A worker declares its enum choices once, on the schema field (`allowed: [...]`); the shared
validator enforces membership. Fields WITHOUT `allowed` behave exactly as before (back-compat).

Ported onto the `v1` line 2026-08-08. The enforcement originally landed on the shared repo's
`main` branch (commit 2a5799b), which is a separate lineage reconstructed from a shipped 1.6.0
wheel — it never had these tests, and `v1` (the authoritative line) never had the enforcement. Two
lineages both shipping "1.6.1" is what produced three different wheels with the same version
number; this port consolidates the behaviour onto v1 and the version moves to 1.7.0.
"""
import pytest

from ctx_worker_shared.contract_validator import (
    ContractViolationError,
    validate_capability_input,
)

_SCHEMA = {
    "worker": "qdrant",
    "input_fields": {
        "SearchType": {"type": "string", "required": "optional",
                       "allowed": ["cosine", "dot", "euclid"]},
        "vectordatabasetable": {"type": "string", "required": "always"},
    },
}


def test_allowed_value_accepted():
    ok, errors = validate_capability_input(
        _SCHEMA, {"vectordatabasetable": "t", "SearchType": "cosine"}, mode="warn")
    assert ok and errors == []


def test_out_of_list_value_rejected():
    ok, errors = validate_capability_input(
        _SCHEMA, {"vectordatabasetable": "t", "SearchType": "manhattan"}, mode="warn")
    assert not ok
    assert any("SearchType" in e and "manhattan" in e for e in errors), errors


def test_strict_mode_raises_on_out_of_list_value():
    with pytest.raises(ContractViolationError):
        validate_capability_input(
            _SCHEMA, {"vectordatabasetable": "t", "SearchType": "manhattan"}, mode="strict")


def test_absent_optional_allowed_field_is_not_membership_checked():
    # CAPSCH-AC-2: absent/None values are not checked — `allowed` constrains VALUES, not presence.
    ok, errors = validate_capability_input(_SCHEMA, {"vectordatabasetable": "t"}, mode="warn")
    assert ok and errors == []
    ok, errors = validate_capability_input(
        _SCHEMA, {"vectordatabasetable": "t", "SearchType": None}, mode="warn")
    assert ok and errors == []


def test_field_without_allowed_is_untouched():
    # CAPSCH-AC-3 back-compat: no `allowed` key => any value passes, exactly as before.
    ok, errors = validate_capability_input(
        _SCHEMA, {"vectordatabasetable": "anything-at-all"}, mode="warn")
    assert ok and errors == []
