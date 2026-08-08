"""Retrieval quality metrics — the single definition of precision, recall and MRR.

Two consumers compute these numbers: the evaluation worker (which produces the
figures shown on the evaluation screens) and the search baseline gate in the
backend. They used to hold separate copies of the arithmetic, which meant the
gate could pass while the screen disagreed with it, or vice versa, with nothing
to detect the divergence.

Keeping the formulas here makes that impossible **structurally** rather than by
comparing outputs and hoping (specs/search v8, SRCH-AC-43).

Behaviour is deliberately identical to the worker's original implementation,
including the guards that return 0.0 on an empty denominator and the rounding to
four decimal places. This module was a move, not a rewrite — if you change a
formula here, you are changing what every historical number meant.
"""
from typing import Any, Dict, Optional

__all__ = [
    "precision",
    "recall",
    "reciprocal_rank",
    "retrieval_metrics",
    "METRIC_PRECISION_DP",
]

#: Decimal places every metric is rounded to before it is stored or compared.
METRIC_PRECISION_DP = 4


def precision(relevant_retrieved: int, retrieved_total: int) -> float:
    """Share of returned results that were relevant.

    An empty result set scores 0.0 rather than raising: a query that returned
    nothing has precision 0, which is what both callers need at that boundary.
    """
    if not retrieved_total:
        return 0.0
    return float(relevant_retrieved) / float(retrieved_total)


def recall(matched_expected: int, expected_total: int) -> float:
    """Share of the expected items that were found.

    Note the denominator is the EXPECTED count, not the retrieved count — this
    is the asymmetry that makes recall independent of how much a system returns,
    and the reason recall cannot be bought by returning more.
    """
    if not expected_total:
        return 0.0
    return float(matched_expected) / float(expected_total)


def reciprocal_rank(first_relevant_position: Optional[int]) -> float:
    """1 / rank of the first relevant hit; 0.0 when nothing relevant was found.

    Positions are 1-based, matching how both callers count them. A 0 or negative
    position is treated as "not found" rather than dividing by zero or returning
    a value above 1.
    """
    if not first_relevant_position or first_relevant_position < 1:
        return 0.0
    return 1.0 / float(first_relevant_position)


def retrieval_metrics(
    *,
    relevant_retrieved: int,
    retrieved_total: int,
    matched_expected: int,
    expected_total: int,
    first_relevant_position: Optional[int] = None,
) -> Dict[str, Any]:
    """All three metrics in the result shape both consumers already store.

    `coverage` mirrors `recall` — it is kept because the evaluation screens and
    stored history already read that key; dropping it here would silently blank
    a column rather than announce a change.
    """
    recall_value = recall(matched_expected, expected_total)
    return {
        "precision": round(precision(relevant_retrieved, retrieved_total), METRIC_PRECISION_DP),
        "recall": round(recall_value, METRIC_PRECISION_DP),
        "mrr": round(reciprocal_rank(first_relevant_position), METRIC_PRECISION_DP),
        "coverage": round(recall_value, METRIC_PRECISION_DP),
        "first_relevant_position": first_relevant_position,
        "found_expected_doc": bool(first_relevant_position),
    }
