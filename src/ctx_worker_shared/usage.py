"""Build FinOps ``usage`` blocks for the worker-results callback.

Spec: coding-governance/specs/finops/requirements.md (FINOPS-AC-3, AC-6,
AC-9). Workers attach ``"usage": [<item>, ...]`` to the callback payload they
already POST — no new transport. The backend hook
(``cost_service.record_cost_from_callback``) prices each item against
user-managed rate cards; capture is always on and fail-open.

One usage item::

    {
      "charge_category": "llm" | "embedding" | "ocr" | "external_api"
                       | "compute" | "storage" | "vector_db",
      "provider_name":  "<provider>",                       # required
      "model":          "<model>",                          # optional
      "quantity":       {"input_tokens": 1200, ...},        # required, multi-component
      "estimated":      false,                              # true = quantity estimated (AC-6)
      "folder_id":      "<package id>",                     # optional attribution
      "connection_id":  "...", "node_id": "...",            # optional
      "attributes":     {...},                              # optional
    }

Quantity component names are the pricing contract: rate-card ``pricing`` keys
equal these component keys (billed = Σ quantity[k] × pricing[k]).

Estimation (AC-6): when a provider returns no usage, quantities MUST be
estimated and flagged ``estimated=True`` — never zero, never omitted. The
default estimator is the char-based heuristic (~4 chars/token); no tokenizer
dependency.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

CHARGE_CATEGORIES = (
    "llm", "embedding", "ocr", "external_api", "compute", "storage", "vector_db",
)

# Char-per-token heuristic for the estimation fallback (deliberately simple;
# a real tokenizer is an optional later improvement — estimates are flagged).
_CHARS_PER_TOKEN = 4


def estimate_tokens(text: Optional[str]) -> int:
    """Estimate a token count from text length. Never returns 0 for non-empty text."""
    if not text:
        return 0
    return max(1, len(text) // _CHARS_PER_TOKEN)


def build_usage_item(
    *,
    charge_category: str,
    provider_name: str,
    quantity: Dict[str, Any],
    model: Optional[str] = None,
    estimated: bool = False,
    folder_id: Optional[str] = None,
    connection_id: Optional[str] = None,
    node_id: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Validate + assemble one usage item (raises ValueError on a bad item —
    callers build items at success time and should fail loudly in dev)."""
    if charge_category not in CHARGE_CATEGORIES:
        raise ValueError(f"charge_category must be one of {CHARGE_CATEGORIES}")
    if not provider_name:
        raise ValueError("provider_name is required")
    if not isinstance(quantity, dict) or not quantity:
        raise ValueError("quantity must be a non-empty dict of components")
    for key, value in quantity.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise ValueError(f"quantity['{key}'] must be a non-negative number")

    item: Dict[str, Any] = {
        "charge_category": charge_category,
        "provider_name": provider_name,
        "quantity": dict(quantity),
        "estimated": bool(estimated),
    }
    if model:
        item["model"] = model
    if folder_id:
        item["folder_id"] = str(folder_id)
    if connection_id:
        item["connection_id"] = str(connection_id)
    if node_id:
        item["node_id"] = str(node_id)
    if attributes:
        item["attributes"] = attributes
    return item


def llm_usage_from_response(
    response: Any,
    *,
    provider_name: str,
    model: Optional[str] = None,
    prompt_text: Optional[str] = None,
    completion_text: Optional[str] = None,
    folder_id: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """Extract LLM token usage from a provider response; estimate on absence.

    AC-3: when the provider reports usage, the recorded quantities equal the
    provider-reported numbers exactly. AC-6: when it doesn't, quantities are
    estimated from the prompt/completion text and flagged ``estimated=True``.
    """
    tokens = extract_token_usage(response)
    if tokens:
        return build_usage_item(
            charge_category="llm",
            provider_name=provider_name,
            model=model,
            quantity=tokens,
            estimated=False,
            folder_id=folder_id,
            **extra,
        )
    quantity = {
        "input_tokens": estimate_tokens(prompt_text),
        "output_tokens": estimate_tokens(completion_text),
    }
    return build_usage_item(
        charge_category="llm",
        provider_name=provider_name,
        model=model,
        quantity=quantity,
        estimated=True,
        folder_id=folder_id,
        **extra,
    )


def extract_token_usage(response: Any) -> Optional[Dict[str, int]]:
    """Normalize provider-reported token usage to {input_tokens, output_tokens}.

    Understands the response shapes CB's workers see today:
      * OpenAI/LiteLLM style:  {"usage": {"prompt_tokens", "completion_tokens"}}
      * Anthropic style:       {"usage": {"input_tokens", "output_tokens"}}
      * Bedrock Converse:      {"usage": {"inputTokens", "outputTokens"}}
      * Bedrock Titan embed:   {"inputTextTokenCount": N}
      * Ollama:                {"prompt_eval_count", "eval_count"}
    Returns None when the response carries no usable usage (⇒ caller estimates).
    """
    if response is None:
        return None
    body = response if isinstance(response, dict) else getattr(response, "__dict__", None)
    if not isinstance(body, dict):
        return None

    usage = body.get("usage")
    if isinstance(usage, dict):
        for in_key, out_key in (
            ("prompt_tokens", "completion_tokens"),
            ("input_tokens", "output_tokens"),
            ("inputTokens", "outputTokens"),
        ):
            if in_key in usage or out_key in usage:
                return {
                    "input_tokens": int(usage.get(in_key) or 0),
                    "output_tokens": int(usage.get(out_key) or 0),
                }

    if "inputTextTokenCount" in body:  # Bedrock Titan embeddings
        return {"input_tokens": int(body.get("inputTextTokenCount") or 0), "output_tokens": 0}

    if "prompt_eval_count" in body or "eval_count" in body:  # Ollama
        return {
            "input_tokens": int(body.get("prompt_eval_count") or 0),
            "output_tokens": int(body.get("eval_count") or 0),
        }
    return None


def embedding_usage(
    *,
    provider_name: str,
    model: Optional[str] = None,
    vectors: int,
    tokens: Optional[int] = None,
    estimated: bool = False,
    folder_id: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """Usage item for an embedding batch (vectorization or query time)."""
    quantity: Dict[str, Any] = {"vectors": int(vectors)}
    if tokens is not None:
        quantity["input_tokens"] = int(tokens)
    return build_usage_item(
        charge_category="embedding",
        provider_name=provider_name,
        model=model,
        quantity=quantity,
        estimated=estimated,
        folder_id=folder_id,
        **extra,
    )


def ocr_usage(
    *,
    provider_name: str,
    pages: int,
    documents: Optional[int] = None,
    folder_id: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """Usage item for a per-page OCR call (e.g. Textract)."""
    quantity: Dict[str, Any] = {"pages": int(pages)}
    if documents is not None:
        quantity["documents"] = int(documents)
    return build_usage_item(
        charge_category="ocr",
        provider_name=provider_name,
        quantity=quantity,
        folder_id=folder_id,
        **extra,
    )


def attach_usage(payload: Dict[str, Any], items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Append usage items to a callback payload (in place; returns it)."""
    if items:
        payload.setdefault("usage", []).extend(items)
    return payload
