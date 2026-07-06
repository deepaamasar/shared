"""Usage-block contract test: worker producer side of the FinOps seam.

Spec: coding-governance/specs/finops/requirements.md. The backend consumer
(cost_service.record_cost_from_callback) prices whatever arrives in the
callback's ``usage`` list; this test pins the producer side so worker changes
can't silently drift the block shape or the token numbers.

FINOPS-AC-3: provider-reported usage is recorded EXACTLY (every provider
response shape CB's workers see today). FINOPS-AC-6: a response without usage
data yields ESTIMATED quantities flagged estimated=True — never zero, never
omitted.

Pure functions, no DB. Run: pytest workers/shared/tests/
"""

import pytest

from ctx_worker_shared.usage import (
    attach_usage,
    build_usage_item,
    embedding_usage,
    estimate_tokens,
    extract_token_usage,
    resolve_embedding_tokens,
    llm_usage_from_response,
    ocr_usage,
)

# The frozen producer→consumer fixture: what a worker callback carries.
FROZEN_LLM_ITEM = {
    "charge_category": "llm",
    "provider_name": "anthropic",
    "model": "claude-sonnet-4-6",
    "quantity": {"input_tokens": 1200, "output_tokens": 250},
    "estimated": False,
    "folder_id": "pkg-001",
}


@pytest.mark.ac("FINOPS-AC-3")
class TestProviderReportedUsageIsExact:
    """AC-3: recorded quantities equal the provider-reported usage."""

    def test_frozen_item_shape(self):
        item = llm_usage_from_response(
            {"usage": {"input_tokens": 1200, "output_tokens": 250}},
            provider_name="anthropic",
            model="claude-sonnet-4-6",
            folder_id="pkg-001",
        )
        assert item == FROZEN_LLM_ITEM

    @pytest.mark.parametrize(
        "response, expected",
        [
            # OpenAI / LiteLLM
            ({"usage": {"prompt_tokens": 11, "completion_tokens": 7}},
             {"input_tokens": 11, "output_tokens": 7}),
            # Anthropic
            ({"usage": {"input_tokens": 900, "output_tokens": 100}},
             {"input_tokens": 900, "output_tokens": 100}),
            # Bedrock Converse
            ({"usage": {"inputTokens": 42, "outputTokens": 5}},
             {"input_tokens": 42, "output_tokens": 5}),
            # Bedrock Titan embeddings
            ({"inputTextTokenCount": 512},
             {"input_tokens": 512, "output_tokens": 0}),
            # Ollama
            ({"prompt_eval_count": 33, "eval_count": 44},
             {"input_tokens": 33, "output_tokens": 44}),
        ],
    )
    def test_extraction_per_provider_shape(self, response, expected):
        assert extract_token_usage(response) == expected

    def test_extracted_item_is_not_estimated(self):
        item = llm_usage_from_response(
            {"usage": {"prompt_tokens": 10, "completion_tokens": 2}},
            provider_name="openai",
        )
        assert item["estimated"] is False
        assert item["quantity"] == {"input_tokens": 10, "output_tokens": 2}


@pytest.mark.ac("FINOPS-AC-6")
class TestEstimationFallback:
    """AC-6: no provider usage ⇒ estimated quantities, flagged — never zero."""

    def test_no_usage_estimates_and_flags(self):
        item = llm_usage_from_response(
            {"some": "response-without-usage"},
            provider_name="ollama",
            model="llama3",
            prompt_text="p" * 400,
            completion_text="c" * 100,
        )
        assert item["estimated"] is True
        assert item["quantity"] == {"input_tokens": 100, "output_tokens": 25}

    def test_none_response_estimates(self):
        item = llm_usage_from_response(
            None, provider_name="ollama", prompt_text="hello world",
        )
        assert item["estimated"] is True
        assert item["quantity"]["input_tokens"] >= 1  # never zero for real text

    def test_estimate_tokens_never_zero_for_text(self):
        assert estimate_tokens("ab") == 1
        assert estimate_tokens("") == 0
        assert estimate_tokens(None) == 0


class TestBuildersAndValidation:
    """The block-shape contract the backend consumer relies on."""

    def test_ocr_usage_pages(self):
        item = ocr_usage(provider_name="aws_textract", pages=12, documents=1,
                         folder_id="pkg-9")
        assert item["charge_category"] == "ocr"
        assert item["quantity"] == {"pages": 12, "documents": 1}

    def test_embedding_usage_vectors_and_tokens(self):
        item = embedding_usage(provider_name="aws_bedrock", model="titan-v2",
                               vectors=256, tokens=4096)
        assert item["quantity"] == {"vectors": 256, "input_tokens": 4096}

    # FINOPS-AC-21: embedding token capture — measured when the provider reports it,
    # else estimated + flagged; the token quantity is NEVER omitted.
    def test_resolve_embedding_tokens_measured(self):
        tokens, estimated = resolve_embedding_tokens(measured=42, text="anything")
        assert tokens == 42 and estimated is False

    def test_resolve_embedding_tokens_estimates_when_absent(self):
        # Ollama's legacy /api/embeddings returns no token count → estimate, flag it.
        tokens, estimated = resolve_embedding_tokens(measured=None, text="hello world of embeddings")
        assert tokens == estimate_tokens("hello world of embeddings")
        assert tokens > 0 and estimated is True

    def test_resolve_embedding_tokens_never_omits(self):
        # Zero/None measured with real text still yields a positive estimate (AC-21/AC-6).
        tokens, estimated = resolve_embedding_tokens(measured=0, text="some chunk text")
        assert tokens > 0 and estimated is True

    def test_invalid_items_raise(self):
        with pytest.raises(ValueError):
            build_usage_item(charge_category="nope", provider_name="x",
                             quantity={"input_tokens": 1})
        with pytest.raises(ValueError):
            build_usage_item(charge_category="llm", provider_name="",
                             quantity={"input_tokens": 1})
        with pytest.raises(ValueError):
            build_usage_item(charge_category="llm", provider_name="x", quantity={})
        with pytest.raises(ValueError):
            build_usage_item(charge_category="llm", provider_name="x",
                             quantity={"input_tokens": -5})

    def test_attach_usage_appends_no_new_transport(self):
        payload = {"task_id": "t1", "status": "success"}
        attach_usage(payload, [FROZEN_LLM_ITEM])
        assert payload["usage"] == [FROZEN_LLM_ITEM]
        attach_usage(payload, [])  # empty = untouched (FINOPS-AC-9 consumer side)
        assert len(payload["usage"]) == 1
