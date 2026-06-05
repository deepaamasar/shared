"""
StorageClient — two-tier storage abstraction.
================================================

Structured rows (state, parser elements, chunks, embeddings) -> Postgres
Binary blobs (raw PDFs, intermediate page TIFFs)              -> fsspec

Per the plan's locked decisions:
  D5  folder_id IS the document identifier.
  D6  Embeddings live in chunk.embedding JSONB (no pgvector); export workers
      push them to Qdrant/OpenSearch downstream.
  D7  Workers connect directly to Postgres for bulk writes; thin callback to
      backend for orchestration events only.
  D8  Parser is responsible for deleting page_*.tiff blobs on its success path.
  D9  Single BLOB_STORAGE_URI env var resolves the fsspec backend
      (file://, s3://, azure://, gcs://, ...). One client interface regardless.
  D10 fsspec is the de facto Python storage abstraction; we standardize on it.

Idempotency: every write uses ON CONFLICT DO UPDATE so Celery task retries are
safe by construction (R7 in the risk register).

Session management
------------------
StorageClient supports two modes:

  Legacy (backward-compatible):
      with get_worker_session() as db:
          storage = StorageClient(blob_uri=..., db=db)
          data = storage.read_parser_elements(folder_id)
          # ... heavy compute ...
          storage.write_chunks(folder_id, chunks)
      # All ops share one session; caller manages commit/rollback.

  Factory mode (recommended for long-running tasks):
      storage = StorageClient.from_factory(
          blob_uri=os.environ['BLOB_STORAGE_URI'],
          session_factory=get_session_factory(),
      )
      data = storage.read_parser_elements(folder_id)
      # --- heavy compute here (no DB connection held) ---
      storage.write_chunks(folder_id, chunks)
      # Each method opens a short session, commits, and closes immediately.
      # The connection is released back to the pool between operations.

DDL Bootstrap
-------------
Call once per worker process at startup (before the first Celery task runs):

    StorageClient.bootstrap_tables(db_url)

This runs all CREATE TABLE / CREATE INDEX / CREATE FUNCTION DDL on a dedicated
AUTOCOMMIT connection so DDL is NEVER mixed into a DML transaction.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Tuple
from uuid import UUID, uuid5, NAMESPACE_URL

import fsspec
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ── Module-level DDL bootstrap guard ─────────────────────────────────────────
# Set to True once bootstrap_tables() has successfully run in this process.
_TABLES_BOOTSTRAPPED: bool = False

# Columns on the chunk table that workers may update directly.
_UPDATABLE_CHUNK_FIELDS = frozenset({
    "classification", "entities", "risk_score", "embedding", "embedding_model",
})
_JSONB_CHUNK_FIELDS = frozenset({"classification", "entities", "embedding"})

# ── DDL strings (used only by bootstrap_tables) ───────────────────────────────

_DDL_PARSER_PAGE = """
CREATE TABLE IF NOT EXISTS public.parser_page (
    id BIGSERIAL PRIMARY KEY,
    folder_id UUID NOT NULL,
    page_number INTEGER NOT NULL,
    elements JSONB NOT NULL DEFAULT '[]'::jsonb,
    stage TEXT NOT NULL DEFAULT 'raw',
    source JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT parser_page_uniq UNIQUE (folder_id, page_number),
    CONSTRAINT parser_page_stage_valid CHECK (stage IN ('raw', 'cleaned'))
);
CREATE INDEX IF NOT EXISTS idx_parser_page_folder_stage
    ON public.parser_page (folder_id, stage);
"""

_DDL_RUN_STATE = """
CREATE TABLE IF NOT EXISTS public.run_state (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    folder_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    state_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_state_uniq UNIQUE (run_id, folder_id, step_name)
);
CREATE INDEX IF NOT EXISTS run_state_run_folder_idx ON public.run_state (run_id, folder_id);
CREATE INDEX IF NOT EXISTS run_state_jsonb_gin ON public.run_state USING GIN (state_data);
"""

_DDL_CHUNK = """
CREATE TABLE IF NOT EXISTS public.chunk (
    id BIGSERIAL PRIMARY KEY,
    folder_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    content TEXT NOT NULL,
    chunk_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    classification JSONB,
    entities JSONB,
    risk_score DOUBLE PRECISION,
    embedding JSONB,
    embedding_model TEXT,
    embedded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chunk_uniq UNIQUE (folder_id, chunk_id)
);
CREATE INDEX IF NOT EXISTS chunk_folder_idx   ON public.chunk (folder_id);
CREATE INDEX IF NOT EXISTS chunk_embedded_idx ON public.chunk (folder_id) WHERE embedding IS NOT NULL;
CREATE INDEX IF NOT EXISTS chunk_metadata_gin ON public.chunk USING GIN (chunk_metadata);
"""

# Recursive JSONB deep-merge function.  Replaces the shallow || operator in
# merge_state so nested dict keys from different workers are preserved rather
# than overwritten by the top-level merge.
_DDL_JSONB_DEEP_MERGE = """
CREATE OR REPLACE FUNCTION public.jsonb_deep_merge(base jsonb, patch jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT CASE
    WHEN jsonb_typeof(base) = 'object' AND jsonb_typeof(patch) = 'object'
    THEN (
      SELECT jsonb_object_agg(
        COALESCE(k1, k2),
        CASE
          WHEN v1 IS NULL THEN v2
          WHEN v2 IS NULL THEN v1
          WHEN jsonb_typeof(v1) = 'object' AND jsonb_typeof(v2) = 'object'
            THEN public.jsonb_deep_merge(v1, v2)
          ELSE v2
        END
      )
      FROM jsonb_each(base) AS e1(k1, v1)
      FULL OUTER JOIN jsonb_each(patch) AS e2(k2, v2) ON k1 = k2
    )
    ELSE patch
  END
$$;
"""


class StorageClient:
    """Two-tier storage facade.

    Construct via StorageClient(blob_uri, db=session) for legacy/test code, or
    via StorageClient.from_factory(blob_uri, session_factory) for production
    workers so each DB method uses its own short-lived session.
    """

    def __init__(
        self,
        blob_uri: str,
        db: Optional[Session] = None,
        *,
        session_factory: Optional[Callable[[], Session]] = None,
    ):
        if not blob_uri:
            raise ValueError("StorageClient requires a non-empty blob_uri")
        if db is None and session_factory is None:
            raise ValueError(
                "Supply either db (legacy session) or session_factory (factory mode)."
            )
        self._blob_uri = blob_uri.rstrip("/")
        self._fs, self._root_path = fsspec.url_to_fs(self._blob_uri)
        self._db = db
        self._factory = session_factory

    @classmethod
    def from_factory(
        cls,
        blob_uri: str,
        session_factory: Callable[[], Session],
    ) -> "StorageClient":
        """Construct in factory mode: each storage method opens its own
        short-lived session, commits, and closes it immediately."""
        return cls(blob_uri=blob_uri, session_factory=session_factory)

    @contextmanager
    def _db_session(self) -> Iterator[Session]:
        """Yield an active Session.

        Factory mode: opens a fresh session, commits on success, rolls back on
        error, closes on exit — connection is released after each method call.
        Legacy mode (db= supplied): yields the caller-managed session unchanged.
        """
        if self._factory is not None:
            session: Session = self._factory()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        else:
            assert self._db is not None
            yield self._db

    # ── DDL Bootstrap ─────────────────────────────────────────────────────────

    @classmethod
    def bootstrap_tables(cls, db_url: str) -> None:
        """Run all CREATE TABLE / CREATE INDEX / CREATE FUNCTION DDL once per
        worker process via a dedicated AUTOCOMMIT connection.

        DDL is kept out of DML transactions to avoid catalog lock contention
        and implicit serialization of concurrent workers.
        Subsequent calls in the same process are no-ops (_TABLES_BOOTSTRAPPED).
        """
        global _TABLES_BOOTSTRAPPED
        if _TABLES_BOOTSTRAPPED:
            return
        engine = create_engine(db_url, isolation_level="AUTOCOMMIT", future=True)
        try:
            with engine.connect() as conn:
                for ddl in (_DDL_PARSER_PAGE, _DDL_RUN_STATE, _DDL_CHUNK, _DDL_JSONB_DEEP_MERGE):
                    conn.execute(text(ddl))
            _TABLES_BOOTSTRAPPED = True
            logger.info("StorageClient.bootstrap_tables: all tables/functions ensured.")
        except Exception as exc:
            logger.warning("StorageClient.bootstrap_tables failed: %s", exc)
        finally:
            engine.dispose()

    @staticmethod
    def _normalize_run_id(run_id: UUID | str) -> str:
        raw = str(run_id)
        try:
            return str(UUID(raw))
        except (ValueError, TypeError, AttributeError):
            return str(uuid5(NAMESPACE_URL, raw))

    # ── Blob ops (raw binaries only) ─────────────────────────────────────────
    def blob_path(self, folder_id: str, name: str) -> str:
        """Return the full URI-style path for a given (folder_id, name)."""
        return f"{self._blob_uri}/{folder_id}/{name}"

    def blob_exists(self, folder_id: str, name: str) -> bool:
        return self._fs.exists(self.blob_path(folder_id, name))

    def write_blob(self, folder_id: str, name: str, data: bytes) -> str:
        """Write bytes to blob storage. Returns the full blob path."""
        folder_dir = f"{self._blob_uri}/{folder_id}"
        # makedirs is a no-op on object stores; safe on file:// (idempotent).
        try:
            self._fs.makedirs(folder_dir, exist_ok=True)
        except (NotImplementedError, AttributeError):
            # Some fsspec backends don't expose makedirs; ignore.
            pass
        path = self.blob_path(folder_id, name)
        with self._fs.open(path, "wb") as f:
            f.write(data)
        return path

    def read_blob(self, folder_id: str, name: str) -> bytes:
        """Read full bytes. Use open_blob() for streaming large files."""
        with self._fs.open(self.blob_path(folder_id, name), "rb") as f:
            return f.read()

    def open_blob(self, folder_id: str, name: str, mode: str = "rb") -> BinaryIO:
        """Return a file-like object for streaming (FastAPI StreamingResponse, etc.)."""
        return self._fs.open(self.blob_path(folder_id, name), mode)

    def list_blobs(self, folder_id: str, pattern: str = "*") -> List[str]:
        """List blob basenames in a folder matching a glob pattern."""
        glob_path = f"{self._blob_uri}/{folder_id}/{pattern}"
        return [PurePosixPath(p).name for p in self._fs.glob(glob_path)]

    def delete_blob(self, folder_id: str, name: str) -> None:
        path = self.blob_path(folder_id, name)
        if self._fs.exists(path):
            self._fs.rm_file(path)

    def delete_blobs_matching(self, folder_id: str, pattern: str) -> int:
        """Delete all blobs in folder matching a glob pattern.

        Used by the parser worker to clean up page_*.tiff intermediates on its
        success path (D8). Returns the count of files removed.
        """
        glob_path = f"{self._blob_uri}/{folder_id}/{pattern}"
        files = self._fs.glob(glob_path)
        for p in files:
            try:
                self._fs.rm_file(p)
            except FileNotFoundError:
                continue
        return len(files)

    # ── Structured ops: run_state ────────────────────────────────────────────
    def merge_state(
        self,
        run_id: UUID,
        folder_id: str,
        step_name: str,
        output_dict: Dict[str, Any],
    ) -> None:
        """Atomic upsert: state for (run_id, folder_id, step_name) becomes the
        existing JSONB merged with output_dict. Concurrent-safe via PK uniqueness.

        Used by:
          - the orchestrator after each step completes (write-back)
          - merge nodes to materialise combined branch state
        """
        storage_run_id = self._normalize_run_id(run_id)
        with self._db_session() as _db:
            _db.execute(text("""
                INSERT INTO public.run_state (run_id, folder_id, step_name, state_data)
                VALUES (:run_id, :folder_id, :step_name, CAST(:data AS jsonb))
                ON CONFLICT (run_id, folder_id, step_name)
                DO UPDATE SET state_data = public.jsonb_deep_merge(
                                  public.run_state.state_data, EXCLUDED.state_data
                              ),
                              updated_at = NOW()
            """), {
                "run_id": storage_run_id,
                "folder_id": folder_id,
                "step_name": step_name,
                "data": json.dumps(output_dict or {}),
            })

    def load_state(
        self, run_id: UUID, folder_id: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Return the full state dict keyed by step_name for a (run, folder).

        Used by the orchestrator to inject state into a worker's payload before
        dispatch — workers see state.<step_name>.<field> via their FieldMapper
        bindings.
        """
        storage_run_id = self._normalize_run_id(run_id)
        with self._db_session() as _db:
            rows = _db.execute(text("""
                SELECT step_name, state_data
                  FROM public.run_state
                 WHERE run_id = :run_id AND folder_id = :folder_id
            """), {"run_id": storage_run_id, "folder_id": folder_id}).mappings().all()
        return {row["step_name"]: dict(row["state_data"] or {}) for row in rows}

    # ── Structured ops: parser_element ───────────────────────────────────────
    def write_parser_elements(
        self, folder_id: str, rows: List[Dict[str, Any]],
    ) -> int:
        """Batch UPSERT parser elements (idempotent on (folder_id, element_id)).

        Each row dict expects:
          element_id   : str   (required, worker-assigned)
          text         : str | None
          page         : int | None
          element_type : str | None        (text/table/figure/heading/paragraph)
          role         : str | None
          bbox         : dict | None       (e.g. {x,y,w,h})
          table_summary: dict | None
          kv_pairs     : list | None
          source       : dict              (required: {"parser": ..., "filename": ...})

        Returns count of rows submitted (also the count UPSERTed).
        """
        if not rows:
            return 0
        # Use the underlying psycopg2 connection for fast batched insert.
        values = [
            (
                folder_id,
                r["element_id"],
                r.get("text"),
                r.get("page"),
                r.get("element_type"),
                r.get("role"),
                json.dumps(r["bbox"]) if r.get("bbox") is not None else None,
                json.dumps(r["table_summary"]) if r.get("table_summary") is not None else None,
                json.dumps(r["kv_pairs"]) if r.get("kv_pairs") is not None else None,
                json.dumps(r["source"]),
            )
            for r in rows
        ]
        with self._db_session() as _db:
            conn = _db.connection().connection
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO public.parser_element
                      (folder_id, element_id, text, page, element_type, role,
                       bbox, table_summary, kv_pairs, source)
                    VALUES %s
                    ON CONFLICT (folder_id, element_id) DO UPDATE SET
                      text          = EXCLUDED.text,
                      page          = EXCLUDED.page,
                      element_type  = EXCLUDED.element_type,
                      role          = EXCLUDED.role,
                      bbox          = EXCLUDED.bbox,
                      table_summary = EXCLUDED.table_summary,
                      kv_pairs      = EXCLUDED.kv_pairs,
                      source        = EXCLUDED.source
                """, values, page_size=500)
        return len(rows)

    def read_parser_elements(
        self, folder_id: str, page: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Load parser elements for a folder, optionally filtered by page."""
        sql = """
            SELECT element_id, text, page, element_type, role,
                   bbox, table_summary, kv_pairs, source
              FROM public.parser_element
             WHERE folder_id = :folder_id
        """
        params: Dict[str, Any] = {"folder_id": folder_id}
        if page is not None:
            sql += " AND page = :page"
            params["page"] = page
        sql += " ORDER BY page NULLS LAST, element_id"
        with self._db_session() as _db:
            rows = _db.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]

    # ── Structured ops: parser_page ─────────────────────────────────────────
    def write_parser_pages(
        self,
        folder_id: str,
        pages: List[Dict[str, Any]],
        stage: str = "raw",
    ) -> int:
        """UPSERT one row per page into parser_page (idempotent on (folder_id, page_number)).

        Each dict in ``pages``:
          page_number : int            (required)
          elements    : list[dict]     (required, may be empty)
          source      : dict | None    (optional parser provenance)

        Returns count of pages written.
        """
        if not pages:
            return 0
        rows = [
            (
                folder_id,
                p["page_number"],
                json.dumps(p.get("elements") or []),
                stage,
                json.dumps(p["source"]) if p.get("source") is not None else None,
            )
            for p in pages
        ]
        with self._db_session() as _db:
            conn = _db.connection().connection
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO public.parser_page
                        (folder_id, page_number, elements, stage, source)
                    VALUES %s
                    ON CONFLICT (folder_id, page_number) DO UPDATE SET
                        elements   = EXCLUDED.elements,
                        stage      = EXCLUDED.stage,
                        source     = EXCLUDED.source,
                        updated_at = NOW()
                """, rows, page_size=200)
        return len(rows)

    def write_flattened_elements_to_pages(
        self,
        folder_id: str,
        elements: List[Dict[str, Any]],
        stage: str = "raw",
    ) -> int:
        """Group a flat document-level element list by page, then UPSERT into parser_page.

        Each element is expected to carry a ``page`` key (int). Elements missing
        a ``page`` value are bucketed under page 1. Used by TextProcessing to write
        back the cleaned element list while preserving page grouping.

        Returns count of pages written.
        """
        page_groups: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for el in elements:
            page_num = int(el.get("page") or 1)
            page_groups[page_num].append(el)

        pages = [
            {"page_number": page_num, "elements": els}
            for page_num, els in sorted(page_groups.items())
        ]
        return self.write_parser_pages(folder_id, pages, stage)

    def read_parser_elements_from_pages(
        self,
        folder_id: str,
        stage: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Read parser_page rows and return a flat document-level element list.

        Rows are ordered by page_number; each element dict gets a ``page`` key
        injected so callers see the same structure as read_parser_elements().

        Fallback: if no parser_page rows exist for this folder_id (legacy data
        written before the migration), transparently reads from parser_element so
        callers need no conditional logic during the rollout transition.
        """
        sql = """
            SELECT page_number, elements
              FROM public.parser_page
             WHERE folder_id = :folder_id
        """
        params: Dict[str, Any] = {"folder_id": folder_id}
        if stage is not None:
            sql += " AND stage = :stage"
            params["stage"] = stage
        sql += " ORDER BY page_number"

        with self._db_session() as _db:
            rows = _db.execute(text(sql), params).mappings().all()

        if not rows:
            # Fallback: legacy parser_element table (transition support)
            return self.read_parser_elements(folder_id)

        result: List[Dict[str, Any]] = []
        for row in rows:
            for el in (row["elements"] or []):
                result.append({**el, "page": row["page_number"]})
        return result

    def count_parser_pages(
        self,
        folder_id: str,
        stage: Optional[str] = None,
    ) -> int:
        """Return the number of parser_page rows for a folder (no element deserialization).

        Used for artifact handle metadata and data_source assertions.
        """
        sql = """
            SELECT COUNT(*) AS n
              FROM public.parser_page
             WHERE folder_id = :folder_id
        """
        params: Dict[str, Any] = {"folder_id": folder_id}
        if stage is not None:
            sql += " AND stage = :stage"
            params["stage"] = stage

        with self._db_session() as _db:
            row = _db.execute(text(sql), params).mappings().first()
        return int(row["n"]) if row else 0

    def count_chunks(self, folder_id: str) -> int:
        """Return the number of chunk rows for a folder. Used for data_source assertions."""
        with self._db_session() as _db:
            row = _db.execute(
                text("SELECT COUNT(*) AS n FROM public.chunk WHERE folder_id = :folder_id"),
                {"folder_id": folder_id},
            ).mappings().first()
        return int(row["n"]) if row else 0

    # ── Structured ops: chunk ────────────────────────────────────────────────
    def write_chunks(
        self, folder_id: str, chunks: List[Dict[str, Any]],
    ) -> int:
        """Batch UPSERT chunk rows (idempotent on (folder_id, chunk_id)).

        Each chunk dict expects:
          chunk_id       : str    (required, e.g. 'chunk_0001')
          content        : str    (required, the chunk text)
          chunk_metadata : dict   (optional, strategy-specific fields)
        """
        if not chunks:
            return 0
        values = [
            (
                folder_id,
                c["chunk_id"],
                c["content"],
                json.dumps(c.get("chunk_metadata") or {}),
            )
            for c in chunks
        ]
        with self._db_session() as _db:
            conn = _db.connection().connection
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO public.chunk (folder_id, chunk_id, content, chunk_metadata)
                    VALUES %s
                    ON CONFLICT (folder_id, chunk_id) DO UPDATE SET
                      content        = EXCLUDED.content,
                      chunk_metadata = EXCLUDED.chunk_metadata,
                      updated_at     = NOW()
                """, values, page_size=500)
        return len(chunks)

    def read_chunks(
        self, folder_id: str, only_unembedded: bool = False,
    ) -> List[Dict[str, Any]]:
        """Load chunk rows. ``only_unembedded=True`` is used by vectorization
        workers to find work-to-do (Celery retry-safe)."""
        sql = """
            SELECT chunk_id, content, chunk_metadata,
                   classification, entities, risk_score,
                   embedding, embedding_model, embedded_at
              FROM public.chunk
             WHERE folder_id = :folder_id
        """
        if only_unembedded:
            sql += " AND embedding IS NULL"
        sql += " ORDER BY chunk_id"
        with self._db_session() as _db:
            rows = _db.execute(text(sql), {"folder_id": folder_id}).mappings().all()
        return [dict(r) for r in rows]

    def update_chunk_field(
        self, folder_id: str, chunk_id: str, field: str, value: Any,
    ) -> None:
        """Update a single column on one chunk row.

        Used by per-chunk enrichment workers (UC2 pattern): classification,
        entities, and risk_score branches each write disjoint columns.

        Field is whitelisted; passing anything else raises ValueError.
        """
        if field not in _UPDATABLE_CHUNK_FIELDS:
            raise ValueError(
                f"refusing to update non-whitelisted chunk column: {field!r}"
            )
        with self._db_session() as _db:
            if field in _JSONB_CHUNK_FIELDS:
                _db.execute(text(f"""
                    UPDATE public.chunk
                       SET {field} = CAST(:v AS jsonb),
                           updated_at = NOW()
                     WHERE folder_id = :folder_id AND chunk_id = :chunk_id
                """), {"v": json.dumps(value), "folder_id": folder_id, "chunk_id": chunk_id})
            else:
                _db.execute(text(f"""
                    UPDATE public.chunk
                       SET {field} = :v,
                           updated_at = NOW()
                     WHERE folder_id = :folder_id AND chunk_id = :chunk_id
                """), {"v": value, "folder_id": folder_id, "chunk_id": chunk_id})

    def update_chunk_embeddings(
        self,
        folder_id: str,
        embeddings: List[Tuple[str, List[float], str]],
    ) -> int:
        """Batch UPDATE embeddings.

        ``embeddings`` is a list of (chunk_id, vector, model_name) tuples.
        Stored as JSONB array of floats (D6 — no pgvector). Returns count.
        """
        if not embeddings:
            return 0
        rows = [
            (folder_id, c_id, json.dumps(vec), model)
            for c_id, vec, model in embeddings
        ]
        with self._db_session() as _db:
            conn = _db.connection().connection
            with conn.cursor() as cur:
                execute_values(cur, """
                    UPDATE public.chunk AS c SET
                        embedding       = data.emb::jsonb,
                        embedding_model = data.model,
                        embedded_at     = NOW(),
                        updated_at      = NOW()
                    FROM (VALUES %s) AS data(folder_id, chunk_id, emb, model)
                    WHERE c.folder_id = data.folder_id
                      AND c.chunk_id  = data.chunk_id
                """, rows, page_size=500)
        return len(embeddings)
