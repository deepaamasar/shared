"""Blob-key contract test: ingest write <-> parser read round-trip.

Ingest and parser never call each other; they agree implicitly that a source
file written under a folder_id is discoverable + readable under the same
folder_id via the shared StorageClient. A mismatch (wrong key layout, wrong
blob_uri handling) fails SILENTLY several pipeline stages downstream — the same
bug class as the empty-document-metadata issue. This test locks the round-trip
so a future change to blob_path / list_blobs can't drift the two apart.

Design invariant (owner-confirmed): exactly ONE source file per folder. Parser
receives only folder_id and discovers the single blob — no filename handoff.

Pure fsspec file:// backend, no DB. Run: pytest workers/shared/tests/
"""

import tempfile
from pathlib import Path

from ctx_worker_shared.storage import StorageClient


def _client(blob_uri: str) -> StorageClient:
    # Blob ops never touch the DB; supply a factory that would raise if used so
    # an accidental DB call in a blob path is caught.
    def _no_db():  # pragma: no cover - must never be called by blob ops
        raise AssertionError("blob ops must not open a DB session")

    return StorageClient(blob_uri=blob_uri, session_factory=_no_db)


def test_write_then_list_then_read_round_trip():
    with tempfile.TemporaryDirectory() as tmp:
        blob_uri = Path(tmp).as_uri()  # file:///...
        folder_id = "11111111-1111-1111-1111-111111111111"
        name = "report.pdf"
        payload = b"%PDF-1.7 fake bytes"

        # INGEST side: write the single source blob under folder_id.
        writer = _client(blob_uri)
        writer.write_blob(folder_id, name, payload)
        assert writer.blob_exists(folder_id, name), "write-landed assertion (ingest)"

        # PARSER side: a *separate* client at the SAME blob_uri discovers + reads.
        reader = _client(blob_uri)
        found = reader.list_blobs(folder_id, "*")
        assert found == [name], f"parser discovery must find exactly the written blob, got {found}"
        assert reader.read_blob(folder_id, name) == payload


def test_single_file_invariant_discovery():
    """Discovery returns exactly one entry for a one-file folder, and the
    file lives at {blob_uri}/{folder_id}/{name} (the shared key layout)."""
    with tempfile.TemporaryDirectory() as tmp:
        blob_uri = Path(tmp).as_uri()
        folder_id = "22222222-2222-2222-2222-222222222222"
        c = _client(blob_uri)
        c.write_blob(folder_id, "doc.pdf", b"x")

        assert len(c.list_blobs(folder_id, "*")) == 1
        # Key layout is exactly {blob_uri}/{folder_id}/{name}
        assert c.blob_path(folder_id, "doc.pdf") == f"{blob_uri}/{folder_id}/doc.pdf"
        # Concrete file exists on disk at that nested path.
        assert (Path(tmp) / folder_id / "doc.pdf").is_file()


def test_missing_folder_lists_empty_not_error():
    """An unknown folder_id lists empty (parser turns this into a loud
    'no source file' error at the boundary, not a silent downstream empty)."""
    with tempfile.TemporaryDirectory() as tmp:
        c = _client(Path(tmp).as_uri())
        assert c.list_blobs("does-not-exist", "*") == []
