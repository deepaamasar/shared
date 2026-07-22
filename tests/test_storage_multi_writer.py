"""ADR-0005 / specs/parsing v10 (P4.2) -- parser_page multi-writer semantics.

PARSE-AC-48/49/50: the widened UNIQUE(folder_id, page_number, stage) + the
open branch-stage CHECK constraint, and count_parser_pages counting distinct
pages rather than total rows. Real Postgres required (DATABASE_URL) -- this
is schema/constraint behavior, not something a fake can stand in for.

Run from workers/shared/ with PYTHONPATH pointed at src/ and DATABASE_URL set:
    python -m pytest tests/test_storage_multi_writer.py -v
"""
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from ctx_worker_shared.db_session import get_db_url
from ctx_worker_shared.storage import StorageClient


@pytest.fixture(scope="module")
def db_session_factory():
    engine = create_engine(get_db_url(), future=True)
    yield sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    engine.dispose()


@pytest.fixture
def storage(db_session_factory):
    session = db_session_factory()
    client = StorageClient(blob_uri="file:///tmp/adr0005_test", db=session)
    yield client, session
    session.rollback()
    session.close()


@pytest.fixture
def folder_id():
    fid = str(uuid.uuid4())
    yield fid
    # Cleanup on a fresh session/connection -- the test's own session may have
    # been rolled back already.
    engine = create_engine(get_db_url(), future=True)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM public.parser_page WHERE folder_id = :fid"), {"fid": fid})
        conn.commit()
    engine.dispose()


@pytest.mark.ac("PARSE-AC-48")
def test_check_constraint_rejects_malformed_stage(storage, folder_id):
    client, session = storage
    with pytest.raises(Exception):
        session.execute(
            text(
                "INSERT INTO public.parser_page (folder_id, page_number, elements, stage) "
                "VALUES (:fid, 1, '[]'::jsonb, :stage)"
            ),
            {"fid": folder_id, "stage": "not_a_valid_stage"},
        )
        session.commit()
    session.rollback()


@pytest.mark.ac("PARSE-AC-48")
def test_check_constraint_accepts_branch_stage(storage, folder_id):
    client, session = storage
    session.execute(
        text(
            "INSERT INTO public.parser_page (folder_id, page_number, elements, stage) "
            "VALUES (:fid, 1, '[]'::jsonb, :stage)"
        ),
        {"fid": folder_id, "stage": "branch_table_specialist"},
    )
    session.commit()
    row = session.execute(
        text("SELECT stage FROM public.parser_page WHERE folder_id = :fid"), {"fid": folder_id},
    ).mappings().first()
    assert row["stage"] == "branch_table_specialist"


@pytest.mark.ac("PARSE-AC-49")
def test_two_writers_different_stages_both_survive(storage, folder_id):
    """The core bug this spec fixes: today (pre-widened constraint), a second
    writer to the same (folder_id, page_number) with a different stage
    silently destroys the first writer's elements."""
    client, session = storage
    client.write_parser_pages(
        folder_id, [{"page_number": 1, "elements": [{"element_id": "elem-0000", "text": "raw content"}]}],
        stage="raw",
    )
    client.write_parser_pages(
        folder_id, [{"page_number": 1, "elements": [{"element_id": "elem-0000", "text": "branch content"}]}],
        stage="branch_table_specialist",
    )
    session.commit()

    rows = session.execute(
        text(
            "SELECT stage, elements FROM public.parser_page "
            "WHERE folder_id = :fid AND page_number = 1 ORDER BY stage"
        ),
        {"fid": folder_id},
    ).mappings().all()
    by_stage = {r["stage"]: r["elements"] for r in rows}
    assert set(by_stage) == {"raw", "branch_table_specialist"}
    assert by_stage["raw"][0]["text"] == "raw content"
    assert by_stage["branch_table_specialist"][0]["text"] == "branch content"


@pytest.mark.ac("PARSE-AC-50")
def test_count_parser_pages_counts_distinct_pages_not_rows(storage, folder_id):
    client, session = storage
    client.write_parser_pages(
        folder_id, [{"page_number": 1, "elements": [{"element_id": "elem-0000", "text": "a"}]}],
        stage="raw",
    )
    client.write_parser_pages(
        folder_id, [{"page_number": 1, "elements": [{"element_id": "elem-0000", "text": "b"}]}],
        stage="branch_table_specialist",
    )
    session.commit()

    assert client.count_parser_pages(folder_id) == 1, (
        "1 distinct page carrying 2 stage rows must count as 1 page, not 2"
    )


def test_read_parser_page_rows_all_stages_returns_every_stage(storage, folder_id):
    client, session = storage
    client.write_parser_pages(
        folder_id, [{"page_number": 1, "elements": [{"element_id": "elem-0000", "text": "raw"}]}],
        stage="raw",
    )
    client.write_parser_pages(
        folder_id, [{"page_number": 1, "elements": [{"element_id": "elem-0000", "text": "branch"}]}],
        stage="branch_table_specialist",
    )
    session.commit()

    rows = client.read_parser_page_rows_all_stages(folder_id)
    stages = {r["stage"] for r in rows}
    assert stages == {"raw", "branch_table_specialist"}


def test_read_parser_page_rows_all_stages_empty_folder_returns_empty_list(storage, folder_id):
    client, session = storage
    assert client.read_parser_page_rows_all_stages(folder_id) == []
