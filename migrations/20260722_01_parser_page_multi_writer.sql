-- 20260722_01_parser_page_multi_writer.sql
-- specs/parsing v10 (P4.2) -- PARSE-AC-48/49. ADR-0005.
--
-- Widens parser_page's uniqueness to include `stage`, so a branch-tagged
-- specialist writer (stage='branch_<queue_name>') can coexist with the
-- canonical 'raw'/'cleaned' row for the same (folder_id, page_number)
-- instead of silently overwriting it. Idempotent (DROP CONSTRAINT IF EXISTS).
--
-- This is the FIRST migration for this repo (workers/shared has no migration
-- runner -- apply manually against the target DB, same ad-hoc pattern every
-- backend/migrations/*.sql file uses). The DDL bootstrap string in
-- src/ctx_worker_shared/storage.py (_DDL_PARSER_PAGE) already reflects this
-- same shape for fresh installs -- this file is for already-provisioned DBs.

ALTER TABLE public.parser_page
    DROP CONSTRAINT IF EXISTS parser_page_uniq;

ALTER TABLE public.parser_page
    ADD CONSTRAINT parser_page_uniq UNIQUE (folder_id, page_number, stage);

ALTER TABLE public.parser_page
    DROP CONSTRAINT IF EXISTS parser_page_stage_valid;

ALTER TABLE public.parser_page
    ADD CONSTRAINT parser_page_stage_valid CHECK (
        stage IN ('raw', 'cleaned') OR stage ~ '^branch_[a-z][a-z0-9_]*$'
    );
