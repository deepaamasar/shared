"""ctx_worker_shared — shared library for ContextKraft v1 Celery workers.

Packaged from the former ``workers/shared`` monorepo folder so each worker repo
can install it from a local wheel (no git / no index at deploy time). Public
modules used by workers: ``storage``, ``db_session``, ``contract_validator``,
``worker_base``.

(``adapter_base`` was removed in 1.1.0 — intra-worker adapters are retired;
shape conversions flow through explicit format_adapter nodes on the canvas.)
"""

__version__ = "1.2.0"
