"""ctx_worker_shared — shared library for ContextKraft v1 Celery workers.

Packaged from the former ``workers/shared`` monorepo folder so each worker repo
can install it from a local wheel (no git / no index at deploy time). Public
modules used by workers: ``storage``, ``db_session``, ``contract_validator``,
``adapter_base``, ``worker_base``.
"""

__version__ = "1.0.0"
