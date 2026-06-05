# ctx_worker_shared

Shared library for ContextKraft **v1** Celery workers. Packaged from the former
`workers/shared` monorepo folder so each worker repo can install it from a
**local wheel** — no git clone and no package index at deploy time.

## Why a wheel

v1 workers run as bare Celery processes on VMs, and code is moved to those VMs by
copy (not git, not a private index). Previously workers imported the shared code
via a `sys.path` hack assuming the side-by-side monorepo layout
(`from workers.shared.storage import ...`), which breaks the moment a single
worker repo is checked out alone. As a wheel, the shared code is a proper
installed package (`from ctx_worker_shared.storage import ...`) that works from
any working directory.

## Public modules

- `ctx_worker_shared.storage` — `StorageClient`
- `ctx_worker_shared.db_session` — `get_worker_session`, `get_blob_storage_uri`, `get_db_url`, `get_session_factory`
- `ctx_worker_shared.contract_validator` — `validate_capability_input`, `validate_capability_output`, `ContractViolationError`, `validate_contract`
- `ctx_worker_shared.adapter_base` — `resolve_adapter`, `register_adapter`, `InputAdapter`
- `ctx_worker_shared.worker_base` — `with_capability`

(`data_source_worker` was an out-of-v1 inline worker, not a library — it is not
part of this package on the `v1` branch.)

## Build

```powershell
.\build-shared.ps1
# -> dist\ctx_worker_shared-1.0.0-py3-none-any.whl
```

Then copy that `.whl` into each worker's deploy bundle. The worker's
`requirements_*.txt` references it by local path:

```
./ctx_worker_shared-1.0.0-py3-none-any.whl
```

Bump `[project].version` in `pyproject.toml` on any change, rebuild, and
redistribute the wheel to every bundle (it is a copied file — there is no
auto-update).
