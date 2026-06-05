"""
Data Source Worker — asserts that pre-existing artifacts are present for a folder
and registers their handles into run_state so downstream nodes can wire to them.

Used for mid-pipeline entry points (e.g. skip parse/TP and start from chunks).
"""
from __future__ import annotations

import logging
import os
import sys as _sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
if _REPO_ROOT not in _sys.path:
    _sys.path.insert(0, _REPO_ROOT)

import requests
from celery import Celery
from kombu import Queue
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from workers.shared.db_session import get_worker_session
from workers.shared.storage import StorageClient

logger = logging.getLogger("data_source_worker")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))
    logger.addHandler(_handler)


class DataSourceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    broker_url: str = Field(
        default="pyamqp://guest:guest@localhost:5672//",
        validation_alias=AliasChoices("BROKER_URL", "CELERY_BROKER_URL"),
    )
    result_backend: str = Field(
        default="rpc://",
        validation_alias=AliasChoices("RESULT_BACKEND", "CELERY_RESULT_BACKEND"),
    )
    queues: str = Field(
        default="data_source",
        validation_alias=AliasChoices("DATA_SOURCE_QUEUES", "QUEUES"),
    )
    worker_results_url: str = Field(
        default="http://localhost:8000/process/worker-results",
        validation_alias=AliasChoices("WORKER_RESULTS_URL"),
    )
    data_backbone_dir: str = Field(
        default="/data/backbone",
        validation_alias=AliasChoices("DATA_BACKBONE_DIR"),
    )
    log_level: str = Field(default="INFO", validation_alias=AliasChoices("LOG_LEVEL"))


_settings = DataSourceSettings()
logger.setLevel(getattr(logging, _settings.log_level.upper(), logging.INFO))

celery_app = Celery("data_source_worker", broker=_settings.broker_url, backend=_settings.result_backend)

_queue_names = [q.strip() for q in _settings.queues.split(",") if q.strip()]
celery_app.conf.task_queues = tuple(Queue(name) for name in _queue_names)
celery_app.conf.task_default_queue = _queue_names[0]
celery_app.conf.task_acks_late = True
celery_app.conf.worker_prefetch_multiplier = 1

CAPABILITY_SCHEMA = {
    "worker": "data_source",
    "version": "1.0",
    "input_fields": {
        "folder_id": {"type": "string",  "required": "always",  "implicit": True},
        "asserts": {
            "type":        "array",
            "required":    "always",
            "description": "Artifact types to assert exist, e.g. ['chunk', 'parser_page']",
        },
    },
    "output_fields": {
        "folder_id": {"type": "string", "always_present": True},
    },
    # artifact_outputs are dynamically driven by the 'asserts' config list;
    # the validator treats each asserted type as a declared output handle.
}

_HANDLE_KEY: dict[str, str] = {
    "parser_page": "pages",
    "chunk":       "chunks",
}


@celery_app.task(name="data_source_worker.data_source_task", bind=True, max_retries=0)
def data_source_task(self, payload: dict) -> dict:
    folder_id: str = payload["folder_id"]
    dag_id: str    = payload.get("dag_id", "")
    run_id: str    = payload.get("run_id", "")
    task_id: str   = self.request.id or ""
    asserts: list  = payload.get("asserts") or []

    logger.info(
        "data_source_worker.start folder_id=%s dag_id=%s run_id=%s asserts=%s",
        folder_id, dag_id, run_id, asserts,
    )

    result: dict = {"folder_id": folder_id}

    try:
        with get_worker_session() as db:
            storage = StorageClient(
                blob_uri=os.environ.get("BLOB_STORAGE_URI", ""),
                db=db,
            )

            for artifact_type in asserts:
                if artifact_type == "parser_page":
                    count = storage.count_parser_pages(folder_id)
                elif artifact_type == "chunk":
                    count = storage.count_chunks(folder_id)
                else:
                    count = 0

                if count == 0:
                    raise ValueError(
                        f"data_source assertion failed: no '{artifact_type}' rows found "
                        f"for folder_id='{folder_id}'. Run the upstream pipeline first."
                    )

                handle_name = _HANDLE_KEY.get(artifact_type, artifact_type)
                result[handle_name] = {
                    "folder_id":     folder_id,
                    "artifact_type": artifact_type,
                    "count":         count,
                }
                logger.info(
                    "data_source_worker.assert_ok artifact_type=%s count=%d folder_id=%s",
                    artifact_type, count, folder_id,
                )

        _post_callback(task_id, folder_id, dag_id, run_id, result, error=None)

    except Exception as exc:  # noqa: BLE001
        logger.exception("data_source_worker.error folder_id=%s error=%s", folder_id, exc)
        _post_callback(task_id, folder_id, dag_id, run_id, result={}, error=str(exc))
        raise

    return result


def _post_callback(
    task_id: str,
    folder_id: str,
    dag_id: str,
    run_id: str,
    result: dict,
    error: str | None,
) -> None:
    url = _settings.worker_results_url
    payload = {
        "task_id":   task_id,
        "folder_id": folder_id,
        "dag_id":    dag_id,
        "run_id":    run_id,
        "status":    "failed" if error else "success",
        "result":    result,
        "error":     error,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logger.warning("data_source_worker.callback_failed url=%s error=%s", url, exc)
