"""
InputAdapter ABC + global adapter registry.

Every worker's adapters subclass InputAdapter and self-register via the
@register_adapter decorator. The registry is module-level; importing the
worker's adapters/__init__.py is sufficient to register them all.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple


@dataclass
class ReadConfigField:
    """One configurable field that the adapter requires the user to fill in."""
    name: str
    type: str                  # 'string' | 'state_path' | 'integer' | 'boolean' | etc.
    required: str = "always"   # 'always' | 'optional'
    description: str = ""
    default: Any = None
    # When type == 'state_path', restrict picker to upstream paths of this type:
    type_filter: Optional[str] = None  # e.g. 'string', 'array', 'object'


class InputAdapter(abc.ABC):
    """Base class. Concrete adapters override stream() and writeback()."""

    # Identity (set by subclass or by @register_adapter)
    name: str = ""
    label: str = ""
    description: str = ""

    # Discovery hints
    auto_detect_when_upstream_is: List[str] = []
    iteration: str = "items"  # 'items' (1-to-1 or 1-to-N per record) | 'aggregate'

    # Declared mappings the user must supply (rendered in Source & Shape tab)
    read_config: List[ReadConfigField] = []

    # Writeback hint for the Outputs tab
    writeback_description: str = ""

    @abc.abstractmethod
    def stream(
        self, storage: Any, ctx: Dict[str, Any]
    ) -> Iterator[Tuple[Any, Any, Any]]:
        """Yield (key, payload, source_row) tuples.

        - key: opaque identifier the writeback can use to rewrite the record
        - payload: the data the core function will operate on (e.g., a string)
        - source_row: the full source record (for writeback to preserve other fields)
        """

    @abc.abstractmethod
    def writeback(
        self,
        storage: Any,
        ctx: Dict[str, Any],
        key: Any,
        result: Any,
        source_row: Any,
    ) -> None:
        """Persist the worker's result. Must be idempotent."""

    def flush(self, storage: Any, ctx: Dict[str, Any]) -> None:
        """Optional post-loop hook for adapters that batch writes.

        Called by the worker after the stream/writeback loop completes.
        Default is a no-op; override in adapters that accumulate state in ctx.
        """

    def to_schema_dict(self) -> Dict[str, Any]:
        """Serialise for inclusion in CAPABILITY_SCHEMA / API responses."""
        return {
            "name": self.name,
            "label": self.label,
            "description": self.description,
            "auto_detect_when_upstream_is": list(self.auto_detect_when_upstream_is),
            "iteration": self.iteration,
            "writeback_description": self.writeback_description,
            "read_config": [
                {
                    "name": f.name,
                    "type": f.type,
                    "required": f.required,
                    "description": f.description,
                    "default": f.default,
                    "type_filter": f.type_filter,
                }
                for f in self.read_config
            ],
        }


# ── Registry ──────────────────────────────────────────────────────────────────

_REGISTRY: Dict[Tuple[str, str], InputAdapter] = {}


def register_adapter(worker: str, name: str):
    """Class decorator: register an adapter under (worker, name)."""
    def deco(cls):
        instance = cls()
        if not instance.name:
            instance.name = name
        _REGISTRY[(worker, name)] = instance
        return cls
    return deco


def get_adapters_for_worker(worker: str) -> List[InputAdapter]:
    """Return all registered adapters for a given worker, in registration order."""
    return [a for (w, _), a in _REGISTRY.items() if w == worker]


def resolve_adapter(worker: str, name: str) -> InputAdapter:
    """Look up a registered adapter. Raises KeyError if not found."""
    if (worker, name) not in _REGISTRY:
        available = [n for (w, n) in _REGISTRY if w == worker]
        raise KeyError(
            f"Adapter '{name}' not registered for worker '{worker}'. "
            f"Available: {available}"
        )
    return _REGISTRY[(worker, name)]
