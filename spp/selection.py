"""Which nodes / zones a run should load.

A `Selection` is an immutable, deduplicated, ordered set of members of one
dimension, carrying a label so logs and results can be attributed to a group.

Groups live in `config/groups.json` and may reference other groups with a
leading `@`:

    {
      "settlement_location": {
        "nppd": ["NPPD_BPWN_NPPD"],
        "kmea": ["KMEA_EMP2_SECI"],
        "core": ["@nppd", "@kmea"]
      }
    }

Expansion is transitive and cycle-checked, so `core` resolves to both nodes and
a self-referential group fails loudly at resolve time rather than recursing.

Changing what gets loaded is a JSON edit or an env var / Airflow Variable — no
code deploy, and no re-download, since `run.py` fetches once and filters the
payload per selection in memory.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .config import settings
from .logging_setup import get_logger

log = get_logger("spp.selection")

SETTLEMENT_LOCATION = "settlement_location"
RESERVE_ZONE = "reserve_zone"
DIMENSIONS = (SETTLEMENT_LOCATION, RESERVE_ZONE)

GroupMap = Mapping[str, Mapping[str, Sequence[str]]]


class UnknownGroup(KeyError):
    """A group name that is not defined for this dimension."""


class EmptySelection(ValueError):
    """A selection that would load nothing — always a configuration mistake."""


class CircularGroupReference(ValueError):
    """Groups that reference each other in a cycle."""


@lru_cache(maxsize=1)
def _load_groups_cached(path: str, mtime: float) -> dict[str, dict[str, list[str]]]:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return {dim: {name: list(members) for name, members in groups.items()}
            for dim, groups in raw.items()}


def load_groups(path: str | Path | None = None) -> dict[str, dict[str, list[str]]]:
    """Read config/groups.json. Cached on (path, mtime) so edits are picked up."""
    target = Path(path) if path else settings.groups_file
    if not target.exists():
        log.warning("groups file missing",
                    extra={"event": "selection.no_groups_file",
                           "path": str(target)})
        return {dim: {} for dim in DIMENSIONS}
    return _load_groups_cached(str(target), target.stat().st_mtime)


def _expand(dimension: str, name: str, groups: Mapping[str, Sequence[str]],
            seen: tuple[str, ...] = ()) -> list[str]:
    if name in seen:
        raise CircularGroupReference(
            f"{dimension}: circular group reference {' -> '.join((*seen, name))}"
        )
    if name not in groups:
        raise UnknownGroup(
            f"{dimension}: unknown group {name!r} "
            f"(known: {', '.join(sorted(groups)) or 'none'})"
        )
    out: list[str] = []
    for entry in groups[name]:
        entry = str(entry).strip()
        if entry.startswith("@"):
            out.extend(_expand(dimension, entry[1:], groups, (*seen, name)))
        elif entry:
            out.append(entry)
    return out


def _dedup(members: Iterable[str]) -> tuple[str, ...]:
    """Preserve first-seen order; duplicates across referenced groups collapse."""
    seen: dict[str, None] = {}
    for member in members:
        cleaned = str(member).strip()
        if cleaned:
            seen.setdefault(cleaned, None)
    return tuple(seen)


@dataclass(frozen=True)
class Selection:
    dimension: str
    members: tuple[str, ...]
    label: str = "adhoc"

    def __post_init__(self) -> None:
        if not self.members:
            raise EmptySelection(f"{self.dimension}: selection {self.label!r} is empty")

    def __len__(self) -> int:
        return len(self.members)

    def __iter__(self):
        return iter(self.members)

    @property
    def member_set(self) -> frozenset[str]:
        return frozenset(self.members)

    # ---------------------------------------------------------------- builders
    @classmethod
    def from_members(cls, dimension: str, members: Sequence[str],
                     label: str | None = None) -> "Selection":
        resolved = _dedup(members)
        if not resolved:
            raise EmptySelection(f"{dimension}: no members given")
        return cls(dimension, resolved, label or "adhoc")

    @classmethod
    def from_group(cls, dimension: str, name: str,
                   groups: GroupMap | None = None) -> "Selection":
        source = groups if groups is not None else load_groups()
        for_dim = source.get(dimension, {})
        return cls(dimension, _dedup(_expand(dimension, name, for_dim)), name)

    @classmethod
    def resolve(cls, dimension: str, *, members: Sequence[str] | None = None,
                groups: Sequence[str] | None = None,
                default_group: str | None = None,
                group_map: GroupMap | None = None) -> list["Selection"]:
        """Precedence: explicit members > named groups > default group."""
        if members:
            return [cls.from_members(dimension, members, "adhoc")]
        if groups:
            return [cls.from_group(dimension, name, group_map) for name in groups]
        if default_group:
            return [cls.from_group(dimension, default_group, group_map)]
        raise EmptySelection(f"{dimension}: nothing to select")


def selections_for_env(dimension: str, feed_name: str) -> list[Selection]:
    """Per-feed override, e.g. SPP_RT_LMP_GROUPS=nppd,kmea.

    Mirrors the Airflow Variable `spp_<feed>_groups` so a laptop run and a
    scheduled run can be made to load exactly the same subset.
    """
    raw = os.environ.get(f"SPP_{feed_name.upper()}_GROUPS", "").strip()
    names = [n.strip() for n in raw.split(",") if n.strip()]
    if not names:
        return []
    log.info("selection from environment",
             extra={"event": "selection.from_env", "feed": feed_name,
                    "groups": names})
    return [Selection.from_group(dimension, name) for name in names]


def describe(selection: Selection | None) -> dict[str, Any]:
    """Uniform log payload for a selection (or the whole payload)."""
    if selection is None:
        return {"group": "all", "members": None}
    return {"group": selection.label, "members": len(selection)}
