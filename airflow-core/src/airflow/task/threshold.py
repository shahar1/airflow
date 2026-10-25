#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Wire format of Threshold trigger rules and the plan the scheduler compiles from it.

``threshold/2:`` + atoms joined by ``;`` in authored order + ``:shrink_route=true|false``. An atom is
``<states><op><n>-><route>``: the counted states sorted and joined by ``+``, an op from ``OPS``, a count
or ``all``, and ``upstream_failed``, ``skipped`` or ``none``.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

import attrs

from airflow.utils.state import TaskInstanceState

PREFIX = "threshold/"
_VERSION_PREFIX = "threshold/2:"
MAX_N = 10000
MAX_ATOMS = 6
_DONE = frozenset({"success", "failed", "upstream_failed", "skipped", "removed"})
_LOWER_BOUND_OPS = (">=", ">")
_ROUTES = {
    "upstream_failed": TaskInstanceState.UPSTREAM_FAILED,
    "skipped": TaskInstanceState.SKIPPED,
    "none": None,
}
_RULE_RE = re.compile(r"threshold/2:(?P<atoms>.*):shrink_route=(?P<shrink>true|false)")
_ATOM_RE = re.compile(
    r"(?P<states>[a-z_]+(?:\+[a-z_]+)*)(?P<op>>=|<=|==|>|<)(?P<n>0|[1-9]\d*|all)"
    r"->(?P<route>upstream_failed|skipped|none)"
)

CompiledAtom = tuple[tuple[str, ...], str, int | None, TaskInstanceState | None]


@attrs.frozen
class CompiledThreshold:
    """
    A parsed Threshold trigger rule.

    :param atoms: ``(states, op, n, route)`` per atom in authored order; ``n`` is None for ``all``.
    :param shrink_route: Whether surplus expanded task instances are routed to ``removed``.
    :param is_fast_triggered: Every atom is a lower bound, so the rule can pass before all upstreams finish.
    :param defers_to_expanded_tis: Some but not all atoms are lower bounds.
    """

    atoms: tuple[CompiledAtom, ...]
    shrink_route: bool
    is_fast_triggered: bool
    defers_to_expanded_tis: bool


# Any, not airflow.sdk.Threshold: core must not import the Task SDK, and that class is the only producer.
def encode_threshold(threshold: Any) -> str:
    """Return the wire string of an SDK ``Threshold``; atoms keep their authored order."""
    atoms = ";".join(
        f"{'+'.join(sorted(atom.count))}{atom.op}{atom.n}->{atom.route or 'none'}" for atom in threshold.atoms
    )
    return f"{_VERSION_PREFIX}{atoms}:shrink_route={str(threshold.shrink_route).lower()}"


@lru_cache(maxsize=1024)
def parse_threshold(value: str) -> CompiledThreshold:
    """
    Compile a Threshold wire string.

    :raises ValueError: if the string is not a ``threshold/2:`` rule in the canonical grammar.
    """
    if not value.startswith(_VERSION_PREFIX):
        raise ValueError(f"Unsure how to evaluate trigger rule {value!r}")
    if (match := _RULE_RE.fullmatch(value)) is None:
        raise ValueError(f"Malformed Threshold trigger rule {value!r}")
    atoms = []
    for raw in match["atoms"].split(";") if match["atoms"] else ():
        if (atom := _ATOM_RE.fullmatch(raw)) is None:
            raise ValueError(f"Malformed atom {raw!r} in Threshold trigger rule {value!r}")
        states = tuple(atom["states"].split("+"))
        if not _DONE.issuperset(states) or list(states) != sorted(set(states)):
            raise ValueError(
                f"Atom {raw!r} in {value!r} must count distinct states from {sorted(_DONE)} in sorted order"
            )
        n = None if atom["n"] == "all" else int(atom["n"])
        if n is not None and n > MAX_N:
            raise ValueError(f"Atom {raw!r} in {value!r} needs n between 0 and {MAX_N}")
        atoms.append((states, atom["op"], n, _ROUTES[atom["route"]]))
    if len(atoms) > MAX_ATOMS:
        raise ValueError(f"Threshold trigger rule {value!r} has more than {MAX_ATOMS} atoms")
    is_fast_triggered = bool(atoms) and all(op in _LOWER_BOUND_OPS for _, op, _, _ in atoms)
    return CompiledThreshold(
        atoms=tuple(atoms),
        shrink_route=match["shrink"] == "true",
        is_fast_triggered=is_fast_triggered,
        defers_to_expanded_tis=not is_fast_triggered and any(op in _LOWER_BOUND_OPS for _, op, _, _ in atoms),
    )
