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

import math
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
# An atom as evaluated: its states' positions in COUNTED, its op's code in _OP_CODES, n, route and its label
# in the unmet reason.
PlannedAtom = tuple[tuple[int, ...], int, int | None, TaskInstanceState | None, str]
_OP_CODES = {"==": 0, ">=": 1, ">": 2, "<=": 3, "<": 4}
# The order of the counts ``CompiledThreshold.decide`` takes.
COUNTED = ("success", "skipped", "failed", "upstream_failed", "removed")


@attrs.frozen
class CompiledThreshold:
    """
    A parsed Threshold trigger rule.

    :param atoms: ``(states, op, n, route)`` per atom in authored order; ``n`` is None for ``all``.
    :param shrink_route: Whether surplus expanded task instances are routed to ``removed``.
    :param is_fast_triggered: Every atom is a lower bound, so the rule can pass before all upstreams finish;
        an atom whose bound is ``all`` is not a lower bound.
    :param defers_to_expanded_tis: Some but not all atoms are lower bounds, as for ``is_fast_triggered``.
    :param plan: ``atoms`` with states as positions in ``COUNTED``, ops as codes and reason labels built once.
    """

    atoms: tuple[CompiledAtom, ...]
    shrink_route: bool
    is_fast_triggered: bool
    defers_to_expanded_tis: bool
    plan: tuple[PlannedAtom, ...]

    def decide(
        self,
        counts: tuple[int, ...],
        done: int,
        upstream: int,
        flag_upstream_failed: bool,
        map_index: int,
    ) -> tuple[bool, TaskInstanceState | None, str]:
        """
        Decide the rule for one pass and describe what does not hold yet, in one walk over the atoms.

        :param counts: Finished upstreams per state, in the order of ``COUNTED``.
        :return: Whether every atom holds, the state to route the task instance to, if any, and the atoms
            that do not hold yet in authored order with their counts (empty when the rule passes).
        """
        pending = upstream - done if upstream > done else 0
        new_state = None
        routed = False
        unmet = sep = ""
        for positions, op, n, route, label in self.plan:
            now = 0
            for i in positions:
                now += counts[i]
            bound = upstream if n is None else n
            hi = now + pending
            # Skip an atom that holds for every completion of ``pending``; else whether it can still hold.
            if op == 0:  # ==
                if now <= bound <= hi:
                    if not pending:
                        continue
                    can_hold = True
                else:
                    can_hold = False
            elif op == 1:  # >=
                if now >= bound:
                    continue
                can_hold = hi >= bound
            elif op == 2:  # >
                if now > bound:
                    continue
                can_hold = hi > bound
            elif op == 3:  # <=
                if hi <= bound:
                    continue
                can_hold = now <= bound
            else:  # <
                if hi < bound:
                    continue
                can_hold = now < bound
            if pending:
                unmet = f"{unmet}{sep}{label}: {now} now, {pending} pending"
            else:
                unmet = f"{unmet}{sep}{label}: {now} now"
            sep = "; "
            # The first FALSE atom routes the rule, even when its route is none, which bars the shrink route.
            if not can_hold and flag_upstream_failed and not routed:
                routed = True
                # The all-skipped convention of the presets: a consumer of all-skipped upstreams skips.
                new_state = TaskInstanceState.SKIPPED if pending == 0 and done == counts[1] else route
        if (
            flag_upstream_failed
            and not routed
            and self.shrink_route
            and map_index >= 0
            and counts[4]
            and counts[0]
            and map_index >= counts[0]
        ):
            new_state = TaskInstanceState.REMOVED
        return not unmet, new_state, unmet


def _get_feasible_interval(op: str, n: int | None) -> tuple[float, float]:
    """
    Return the counts ``[lo, hi]`` for which ``op n`` holds, as the SDK's ``Atom.get_feasible_interval``.

    For ``all`` (``n`` is None) the bounds are offsets from the number of upstreams, which no count exceeds.
    """
    bound, floor, ceiling = (0, -math.inf, 0.0) if n is None else (n, 0, math.inf)
    lo = {">=": bound, ">": bound + 1, "==": bound}.get(op, floor)
    hi = {"<=": bound, "<": bound - 1, "==": bound}.get(op, ceiling)
    return max(lo, floor), min(hi, ceiling)


# Any, not airflow.sdk.Threshold: the check-sdk-imports-in-core prek hook forbids a new airflow.sdk import here.
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
    atoms: list[CompiledAtom] = []
    raws: list[str] = []
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
        lo, hi = _get_feasible_interval(atom["op"], n)
        if lo > hi:
            raise ValueError(f"Atom {raw!r} in {value!r} can never hold")
        atoms.append((states, atom["op"], n, _ROUTES[atom["route"]]))
        raws.append(raw)
    # Before the pairwise check below, which is quadratic in the number of atoms.
    if len(atoms) > MAX_ATOMS:
        raise ValueError(f"Threshold trigger rule {value!r} has more than {MAX_ATOMS} atoms")
    # Same rule as the SDK's Threshold: atoms on one state set, both counts or both ``all``, must overlap.
    for i, (states, op, n, _) in enumerate(atoms):
        lo, hi = _get_feasible_interval(op, n)
        for (other_states, other_op, other_n, _), other_raw in zip(atoms[:i], raws):
            if other_states != states or (other_n is None) != (n is None):
                continue
            other_lo, other_hi = _get_feasible_interval(other_op, other_n)
            if max(lo, other_lo) > min(hi, other_hi):
                raise ValueError(f"Atoms {other_raw!r} and {raws[i]!r} in {value!r} can never both hold")
    # A bound of ``all`` is not a lower bound: no subset of the upstreams can satisfy it.
    lower_bounds = [op in _LOWER_BOUND_OPS and n is not None for _, op, n, _ in atoms]
    is_fast_triggered = bool(atoms) and all(lower_bounds)
    return CompiledThreshold(
        atoms=tuple(atoms),
        shrink_route=match["shrink"] == "true",
        is_fast_triggered=is_fast_triggered,
        defers_to_expanded_tis=not is_fast_triggered and any(lower_bounds),
        plan=tuple(
            (
                tuple(map(COUNTED.index, states)),
                _OP_CODES[op],
                n,
                route,
                f"{'+'.join(states)}{op}{'all' if n is None else n}",
            )
            for states, op, n, route in atoms
        ),
    )
