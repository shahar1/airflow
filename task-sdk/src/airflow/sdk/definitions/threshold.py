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
"""Threshold trigger rules: ordered atoms that each compare a count of upstream states with a bound."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Literal

import attrs

ALL = "all"
MAX_N = 10000
MAX_ATOMS = 6
OPS = (">=", ">", "<=", "<", "==")
ROUTES = ("upstream_failed", "skipped", None)

#: Every upstream state an :class:`Atom` can count; the ``count`` of an atom is a subset of it.
DONE = frozenset({"success", "failed", "upstream_failed", "skipped", "removed"})
#: The failed subset of ``DONE``: ``failed`` plus ``upstream_failed``.
FAILED_ANY = frozenset({"failed", "upstream_failed"})


def _normalize_states(states: str | Iterable[str]) -> frozenset[str]:
    if isinstance(states, str):
        states = (states,)
    return frozenset(getattr(state, "value", state) for state in states)


def _to_atoms(atoms: Atom | Iterable[Atom]) -> tuple[Atom, ...]:
    if isinstance(atoms, Atom):
        return (atoms,)
    return tuple(atoms)


@attrs.frozen
class Atom:
    """
    Compare the number of upstreams in any of the ``count`` states with ``n``.

    :param count: The upstream states to count, from ``DONE``.
    :param op: One of ``>=``, ``>``, ``<=``, ``<``, ``==``.
    :param n: A count in ``[0, 10000]``, or ``"all"`` for the number of upstreams.
    :param route: The state the task instance gets when this atom fails first.
    """

    count: frozenset[str] = attrs.field(
        converter=_normalize_states, repr=lambda states: f"frozenset({sorted(states)!r})"
    )
    op: Literal[">=", ">", "<=", "<", "=="]
    n: int | Literal["all"]
    route: Literal["upstream_failed", "skipped"] | None = "upstream_failed"

    def __attrs_post_init__(self) -> None:
        if not self.count:
            raise ValueError(f"{self!r} counts no states")
        if unknown := self.count - DONE:
            raise ValueError(
                f"{self!r} counts unknown states {sorted(unknown)}; use states from {sorted(DONE)}"
            )
        if self.op not in OPS:
            raise ValueError(f"{self!r} has unknown op {self.op!r}; use one of {OPS}")
        if self.n != ALL and (isinstance(self.n, bool) or not isinstance(self.n, int)):
            raise ValueError(f"{self!r} needs n to be an int or {ALL!r}")
        if isinstance(self.n, int) and not 0 <= self.n <= MAX_N:
            raise ValueError(f"{self!r} needs n between 0 and {MAX_N}")
        if self.route not in ROUTES:
            raise ValueError(f"{self!r} has unknown route {self.route!r}; use one of {ROUTES}")
        lo, hi = self.get_feasible_interval()
        if lo > hi:
            raise ValueError(f"{self!r} can never hold")

    def get_feasible_interval(self) -> tuple[float, float]:
        """
        Return the counts ``[lo, hi]`` for which the comparison holds.

        For ``n == "all"`` the bounds are offsets from the number of upstreams, which no count exceeds.
        """
        n, floor, ceiling = (0, -math.inf, 0.0) if self.n == ALL else (int(self.n), 0, math.inf)
        lo = {">=": n, ">": n + 1, "==": n}.get(self.op, floor)
        hi = {"<=": n, "<": n - 1, "==": n}.get(self.op, ceiling)
        return max(lo, floor), min(hi, ceiling)


@attrs.frozen
class Threshold:
    """
    A trigger rule that passes when every atom holds; the first failing atom routes the task instance.

    :param atoms: The atoms, in routing order.
    :param shrink_route: Route an expanded task instance whose map index is beyond the number of
        successful upstreams to ``removed`` when some upstreams were removed.
    """

    atoms: tuple[Atom, ...] = attrs.field(converter=_to_atoms)
    shrink_route: bool = False

    def __attrs_post_init__(self) -> None:
        if len(self.atoms) > MAX_ATOMS:
            raise ValueError(f"A Threshold takes at most {MAX_ATOMS} atoms, got {len(self.atoms)}")
        for i, atom in enumerate(self.atoms):
            if not isinstance(atom, Atom):
                raise ValueError(f"Threshold atoms must be Atom instances, got {atom!r}")
            lo, hi = atom.get_feasible_interval()
            for other in self.atoms[:i]:
                if other.count != atom.count or (other.n == ALL) != (atom.n == ALL):
                    continue
                other_lo, other_hi = other.get_feasible_interval()
                if max(lo, other_lo) > min(hi, other_hi):
                    raise ValueError(f"{other!r} and {atom!r} can never both hold")
