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

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

import structlog

if TYPE_CHECKING:
    import sys
    from collections.abc import Collection, Iterable

    # Replicate `airflow.typing_compat.Self` to avoid illegal imports
    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

    from ..logging.types import Logger


class DagProtocol(Protocol):
    """Protocol defining the minimum interface required for Dag generic type."""

    dag_id: str
    task_dict: dict[str, Any]

    def get_task(self, tid: str) -> Any:
        """Retrieve a task by its task ID."""
        ...


class TaskProtocol(Protocol):
    """Protocol defining the minimum interface required for Task generic type."""

    task_id: str
    is_setup: bool
    is_teardown: bool
    downstream_list: Iterable[Self]
    downstream_task_ids: set[str]


class TaskGroupProtocol(Protocol):
    """Protocol defining the minimum interface required for TaskGroup generic type."""

    node_id: str
    prefix_group_id: bool


Dag = TypeVar("Dag", bound=DagProtocol)
Task = TypeVar("Task", bound=TaskProtocol)
TaskGroup = TypeVar("TaskGroup", bound=TaskGroupProtocol)


class GenericDAGNode(Generic[Dag, Task, TaskGroup]):
    """
    Generic class for a node in the graph of a workflow.

    A node may be an operator or task group, either mapped or unmapped.
    """

    dag: Dag | None
    task_group: TaskGroup | None
    downstream_group_ids: set[str | None]
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]

    _log_config_logger_name: str | None = None
    _logger_name: str | None = None
    _cached_logger: Logger | None = None

    def __init__(self):
        super().__init__()
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()

    @property
    def log(self) -> Logger:
        if self._cached_logger is not None:
            return self._cached_logger

        typ = type(self)

        logger_name: str = (
            self._logger_name if self._logger_name is not None else f"{typ.__module__}.{typ.__qualname__}"
        )

        if self._log_config_logger_name:
            logger_name = (
                f"{self._log_config_logger_name}.{logger_name}"
                if logger_name
                else self._log_config_logger_name
            )

        self._cached_logger = structlog.get_logger(logger_name)
        return self._cached_logger

    @property
    def dag_id(self) -> str:
        if self.dag:
            return self.dag.dag_id
        return "_in_memory_dag_"

    @property
    def node_id(self) -> str:
        raise NotImplementedError()

    @property
    def label(self) -> str | None:
        tg = self.task_group
        if tg and tg.node_id and tg.prefix_group_id:
            # "task_group_id.task_id" -> "task_id"
            return self.node_id[len(tg.node_id) + 1 :]
        return self.node_id

    @property
    def upstream_list(self) -> Iterable[Task]:
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a Dag yet")
        return [self.dag.get_task(tid) for tid in self.upstream_task_ids]

    @property
    def downstream_list(self) -> Iterable[Task]:
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a Dag yet")
        return [self.dag.get_task(tid) for tid in self.downstream_task_ids]

    def has_dag(self) -> bool:
        return self.dag is not None

    def get_dag(self) -> Dag | None:
        return self.dag

    def get_direct_relative_ids(self, upstream: bool = False) -> set[str]:
        """Get set of the direct relative ids to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        return self.downstream_task_ids

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[Task]:
        """Get list of the direct relatives to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_list
        return self.downstream_list

    def get_flat_relative_ids(self, *, upstream: bool = False, depth: int | None = None) -> set[str]:
        """
        Get a flat set of relative IDs, upstream or downstream.

        Will recurse each relative found in the direction specified.

        :param upstream: Whether to look for upstream or downstream relatives.
         :param depth: Maximum number of levels to traverse. If None, traverses all levels.
            Must be non-negative.
        """
        if depth is not None and depth < 0:
            raise ValueError(f"depth must be non-negative, got {depth}")

        dag = self.get_dag()
        if not dag:
            return set()

        relatives: set[str] = set()

        # This is intentionally implemented as a loop, instead of calling
        # get_direct_relative_ids() recursively, since Python has significant
        # limitation on stack level, and a recursive implementation can blow up
        # if a DAG contains very long routes.
        task_ids_to_trace = self.get_direct_relative_ids(upstream)
        levels_remaining = depth
        while task_ids_to_trace:
            # if depth is set we have bounded traversal and should break when
            # there are no more levels remaining
            if levels_remaining is not None and levels_remaining <= 0:
                break
            task_ids_to_trace_next: set[str] = set()
            for task_id in task_ids_to_trace:
                if task_id in relatives:
                    continue
                task_ids_to_trace_next.update(dag.task_dict[task_id].get_direct_relative_ids(upstream))
                relatives.add(task_id)
            task_ids_to_trace = task_ids_to_trace_next
            if levels_remaining is not None:
                levels_remaining -= 1

        return relatives

    def get_relative_weight_sums(self, weights: dict[str, int], *, upstream: bool = False) -> dict[str, int]:
        """
        Sum ``weights`` over the transitive relatives of *every* task in the Dag.

        Returns a mapping of ``task_id`` to the sum of ``weights`` over all of that
        task's transitive relatives (upstream or downstream), excluding the task
        itself. This is the batch equivalent of calling :meth:`get_flat_relative_ids`
        once per task and summing, but it traverses the whole graph once instead of
        once per task, turning ``n`` independent traversals (``O(n * (V + E))``) into
        a single pass.

        Reachable sets are deduplicated with integer bitsets -- one task per bit --
        so each edge contributes a single C-level ``OR`` rather than a Python set
        union. When every weight is equal (the common case: ``priority_weight``
        defaults to 1 and is rarely customised) the per-task sum collapses to a
        single C-level ``popcount``, which is dramatically faster than iterating the
        reachable set.

        Unlike :meth:`get_flat_relative_ids`, this always traverses to full depth.

        :param weights: Mapping of ``task_id`` to its weight. Missing tasks count 0.
        :param upstream: Whether to look for upstream or downstream relatives.
        """
        dag = self.get_dag()
        if not dag:
            return {}

        # Assign each task a bit position so reachable sets can be deduped with int OR.
        task_ids = list(dag.task_dict)
        index = {task_id: i for i, task_id in enumerate(task_ids)}
        children = [
            [index[rel] for rel in dag.task_dict[task_id].get_direct_relative_ids(upstream)]
            for task_id in task_ids
        ]

        reach = [0] * len(task_ids)  # reach[i] = bitset of i's transitive relatives
        computed = bytearray(len(task_ids))
        # Iterative post-order DFS with an explicit stack rather than recursion:
        # Python's recursion limit makes a recursive walk blow up on very long
        # routes (same reasoning as get_flat_relative_ids above). A node is only
        # finalized once all of its direct relatives are, so its bitset is the OR of
        # theirs. Each task is pushed once to expand its relatives and once
        # (children_done) to assemble its bitset.
        for root in range(len(task_ids)):
            if computed[root]:
                continue
            stack: list[tuple[int, bool]] = [(root, False)]
            # Tasks currently on the DFS path. A valid Dag is acyclic, so a relative
            # is never re-encountered while on the path; guarding on it truncates
            # back edges so a malformed (cyclic) graph still terminates.
            on_path: set[int] = set()
            while stack:
                i, children_done = stack.pop()
                if children_done:
                    on_path.discard(i)
                    bits = 0
                    for child in children[i]:
                        bits |= (1 << child) | reach[child]
                    reach[i] = bits
                    computed[i] = 1
                elif not computed[i] and i not in on_path:
                    on_path.add(i)
                    stack.append((i, True))
                    stack.extend(
                        (child, False)
                        for child in children[i]
                        if not computed[child] and child not in on_path
                    )

        weight_list = [weights.get(task_id, 0) for task_id in task_ids]
        uniform_weight = weight_list[0] if len(set(weight_list)) == 1 else None
        sums: dict[str, int] = {}
        for i, task_id in enumerate(task_ids):
            bitset = reach[i]
            if uniform_weight is not None:
                # Every relative has the same weight, so the sum is weight * count.
                sums[task_id] = uniform_weight * bitset.bit_count()
            else:
                total = 0
                while bitset:
                    low = bitset & -bitset
                    total += weight_list[low.bit_length() - 1]
                    bitset ^= low
                sums[task_id] = total
        return sums

    def get_flat_relatives(self, upstream: bool = False, depth: int | None = None) -> Collection[Task]:
        """
        Get a flat list of relatives, either upstream or downstream.

        :param upstream: Whether to look for upstream or downstream relatives.
        :param depth: Maximum number of levels to traverse. If None, traverses all levels.
            Must be non-negative.
        """
        dag = self.get_dag()
        if not dag:
            return set()
        return [
            dag.task_dict[task_id] for task_id in self.get_flat_relative_ids(upstream=upstream, depth=depth)
        ]

    def get_upstreams_follow_setups(self, depth: int | None = None) -> Iterable[Task]:
        """
        All upstreams and, for each upstream setup, its respective teardowns.

        :param depth: Maximum number of levels to traverse. If None, traverses all levels.
            Must be non-negative.
        """
        for task in self.get_flat_relatives(upstream=True, depth=depth):
            yield task
            if task.is_setup:
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups_and_teardowns(self) -> Iterable[Task]:
        """
        Only *relevant* upstream setups and their teardowns.

        This method is meant to be used when we are clearing the task (non-upstream) and we need
        to add in the *relevant* setups and their teardowns.

        Relevant in this case means, the setup has a teardown that is downstream of ``self``,
        or the setup has no teardowns.
        """
        downstream_teardown_ids = {
            x.task_id for x in self.get_flat_relatives(upstream=False) if x.is_teardown
        }
        for task in self.get_flat_relatives(upstream=True):
            if not task.is_setup:
                continue
            has_no_teardowns = not any(x.is_teardown for x in task.downstream_list)
            # if task has no teardowns or has teardowns downstream of self
            if has_no_teardowns or task.downstream_task_ids.intersection(downstream_teardown_ids):
                yield task
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups(self) -> Iterable[Task]:
        """
        Return relevant upstream setups.

        This method is meant to be used when we are checking task dependencies where we need
        to wait for all the upstream setups to complete before we can run the task.
        """
        for task in self.get_upstreams_only_setups_and_teardowns():
            if task.is_setup:
                yield task
