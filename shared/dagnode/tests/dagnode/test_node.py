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

from dataclasses import dataclass, field
from unittest import mock

import pytest

from airflow_shared.dagnode.node import GenericDAGNode


class Task:
    """Task type for tests."""


class GraphNode(GenericDAGNode):
    """Minimal node that participates in a Dag graph keyed by ``task_id``."""

    def __init__(self, task_id: str) -> None:
        super().__init__()
        self.task_id = task_id
        self.dag: GraphDag | None = None

    @property
    def node_id(self) -> str:
        return self.task_id


class GraphDag:
    """Dag holding ``GraphNode`` instances, wiring edges from a downstream map."""

    dag_id = "graph_dag"

    def __init__(self, edges: dict[str, set[str]]) -> None:
        self.task_dict: dict[str, GraphNode] = {tid: GraphNode(tid) for tid in edges}
        for tid, downstream in edges.items():
            node = self.task_dict[tid]
            node.dag = self
            node.downstream_task_ids = set(downstream)
            for child in downstream:
                self.task_dict[child].upstream_task_ids.add(tid)

    def get_task(self, tid: str) -> GraphNode:
        return self.task_dict[tid]


@dataclass
class TaskGroup:
    """Task group type for tests."""

    prefix_group_id: bool
    node_id: str = field(init=False, default="test_group_id")


class Dag:
    """Dag type for tests."""

    dag_id = "test_dag_id"


class ConcreteDAGNode(GenericDAGNode[Dag, Task, TaskGroup]):
    """Concrete DAGNode variant for tests."""

    dag: Dag | None = None
    task_group: TaskGroup | None = None

    @property
    def node_id(self) -> str:
        return "test_group_id.test_node_id"


class TestDAGNode:
    @pytest.fixture
    def node(self):
        return ConcreteDAGNode()

    def test_log(self, node: ConcreteDAGNode) -> None:
        assert node._cached_logger is None
        with mock.patch("structlog.get_logger") as mock_get_logger:
            log = node.log
        assert log is node._cached_logger
        assert mock_get_logger.mock_calls == [mock.call("tests.dagnode.test_node.ConcreteDAGNode")]

    def test_dag_id(self, node: ConcreteDAGNode) -> None:
        assert node.dag is None
        assert node.dag_id == "_in_memory_dag_"
        node.dag = Dag()
        assert node.dag_id == "test_dag_id"

    @pytest.mark.parametrize(
        ("prefix_group_id", "expected_label"),
        [(True, "test_node_id"), (False, "test_group_id.test_node_id")],
    )
    def test_label(self, node: ConcreteDAGNode, prefix_group_id: bool, expected_label: str) -> None:
        assert node.task_group is None
        assert node.label == "test_group_id.test_node_id"
        node.task_group = TaskGroup(prefix_group_id)
        assert node.label == expected_label


class TestGetRelativeWeightSums:
    # Diamond with a tail: a -> b, a -> c, b -> d, c -> d, d -> e
    EDGES = {
        "a": {"b", "c"},
        "b": {"d"},
        "c": {"d"},
        "d": {"e"},
        "e": set(),
    }

    @staticmethod
    def _naive(node, weights, upstream):
        return sum(weights[rel] for rel in node.get_flat_relative_ids(upstream=upstream))

    @pytest.mark.parametrize("upstream", [False, True])
    @pytest.mark.parametrize(
        "weights",
        [
            pytest.param({k: 1 for k in EDGES}, id="uniform"),
            pytest.param({"a": 3, "b": 5, "c": 7, "d": 11, "e": 13}, id="varied"),
        ],
    )
    def test_matches_per_task_traversal(self, weights: dict[str, int], upstream: bool) -> None:
        dag = GraphDag(self.EDGES)
        sums = dag.task_dict["a"].get_relative_weight_sums(weights, upstream=upstream)
        assert sums.keys() == dag.task_dict.keys()
        # The batch result must equal summing get_flat_relative_ids on each task.
        for task_id, node in dag.task_dict.items():
            assert sums[task_id] == self._naive(node, weights, upstream)

    def test_downstream_values(self) -> None:
        dag = GraphDag(self.EDGES)
        weights = {"a": 3, "b": 5, "c": 7, "d": 11, "e": 13}
        sums = dag.task_dict["a"].get_relative_weight_sums(weights, upstream=False)
        assert sums["a"] == 5 + 7 + 11 + 13  # b, c, d, e
        assert sums["b"] == 11 + 13  # d, e
        assert sums["d"] == 13  # e
        assert sums["e"] == 0

    def test_upstream_values(self) -> None:
        dag = GraphDag(self.EDGES)
        weights = {"a": 3, "b": 5, "c": 7, "d": 11, "e": 13}
        sums = dag.task_dict["a"].get_relative_weight_sums(weights, upstream=True)
        assert sums["e"] == 3 + 5 + 7 + 11  # a, b, c, d
        assert sums["d"] == 3 + 5 + 7  # a, b, c
        assert sums["a"] == 0

    def test_missing_weight_counts_zero(self) -> None:
        dag = GraphDag(self.EDGES)
        # "e" omitted from weights -> treated as 0.
        sums = dag.task_dict["a"].get_relative_weight_sums({"b": 5, "c": 7, "d": 11}, upstream=False)
        assert sums["d"] == 0  # only relative is e, which has no weight
        assert sums["a"] == 5 + 7 + 11

    def test_no_dag_returns_empty(self) -> None:
        assert ConcreteDAGNode().get_relative_weight_sums({}) == {}

    def test_cycle_does_not_hang(self) -> None:
        # Not a valid Dag (cycles are rejected at parse time), so the exact sums are
        # best-effort; the only contract is that the traversal terminates.
        dag = GraphDag({"a": {"b"}, "b": {"a"}})
        sums = dag.task_dict["a"].get_relative_weight_sums({"a": 2, "b": 3}, upstream=False)
        assert sums.keys() == {"a", "b"}
        assert all(isinstance(value, int) and value >= 0 for value in sums.values())
