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
Strip success_threshold from serialized Dags on downgrade; refuse Threshold rules.

Revision ID: 53092df95325
Revises: c71065a1b14f
Create Date: 2026-10-26 19:06:57.000000

"""

from __future__ import annotations

import json
import zlib
from collections.abc import Iterable
from typing import Any

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from airflow.utils.hashlib_wrapper import md5

revision = "53092df95325"
down_revision = "c71065a1b14f"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"

serialized_dag = sa.table(
    "serialized_dag",
    sa.column("id", sa.Uuid()),
    sa.column("dag_id", sa.String(250)),
    sa.column("dag_version_id", sa.Uuid()),
    sa.column("data", sa.JSON().with_variant(JSONB(), "postgresql")),
    sa.column("data_compressed", sa.LargeBinary()),
    sa.column("dag_hash", sa.String(32)),
)
dag_run = sa.table(
    "dag_run",
    sa.column("dag_id", sa.String(250)),
    sa.column("run_id", sa.String(250)),
    sa.column("state", sa.String(50)),
    sa.column("created_dag_version_id", sa.Uuid()),
)
task_instance = sa.table(
    "task_instance",
    sa.column("dag_id", sa.String(250)),
    sa.column("run_id", sa.String(250)),
    sa.column("dag_version_id", sa.Uuid()),
)


def upgrade():
    """Do nothing: format version 4 needs no schema change, only the downgrade has to rewrite it."""


def downgrade():
    """
    Rewrite format-version-4 serialized Dags as version 3, refusing while unfinished Dag runs use them.

    A version-5 row carries a Threshold trigger rule, which no preset can stand in for, so the downgrade
    refuses while any exists.
    """
    if context.is_offline_mode():
        print(
            """
            --  WARNING: serialized Dags at format version 4 (success_threshold) cannot be rewritten offline.
            --  Version-4 rows must be handled online: run this downgrade step online before the older
            --  Airflow version reads the serialized_dag table. Version-5 rows (Threshold trigger rules)
            --  block the downgrade until the Dags that own them are deleted.
            """
        )
        return

    conn = op.get_bind()
    # Uncompressed rows are filtered in SQL; compressed rows have to be read to learn their version.
    # shortcut: holds every version-4 payload in memory; fine while few Dags use success_threshold.
    rows = []
    threshold_dag_ids = set()
    for row in conn.execute(
        sa.select(
            serialized_dag.c.id,
            serialized_dag.c.dag_id,
            serialized_dag.c.dag_version_id,
            serialized_dag.c.data,
            serialized_dag.c.data_compressed,
        )
        .where(
            sa.or_(
                serialized_dag.c.data_compressed.isnot(None),
                serialized_dag.c.data["__version"].as_integer() >= 4,
            )
        )
        .execution_options(yield_per=200)
    ):
        payload = json.loads(zlib.decompress(row.data_compressed)) if row.data_compressed else row.data
        version = payload.get("__version") if payload else None
        if version == 4:
            rows.append((row, payload))
        elif version == 5:
            threshold_dag_ids.add(row.dag_id)
    if threshold_dag_ids:
        # Every stored Dag version keeps its row, so re-parsing without the Threshold does not clear them.
        raise RuntimeError(
            "Cannot downgrade: stored versions of these Dags carry a Threshold trigger rule, which older "
            "Airflow versions cannot read and no preset can stand in for. Delete the Dags (this removes "
            f"their stored versions), then retry: {sorted(threshold_dag_ids)}"
        )
    if not rows:
        return

    version_ids = [row.dag_version_id for row, _ in rows]
    blocking_runs = conn.execute(
        sa.select(dag_run.c.dag_id, dag_run.c.run_id)
        .where(
            dag_run.c.state.in_(("queued", "running")),
            sa.or_(
                dag_run.c.created_dag_version_id.in_(version_ids),
                sa.exists().where(
                    task_instance.c.dag_id == dag_run.c.dag_id,
                    task_instance.c.run_id == dag_run.c.run_id,
                    task_instance.c.dag_version_id.in_(version_ids),
                ),
            ),
        )
        .order_by(dag_run.c.dag_id, dag_run.c.run_id)
    ).all()
    if blocking_runs:
        runs = ", ".join(f"dag_id={dag_id!r} run_id={run_id!r}" for dag_id, run_id in blocking_runs)
        raise RuntimeError(
            "Cannot downgrade: unfinished Dag runs use serialized Dags that set success_threshold, "
            f"which older Airflow versions cannot read. Let them finish or mark them failed first: {runs}"
        )

    for row, payload in rows:
        for task in payload["dag"]["tasks"]:
            task["__var"].pop("success_threshold", None)
        payload["__version"] = 3
        if row.data_compressed:
            values = {"data_compressed": zlib.compress(json.dumps(payload, sort_keys=True).encode("utf-8"))}
        else:
            values = {"data": payload}
        conn.execute(
            serialized_dag.update()
            .where(serialized_dag.c.id == row.id)
            .values(dag_hash=_hash_dag(payload), **values)
        )


def _hash_dag(dag_data: dict[str, Any]) -> str:
    """Hash a serialized Dag as ``SerializedDagModel.hash`` does; migrations must not import ORM models."""
    data = _sort_serialized_dag_dict(dag_data)
    data["dag"].pop("fileloc", None)
    data["dag"].pop("bundle_name", None)
    return md5(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()


def _sort_serialized_dag_dict(value: Any) -> Any:
    """Recursively sort a serialized Dag as ``SerializedDagModel._sort_serialized_dag_dict`` does."""
    if isinstance(value, dict):
        return {k: _sort_serialized_dag_dict(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        if all(isinstance(i, dict) for i in value):
            if all(
                isinstance(i.get("__var", {}), Iterable) and "task_id" in i.get("__var", {}) for i in value
            ):
                return sorted(
                    [_sort_serialized_dag_dict(i) for i in value],
                    key=lambda x: x["__var"]["task_id"],
                )
        elif all(isinstance(item, str) for item in value):
            return sorted(value)
        return [_sort_serialized_dag_dict(i) for i in value]
    return value
