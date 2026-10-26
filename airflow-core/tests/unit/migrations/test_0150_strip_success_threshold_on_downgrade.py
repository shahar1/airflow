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
Tests for the downgrade that strips success_threshold, written against the schema as it stood at that revision.

The tables are declared here instead of taken from ``airflow.models``, so later model changes cannot change
what these tests exercise.
"""

from __future__ import annotations

import json
import zlib
from importlib import import_module
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.db import _get_alembic_config, downgrade, upgradedb
from airflow.utils.sqlalchemy import UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0150_3_5_0_strip_success_threshold_on_downgrade")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
DAG_ID = "migration_0150"

metadata = sa.MetaData()
dag_bundle = sa.Table("dag_bundle", metadata, sa.Column("name", sa.String(250), primary_key=True))
dag = sa.Table(
    "dag",
    metadata,
    sa.Column("dag_id", sa.String(250), primary_key=True),
    sa.Column("is_paused", sa.Boolean),
    sa.Column("is_stale", sa.Boolean),
    sa.Column("bundle_name", sa.String(250)),
    sa.Column("timetable_type", sa.String(255)),
    sa.Column("partition_mapper_info", sa.JSON),
    sa.Column("max_active_tasks", sa.Integer),
    sa.Column("max_consecutive_failed_dag_runs", sa.Integer),
    sa.Column("has_task_concurrency_limits", sa.Boolean),
)
dag_version = sa.Table(
    "dag_version",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("version_number", sa.Integer),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("created_at", UtcDateTime),
    sa.Column("last_updated", UtcDateTime),
)
serialized_dag = sa.Table(
    "serialized_dag",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("data", sa.JSON),
    sa.Column("data_compressed", sa.LargeBinary),
    sa.Column("created_at", UtcDateTime),
    sa.Column("last_updated", UtcDateTime),
    sa.Column("dag_hash", sa.String(32)),
    sa.Column("dag_version_id", sa.Uuid),
)
dag_run = sa.Table(
    "dag_run",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("run_type", sa.String(50)),
    sa.Column("run_after", UtcDateTime),
    sa.Column("state", sa.String(50)),
    sa.Column("created_dag_version_id", sa.Uuid),
)
task_instance = sa.Table(
    "task_instance",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("task_id", sa.String(250)),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("try_number", sa.Integer),
    sa.Column("pool", sa.String(256)),
    sa.Column("pool_slots", sa.Integer),
    sa.Column("dag_version_id", sa.Uuid),
)


def _delete_rows(connection):
    connection.execute(task_instance.delete().where(task_instance.c.dag_id == DAG_ID))
    connection.execute(dag_run.delete().where(dag_run.c.dag_id == DAG_ID))
    connection.execute(serialized_dag.delete().where(serialized_dag.c.dag_id == DAG_ID))
    connection.execute(dag_version.delete().where(dag_version.c.dag_id == DAG_ID))
    connection.execute(dag.delete().where(dag.c.dag_id == DAG_ID))
    connection.execute(dag_bundle.delete().where(dag_bundle.c.name == DAG_ID))


@pytest.fixture
def database_at_head():
    """Hold the database at the current head with a Dag of this test's own, then clean up and restore it."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        upgradedb()
        with settings.engine.begin() as connection:
            _delete_rows(connection)
            connection.execute(dag_bundle.insert().values(name=DAG_ID))
            connection.execute(
                dag.insert().values(
                    dag_id=DAG_ID,
                    is_paused=False,
                    is_stale=False,
                    bundle_name=DAG_ID,
                    timetable_type="",
                    partition_mapper_info={},
                    max_active_tasks=16,
                    max_consecutive_failed_dag_runs=0,
                    has_task_concurrency_limits=False,
                )
            )
        yield settings.engine
    finally:
        try:
            with settings.engine.begin() as connection:
                _delete_rows(connection)
            upgradedb()
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


def _payload(version):
    threshold = {"success_threshold": 2} if version == 4 else {}
    return {
        "__version": version,
        "dag": {
            "dag_id": DAG_ID,
            "fileloc": "/dags/migration_0150.py",
            "tasks": [
                {"__type": "operator", "__var": {"task_id": "b", **threshold}},
                {"__type": "operator", "__var": {"task_id": "a", **threshold}},
            ],
        },
    }


def _add_version(connection, number, version, *, compressed=False, run_state=None, run_link=None):
    """Add a Dag version and its serialized Dag, and a Dag run linked to it by *run_link* if given."""
    now = timezone.utcnow()
    version_id = uuid4()
    payload = _payload(version)
    connection.execute(
        dag_version.insert().values(
            id=version_id, version_number=number, dag_id=DAG_ID, created_at=now, last_updated=now
        )
    )
    connection.execute(
        serialized_dag.insert().values(
            id=uuid4(),
            dag_id=DAG_ID,
            data=None if compressed else payload,
            data_compressed=zlib.compress(json.dumps(payload, sort_keys=True).encode())
            if compressed
            else None,
            created_at=now,
            last_updated=now,
            dag_hash=SerializedDagModel.hash(payload),
            dag_version_id=version_id,
        )
    )
    if run_link:
        run_id = f"run_{number}"
        connection.execute(
            dag_run.insert().values(
                dag_id=DAG_ID,
                run_id=run_id,
                run_type="manual",
                run_after=now,
                state=run_state,
                created_dag_version_id=version_id if run_link == "created" else None,
            )
        )
        if run_link == "task_instance":
            connection.execute(
                task_instance.insert().values(
                    id=uuid4(),
                    task_id="a",
                    dag_id=DAG_ID,
                    run_id=run_id,
                    try_number=0,
                    pool="default_pool",
                    pool_slots=1,
                    dag_version_id=version_id,
                )
            )
    return version_id


def _read_rows(engine):
    with engine.connect() as connection:
        return {
            row.dag_version_id: row
            for row in connection.execute(sa.select(serialized_dag).where(serialized_dag.c.dag_id == DAG_ID))
        }


def test_downgrade_strips_version_4_rows_no_unfinished_run_uses(database_at_head):
    with database_at_head.begin() as connection:
        in_data = _add_version(connection, 1, 4)
        compressed = _add_version(
            connection, 2, 4, compressed=True, run_state="success", run_link="task_instance"
        )
        version_3 = _add_version(connection, 3, 3, run_state="running", run_link="created")
    before = _read_rows(database_at_head)

    downgrade(to_revision=PREVIOUS_REVISION)

    after = _read_rows(database_at_head)
    stripped = _payload(3)
    assert after[version_3] == before[version_3]
    assert after[in_data].data_compressed is None
    assert after[in_data].data == stripped
    assert after[compressed].data is None
    assert json.loads(zlib.decompress(after[compressed].data_compressed)) == stripped
    assert after[in_data].dag_hash == after[compressed].dag_hash == SerializedDagModel.hash(stripped)


@pytest.mark.parametrize(("run_state", "run_link"), [("queued", "created"), ("running", "task_instance")])
def test_downgrade_refuses_while_an_unfinished_run_uses_a_version_4_row(
    database_at_head, run_state, run_link
):
    with database_at_head.begin() as connection:
        _add_version(connection, 1, 4)
        _add_version(connection, 2, 4, compressed=True, run_state=run_state, run_link=run_link)
    before = _read_rows(database_at_head)

    with pytest.raises(RuntimeError, match=f"dag_id='{DAG_ID}' run_id='run_2'"):
        downgrade(to_revision=PREVIOUS_REVISION)

    assert _read_rows(database_at_head) == before


def test_offline_downgrade_only_states_that_version_4_rows_need_online(capsys, monkeypatch):
    monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", "postgresql://")

    command.downgrade(_get_alembic_config(), f"{REVISION}:{PREVIOUS_REVISION}", sql=True)

    emitted = capsys.readouterr().out
    assert "Version-4 rows must be handled online" in emitted
    assert "serialized_dag SET" not in emitted
