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

from contextlib import ExitStack

import pytest

from airflow.utils.file_locks import LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN, flock

# Both flock and the Windows LockFileEx backend scope locks to the open file handle,
# so two handles in one process conflict exactly like two processes do.


@pytest.fixture
def lock_handles(tmp_path):
    lock_file = tmp_path / "test.lock"
    lock_file.touch()
    with ExitStack() as stack:
        yield lambda: stack.enter_context(open(lock_file, "a"))


def test_exclusive_lock_blocks_other_handles(lock_handles):
    first, second = lock_handles(), lock_handles()
    flock(first, LOCK_EX | LOCK_NB)
    with pytest.raises(BlockingIOError):
        flock(second, LOCK_EX | LOCK_NB)
    with pytest.raises(BlockingIOError):
        flock(second, LOCK_SH | LOCK_NB)
    flock(first, LOCK_UN)
    flock(second, LOCK_EX | LOCK_NB)


def test_shared_locks_coexist_and_block_exclusive(lock_handles):
    first, second, third = lock_handles(), lock_handles(), lock_handles()
    flock(first, LOCK_SH | LOCK_NB)
    flock(second, LOCK_SH | LOCK_NB)
    with pytest.raises(BlockingIOError):
        flock(third, LOCK_EX | LOCK_NB)
    flock(first, LOCK_UN)
    flock(second, LOCK_UN)
    flock(third, LOCK_EX | LOCK_NB)


def test_lock_released_when_handle_closes(lock_handles):
    first = lock_handles()
    flock(first, LOCK_EX | LOCK_NB)
    first.close()
    flock(lock_handles(), LOCK_EX | LOCK_NB)
