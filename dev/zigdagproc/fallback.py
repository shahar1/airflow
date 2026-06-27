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
Python fallback for the native-Zig Dag processor: fully process the files Zig can't
(operators/constructs outside its subset) with the real Airflow machinery, into the
same `dags-folder` bundle. `import airflow` is paid once for the whole batch.

Invoked by the Zig processor as:  python3 fallback.py <file.py> [<file.py> ...]
"""

from __future__ import annotations

import sys
from pathlib import Path


def main(paths: list[str]) -> int:
    from airflow.dag_processing.dagbag import BundleDagBag, sync_bag_to_db

    rc = 0
    for arg in paths:
        p = Path(arg)
        try:
            bag = BundleDagBag(
                dag_folder=str(p),
                bundle_path=p.parent,  # the dags-folder bundle root
                bundle_name="dags-folder",
                load_op_links=False,
            )
            sync_bag_to_db(bag, bundle_name="dags-folder", bundle_version=None)
            if bag.import_errors:
                print(f"  fallback: {p.name} import errors: {list(bag.import_errors.values())[:1]}")
                rc = 1
            else:
                print(f"  fallback: wrote {p.name} ({len(bag.dags)} dag(s))")
        except Exception as e:  # noqa: BLE001 - report and continue with the rest of the batch
            print(f"  fallback: FAILED {p.name}: {type(e).__name__}: {e}")
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
