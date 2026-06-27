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
Differential oracle for the native-Zig Dag processor.

For each REAL Python Dag under dev/zigdagproc/dags/, parse it two ways:
  - Zig: static parse (no execution) via the binary's `--emit`
  - Airflow: the actual DagBag, which EXECUTES the file like the real processor
and compare the serialized blobs. Same `.py`, two parsers — the strongest oracle.

Run (from repo root):
  uv run --project airflow-core python dev/zig_verify_emit.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ZIG_DIR = Path(__file__).resolve().parent / "zigdagproc"
ZIG_BIN = ZIG_DIR / "zig-out" / "bin" / "zigdagproc"
DAGS_DIR = ZIG_DIR / "dags"


def real_serialized(py_path: Path):
    """Parse the .py with Airflow's actual DagBag (executes the file) and serialize it."""
    from airflow.dag_processing.dagbag import BundleDagBag
    from airflow.serialization.serialized_objects import DagSerialization

    bag = BundleDagBag(
        dag_folder=str(py_path),
        bundle_path=py_path.parent,
        bundle_name="zig",
        load_op_links=False,
    )
    if bag.import_errors:
        raise RuntimeError(f"airflow failed to import {py_path.name}: {bag.import_errors}")
    dag = next(iter(bag.dags.values()))
    return DagSerialization.to_dict(dag)


def diff(a, b, path=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}.{k}: missing in ZIG (real={b[k]!r})")
            elif k not in b:
                out.append(f"{path}.{k}: extra in ZIG ({a[k]!r})")
            else:
                out += diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append(f"{path}: len {len(a)} != {len(b)}")
        else:
            for i, (x, y) in enumerate(zip(a, b)):
                out += diff(x, y, f"{path}[{i}]")
    elif a != b:
        out.append(f"{path}: ZIG {a!r} != REAL {b!r}")
    return out


def main() -> int:
    from airflow.configuration import conf
    from airflow.models.serialized_dag import SerializedDagModel

    if not ZIG_BIN.exists():
        print(f"build the binary first: (cd {ZIG_DIR} && zig build)")
        return 2

    # The real serialization stamps _processor_dags_folder = core.dags_folder (a global
    # config, part of the hash). Give Zig the same value so the hashes are comparable.
    dags_folder = conf.get("core", "dags_folder")

    all_ok = True
    for py_path in sorted(DAGS_DIR.glob("*.py")):
        # Zig side: static parse, --emit prints "<blob>\n" then "dag_hash=... dag_id=...".
        proc = subprocess.run(
            [
                str(ZIG_BIN),
                "--emit",
                str(Path("dags") / py_path.name),
                "--processor-dags-folder",
                dags_folder,
            ],
            cwd=ZIG_DIR,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            print(f"{py_path.name}: ZIG FAILED\n{proc.stderr}")
            all_ok = False
            continue
        blob_line, _, meta = proc.stdout.partition("\n")
        zig = json.loads(blob_line)
        zig_hash = meta.split("dag_hash=")[1].split()[0]

        # Real side: Airflow actually imports/executes the .py.
        real = real_serialized(py_path)
        real_hash = SerializedDagModel.hash(real)
        hash_match = zig_hash == real_hash

        # Normalize environment-derived fields (real fileloc/dags_folder differ from Zig's).
        for d in (zig, real):
            d["dag"]["fileloc"] = "<fileloc>"
            d["dag"]["_processor_dags_folder"] = "<dags>"
        # Project real through JSON (str-enums -> str, tuples -> arrays) to compare what is stored.
        real = json.loads(json.dumps(real, default=str))

        diffs = diff(zig["dag"], real["dag"], "dag")
        struct_match = json.dumps(zig, sort_keys=True) == json.dumps(real, sort_keys=True)
        status = "OK" if not diffs else f"{len(diffs)} DIFFS"
        print(
            f"{py_path.name:24} {status:10} struct={struct_match!s:5} hash_identical={hash_match!s:5} "
            f"zig={zig_hash[:10]} real={real_hash[:10]}"
        )
        for d in diffs[:15]:
            print(f"    {d}")
        if diffs or not hash_match:
            all_ok = False

    print("\nRESULT:", "ALL MATCH ✅" if all_ok else "MISMATCHES ❌ (see diffs above)")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
