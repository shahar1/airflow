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
"""Aggregate the layer-1 series: medians over all runs per arm, base-vs-base spread, gates."""

from __future__ import annotations

import json
import os
import statistics
import sys
from pathlib import Path

OUT = Path(os.environ.get("BENCH_OUT", "files/aip106-bench"))
PRESETS = [
    "all_success",
    "all_failed",
    "all_done",
    "all_done_min_one_success",
    "one_success",
    "one_failed",
    "one_done",
    "none_failed",
    "none_failed_min_one_success",
    "none_skipped",
    "all_skipped",
]


def load(arm: str) -> list[dict]:
    runs = []
    for p in sorted(OUT.glob(f"layer1-{arm}-*.json")):
        runs.append(json.loads(p.read_text())["results"])
    return runs


STAT = os.environ.get(
    "BENCH_STAT", "min_us"
)  # per-run statistic: min of the repeats (timeit convention) or median_us


def per_run_values(runs: list[dict], size: str, rule: str, key: str | None = None) -> list[float]:
    key = key or STAT
    vals = []
    for r in runs:
        entry = r.get(size, {}).get(rule)
        if entry is not None:
            vals.append(entry[key])
    return vals


def main() -> int:
    base, head = load("base"), load("head")
    if not base or not head:
        print(f"no runs under {OUT} for base={len(base)} head={len(head)}; nothing to gate")
        return 2
    sizes = sorted({s for r in base for s in r}, key=int)
    print(f"runs: base={len(base)} head={len(head)}; sizes={sizes}; per-run statistic={STAT}")
    gate_ok = True
    lines = [
        "| F | rule | base median us | base spread (min-max) | HEAD median us | delta | within base spread | base min us | HEAD min us |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for size in sizes:
        for rule in PRESETS + ["control"]:
            b = per_run_values(base, size, rule)
            h = per_run_values(head, size, rule)
            if not b or not h:
                continue
            bm, hm = statistics.median(b), statistics.median(h)
            lo, hi = min(b), max(b)
            spread = hi - lo
            strict = lo <= hm <= hi
            ok = strict or abs(hm - bm) <= spread
            if rule == "control":
                ok = abs(hm - bm) / bm <= 0.10
                tag = "control <=10%"
            else:
                tag = ("yes" if strict else "yes (by |delta| <= spread)") if ok else "NO"
                gate_ok = gate_ok and ok
            lines.append(
                f"| {size} | {rule} | {bm:.1f} | {lo:.1f}-{hi:.1f} | {hm:.1f} | {hm - bm:+.1f} ({(hm - bm) / bm * 100:+.1f}%) | {tag} | {min(per_run_values(base, size, rule, 'min_us')):.1f} | {min(per_run_values(head, size, rule, 'min_us')):.1f} |"
            )
            if rule == "control" and not ok:
                print(f"CONTROL MOVED >10% at F={size}: base {bm:.1f} head {hm:.1f}")
    print("\n".join(lines))
    print()
    noise_ok = True
    for size in ("10", "100"):
        for rule in PRESETS + ["control"]:
            b = per_run_values(base, size, rule)
            if not b:
                continue
            bm = statistics.median(b)
            if (max(b) - min(b)) / bm > 0.10:
                noise_ok = False
                print(f"NOISE F={size} {rule}: (max-min)/median = {(max(b) - min(b)) / bm * 100:.1f}% > 10%")
    tlines = [
        "| F | preset | HEAD preset us | HEAD Threshold us | delta us | gate (F=10 only, <= +3 us) |",
        "|---|---|---|---|---|---|",
    ]
    t_ok = True
    for size in sizes:
        for rule in PRESETS:
            h = per_run_values(head, size, rule)
            t = per_run_values(head, size, f"threshold:{rule}")
            if not h or not t:
                continue
            hm, tm = statistics.median(h), statistics.median(t)
            d = tm - hm
            if size == "10":
                ok = d <= 3.0
                t_ok = t_ok and ok
                tag = "yes" if ok else "NO"
            else:
                tag = "reported"
            tlines.append(
                f"| {size} | {rule} | {hm:.1f} | {tm:.1f} | {d:+.1f} ({d / hm * 100:+.1f}%) | {tag} |"
            )
    print("\n".join(tlines))
    print()
    print(
        "Gate-1 predicate: HEAD median within [base min, base max] of the base runs, or |HEAD median - base median| <= (base max - base min); rows passing only by the second clause are marked."
    )
    print(f"GATE enum-within-base-spread: {'PASS' if gate_ok else 'FAIL'}")
    print(f"GATE threshold-vs-preset F=10 <= +3us: {'PASS' if t_ok else 'FAIL'}")
    print(f"GATE bench-noise F=10/100 spread<=10%: {'PASS' if noise_ok else 'FAIL'}")
    return 0 if gate_ok and t_ok and noise_ok else 1


if __name__ == "__main__":
    sys.exit(main())
