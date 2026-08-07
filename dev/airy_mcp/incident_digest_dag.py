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
Incident digest consumer Dag for the Airy demo.

Runs whenever the ``incident_triage`` Dag publishes a new incident report to
the ``incident_report`` Asset. Reads the markdown report from the triggering
Asset event's extra payload and delivers it as the daily digest (here: logged
in full — no external delivery). Copy to the Dag bundle alongside
``incident_triage_dag.py``.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import Asset, dag, task

REPORT_ASSET = Asset("incident_report")


@dag(
    dag_id="incident_digest",
    schedule=[REPORT_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "airy"],
    doc_md=__doc__,
)
def incident_digest():
    @task
    def deliver_digest(**context) -> dict[str, int | str]:
        """Log the triage reports carried by the triggering Asset events."""
        events = list(context["triggering_asset_events"].get(REPORT_ASSET, []))
        print(f"Received {len(events)} '{REPORT_ASSET.name}' event(s)")
        delivered = 0
        highest = "none"
        for event in events:
            extra = getattr(event, "extra", None) or {}
            report = extra.get("report")
            if not report:
                print(
                    "Event carries no report payload; open the producing "
                    "incident_triage run to read its publish_report XCom."
                )
                continue
            delivered += 1
            highest = extra.get("highest_severity", "unknown")
            print(
                f"Digest for report generated at {extra.get('generated_at', 'unknown')} "
                f"({extra.get('record_count', '?')} incidents, highest severity: {highest}):"
            )
            print(report)
        print(f"Delivered {delivered} digest(s); no external delivery attempted.")
        return {"events": len(events), "delivered": delivered, "highest_severity": highest}

    deliver_digest()


digest_dag = incident_digest()
