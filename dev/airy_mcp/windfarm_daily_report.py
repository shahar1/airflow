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
Daily production report for the Clyde Moor wind farm.

Every morning the SCADA system exports yesterday's turbine readings to
``windfarm_feed.json`` next to this file. This Dag reads the export, works out
how hard each turbine ran against its rated capacity, and publishes the daily
report that goes to the grid operator.

Copy this file and ``windfarm_feed.json`` into the Dag bundle to run the Airy
demo (``dev/airy_mcp/README.md``).
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pendulum

from airflow.sdk import dag, task

FEED_FILE = Path(__file__).with_name("windfarm_feed.json")

# Rated capacity of each turbine in megawatts (static master data).
RATED_MW = {
    "WTG-01": 3.6,
    "WTG-02": 3.6,
    "WTG-03": 3.6,
    "WTG-04": 3.6,
    "WTG-05": 4.2,
    "WTG-06": 4.2,
    "WTG-07": 4.2,
    "WTG-08": 4.2,
}


@dag(
    dag_id="windfarm_daily_report",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "energy"],
    doc_md=__doc__,
)
def windfarm_daily_report():
    @task
    def fetch_turbine_readings() -> dict[str, Any]:
        feed = json.loads(FEED_FILE.read_text())
        readings = feed["readings"]
        print(f"Loaded {len(readings)} turbine readings from {feed['site']} for {feed['export_date']}")
        return feed

    @task
    def compute_capacity_factors(feed: dict[str, Any]) -> list[dict[str, Any]]:
        results = []
        for reading in feed["readings"]:
            turbine = reading["turbine"]
            possible_mwh = RATED_MW[turbine] * 24
            factor = reading["energy_mwh"] / possible_mwh
            results.append(
                {
                    "turbine": turbine,
                    "energy_mwh": reading["energy_mwh"],
                    "avg_wind_ms": reading["avg_wind_ms"],
                    "capacity_factor": round(factor, 3),
                }
            )
            print(f"{turbine}: {reading['energy_mwh']:.1f} MWh, capacity factor {factor:.1%}")
        return results

    @task
    def publish_grid_report(feed: dict[str, Any], turbines: list[dict[str, Any]]) -> dict[str, Any]:
        # The SCADA export stamps the day it covers in this format.
        export_day = datetime.strptime(feed["export_date"], "%Y-%m-%d").date()
        total_mwh = sum(t["energy_mwh"] for t in turbines)
        possible_mwh = sum(RATED_MW[t["turbine"]] * 24 for t in turbines)
        report = {
            "report_id": f"GRID-{export_day:%Y%m%d}",
            "site": feed["site"],
            "period": export_day.isoformat(),
            "turbines_reported": len(turbines),
            "total_mwh": round(total_mwh, 1),
            "fleet_capacity_factor": round(total_mwh / possible_mwh, 3),
            "lowest_turbine": min(turbines, key=lambda t: t["capacity_factor"])["turbine"],
        }
        print(f"Published {report['report_id']} to the grid operator: {json.dumps(report)}")
        return report

    feed = fetch_turbine_readings()
    publish_grid_report(feed, compute_capacity_factors(feed))


windfarm_daily_report()
