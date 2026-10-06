# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reading egress-tests --output reports: phases, series, and histograms."""

from __future__ import annotations

import bisect
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Components in plotting order, each with a fixed color: the first seven
# slots of the reference categorical palette, so a component keeps its color
# in every panel and every run.
COMPONENT_COLORS = {
    "gateway": "#2a78d6",
    "workers": "#eb6834",
    "ateapi": "#1baf7a",
    "targets": "#eda100",
    "dns": "#e87ba4",
    "router": "#008300",
    "actors": "#4a3aa7",
}

# Containers that split a worker's ateom into parts; they are not added to
# the component's total.
SPLIT_CONTAINERS = {"actors", "atunnel"}

PHASES = ("create", "resume", "start", "steady", "stop", "suspend")


def parse_time(s: str) -> float:
    """RFC 3339 with up to nanoseconds, to epoch seconds."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    main, _, rest = s.partition(".")
    frac, tz = "", ""
    if rest:
        i = next((k for k, ch in enumerate(rest) if not ch.isdigit()), len(rest))
        frac, tz = rest[:i], rest[i:]
    else:
        for sign in ("+", "-"):
            j = main.rfind(sign)
            if j > 10:
                main, tz = main[:j], main[j:]
                break
    t = datetime.fromisoformat(main + tz).timestamp()
    return t + (float("0." + frac) if frac else 0.0)


@dataclass(eq=False)
class Run:
    path: Path
    report: dict
    label: str
    phases: dict[str, tuple[float, float]] = field(default_factory=dict)

    @property
    def steady_start(self) -> float:
        return self.phases["steady"][0]

    def series(self, metric: str, sources: tuple[str, ...] | None = None) -> list[dict]:
        rows = (self.report.get("resources") or {}).get("series") or []
        return [r for r in rows if r["metric"] == metric and (sources is None or r["source"] in sources)]


def run_label(report: dict) -> str:
    c = report["config"]
    return f"B={c['parallel']} C={c['endpoints']} {c['connMode']}"


def load_run(path: str | Path) -> Run:
    p = Path(path)
    report = json.loads(p.read_text())
    phases = {ph["name"]: (parse_time(ph["start"]), parse_time(ph["end"])) for ph in report.get("phases") or []}
    if "steady" not in phases:
        raise ValueError(f"{p}: no steady phase; the run never started its loops")
    return Run(path=p, report=report, label=run_label(report), phases=phases)


def step_sum(series: dict[str, list[tuple[float, float]]], grid: list[float]) -> list[float | None]:
    """Sums piecewise-constant series on grid.

    Each series maps to sorted (t, value) points, where value holds from the
    point before up to t, as for a rate over the interval ending at t. A grid
    point no series covers is None.
    """
    times = {k: [t for t, _ in pts] for k, pts in series.items()}
    out: list[float | None] = []
    for g in grid:
        total, covered = 0.0, False
        for k, pts in series.items():
            i = bisect.bisect_right(times[k], g)  # points at or before g
            if 1 <= i < len(pts):
                total += pts[i][1]
                covered = True
        out.append(total if covered else None)
    return out


def last_value(series: dict[str, list[tuple[float, float]]], grid: list[float]) -> list[float | None]:
    """Sums, on grid, each sorted series' latest gauge reading."""
    times = {k: [t for t, _ in pts] for k, pts in series.items()}
    out: list[float | None] = []
    for g in grid:
        total, covered = 0.0, False
        for k, pts in series.items():
            i = bisect.bisect_right(times[k], g)
            if i:
                total += pts[i - 1][1]
                covered = True
        out.append(total if covered else None)
    return out


# Histogram buckets of internal/egressapi: upper bounds 10^(k/20)
# microseconds, up to the first at or past 60 s, plus an overflow bucket.
# Mirrors bucketBounds in internal/egressapi/histogram.go; change both.
def bucket_bounds() -> list[float]:
    bounds, k = [], 0
    while True:
        b = 10 ** (k / 20)
        bounds.append(b)
        if b >= 60_000_000:
            return bounds
        k += 1


BUCKETS = bucket_bounds()


def quantile_ms(hist: dict, q: float) -> float:
    """The q-quantile of an egressapi.Histogram, in milliseconds."""
    count = hist.get("count", 0)
    if count == 0:
        return 0.0
    rank = min(max(math.ceil(q * count), 1), count)
    seen = 0
    for i, c in enumerate(hist.get("counts") or []):
        seen += c
        if seen >= rank:
            if i == len(BUCKETS):
                break
            return min(math.ceil(BUCKETS[i]), hist["maxMicros"]) / 1000
    return hist["maxMicros"] / 1000


def median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2
