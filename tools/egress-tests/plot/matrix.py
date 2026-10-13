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

"""Turns the run reports of a B x C matrix into one Markdown table, HTTP and
HTTPS rows together per B x C:

    matrix.py [--out MATRIX.md] [--json matrix.json] RUN.json...

Each row comes from one report alone: B, C and the scheme from its config;
the gateway replica count from the resources' gateway component; opening
cores per 1000 req/s from the gateway pod cgroups and the loop timeline
between the loop's start and the settled window's start; settled cores per
1000 req/s as the driver computed it; Envoy heap as the replicas' allocated
bytes summed per second, at the run's maximum; and the settled p99 and p99.9
from the timeline's cumulative latency histograms differenced over the settled
window, with the driver's bucket rule.
"""
import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path

BUCKETS_PER_DECADE = 20
MAX_BUCKET_MICROS = 60_000_000
# The inclusive upper bound of each finite bucket, as egressapi/histogram.go.
BOUNDS = []
while True:
    _b = 10 ** (len(BOUNDS) / BUCKETS_PER_DECADE)
    BOUNDS.append(_b)
    if _b >= MAX_BUCKET_MICROS:
        break

COLUMNS = ("B x C", "protocol", "tunnels", "replicas", "opening cores / krps", "settled cores / krps",
           "Envoy heap", "settled p99", "settled p99.9")
SCHEME_ORDER = {"http": 0, "https": 1}


def parse_time(text: str) -> float:
    """A report timestamp (RFC 3339, up to nanoseconds, Z) as epoch seconds."""
    text = text.replace("Z", "+00:00")
    if "." in text:
        head, rest = text.split(".", 1)
        frac, tz = rest[:-6], rest[-6:]
        text = f"{head}.{frac[:6].ljust(6, '0')}{tz}"
    return datetime.fromisoformat(text).timestamp()


def quantile(counts: list, q: float, max_micros: int) -> float:
    """The driver's rule: the upper bound of the bucket holding the q-th
    sample, capped at the longest sample; the overflow bucket is the max."""
    total = sum(counts)
    if total == 0:
        return math.nan
    want = math.ceil(q * total)
    seen = 0
    for i, c in enumerate(counts):
        seen += c
        if seen >= want:
            if i >= len(BOUNDS):
                return max_micros
            return min(math.ceil(BOUNDS[i]), max_micros)
    return max_micros


def gateway_cores(samples: list, t0: float, t1: float) -> float:
    """The gateway pods' cgroup CPU rate over [t0, t1], summed over pods."""
    per: dict = {}
    for s in samples:
        if s.get("component") != "gateway" or s.get("container") != "POD":
            continue
        t = parse_time(s["t"])
        if t0 <= t <= t1:
            per.setdefault(s["pod"], []).append((t, s["cpuSeconds"]))
    cores = 0.0
    for pts in per.values():
        pts.sort()
        if len(pts) >= 2 and pts[-1][0] > pts[0][0]:
            cores += (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0])
    return cores


def window(timeline: list, t0: float, t1: float) -> tuple:
    """Requests per second and the latency histogram of the timeline between
    the entries nearest t0 and t1, and the longest sample seen."""
    pts = [(parse_time(e["t"]), e) for e in timeline]
    a = min(pts, key=lambda p: abs(p[0] - t0))
    b = min(pts, key=lambda p: abs(p[0] - t1))
    if a is b or b[0] <= a[0]:
        return math.nan, None, 0
    rate = (b[1]["requests"] - a[1]["requests"]) / (b[0] - a[0])
    counts = [y - x for x, y in zip(a[1]["latency"]["counts"], b[1]["latency"]["counts"])]
    return rate, counts, max(a[1]["latency"]["maxMicros"], b[1]["latency"]["maxMicros"])


def heap_total_max(memory: list) -> tuple:
    """The replicas' allocated bytes summed per sampling second, at the run's
    maximum, and the replica count that sum covers."""
    by_second: dict = {}
    for m in memory:
        by_second.setdefault(int(parse_time(m["t"])), {})[m["pod"]] = m["allocatedBytes"]
    pods = max((len(v) for v in by_second.values()), default=0)
    totals = [sum(v.values()) for v in by_second.values() if len(v) == pods]
    return (max(totals) if totals else math.nan), pods


def extract(path: Path) -> dict:
    """One table row's values from a run report."""
    d = json.loads(Path(path).read_text())
    cfg, res = d["config"], d["resources"]
    phases = {p["name"]: p for p in d["phases"]}
    loop0, loop1 = parse_time(phases["steady"]["start"]), parse_time(phases["steady"]["end"])
    settled0 = parse_time(res["settledWindow"]["start"])
    o_rate, _, _ = window(d["loopTimeline"], loop0, settled0)
    opening = gateway_cores(res["samples"], loop0, settled0) / (o_rate / 1000) if o_rate else math.nan
    _, counts, max_us = window(d["loopTimeline"], settled0, loop1)
    heap, _ = heap_total_max(res.get("envoyMemory", []))
    return {
        "run": Path(path).stem, "B": cfg["parallel"], "C": cfg["endpoints"], "scheme": cfg["scheme"],
        "tunnels": cfg["parallel"] * cfg["endpoints"], "replicas": res["components"]["gateway"]["pods"],
        "openingCoresPerKrps": opening, "settledCoresPerKrps": res["components"]["gateway"]["settled"]["coresPerKrps"],
        "envoyHeapBytes": heap,
        "settledP99Micros": quantile(counts, 0.99, max_us) if counts else math.nan,
        "settledP999Micros": quantile(counts, 0.999, max_us) if counts else math.nan,
    }


def fmt_duration(micros: float) -> str:
    if math.isnan(micros):
        return "n/a"
    return f"{micros / 1e6:.3g} s" if micros >= 1_000_000 else f"{micros / 1000:.3g} ms"


def fmt_gib(b: float) -> str:
    return "n/a" if math.isnan(b) else f"{b / 2 ** 30:.1f} GiB"


def sort_key(r: dict) -> tuple:
    return (r["B"] * r["C"], r["B"], r["C"], SCHEME_ORDER.get(r["scheme"], 9), r["scheme"])


def render(rows: list) -> str:
    """The Markdown table, rows ordered by tunnels, then B, with HTTP before
    HTTPS within one B x C."""
    out = ["| " + " | ".join(COLUMNS) + " |", "|" + "---|" * len(COLUMNS)]
    for r in sorted(rows, key=sort_key):
        out.append("| " + " | ".join([
            f"{r['B']} x {r['C']}", r["scheme"].upper(), f"{r['tunnels']:,}", str(r["replicas"]),
            f"{r['openingCoresPerKrps']:.2f}", f"{r['settledCoresPerKrps']:.2f}", fmt_gib(r["envoyHeapBytes"]),
            fmt_duration(r["settledP99Micros"]), fmt_duration(r["settledP999Micros"])]) + " |")
    return "\n".join(out) + "\n"


NOTE = ("Opening is from the loop's start to the settled window's start, the pass that opens every tunnel; "
        "settled is the driver's new-connection plateau window to the end of the run. Gateway cores are the "
        "replicas' pod cgroups summed. Envoy heap is `server.memory_allocated` summed over the replicas per "
        "sampling second, at the run's maximum. The percentiles are over the settled window.\n")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("reports", nargs="+", type=Path)
    p.add_argument("--out", type=Path, help="write the Markdown here instead of stdout")
    p.add_argument("--json", type=Path, help="also write the rows as JSON")
    p.add_argument("--title", default="Egress gateway matrix")
    args = p.parse_args(argv)
    rows = [extract(r) for r in args.reports]
    text = f"# {args.title}\n\n{NOTE}\n{render(rows)}"
    if args.json:
        args.json.write_text(json.dumps(sorted(rows, key=sort_key), indent=1) + "\n")
    if args.out:
        args.out.write_text(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
