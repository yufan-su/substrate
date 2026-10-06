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

"""Checks that span several egress-tests runs.

Usage: python3 verify.py run1.json run2.json ...

- load: the gateway's marginal cores per 1000 req/s agree, within 20% of
  their median, across runs of the same C, mode and pacing with B >= 10.
  B=1 runs are reported but not judged: idle CPU dominates at ~10 req/s.
- connections: the gateway opened one mitm_internal connection per actor
  connection, about B x C in keepalive mode and one per request in
  new-conn mode, within 2%.
- on/off: runs of the same shape with and without --usage reach the
  same req/s and p99: the median with resources lies within the range of
  the runs without. Three of each make the range meaningful.
- overhead: the in-run overhead checks of every run passed.

Prints one line per check and exits 1 when any fails.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass

import runs as rr

LOAD_BAND = 0.20


@dataclass
class Result:
    check: str
    scope: str
    got: str
    want: str
    passed: bool
    info: bool = False

    def line(self) -> str:
        status = "INFO" if self.info else ("ok" if self.passed else "FAIL")
        return f"{status:4} {self.check} {self.scope}: got {self.got}, want {self.want}"


def request_rate(report: dict) -> float:
    loop = report.get("loop") or {}
    elapsed = loop.get("elapsed", 0) / 1e9
    return loop.get("requests", 0) / elapsed if elapsed > 0 else 0.0


def load_window(report: dict) -> tuple[dict | None, float]:
    """The gateway's settled summary and request rate, or steady's when the
    run has no settled window."""
    res = report.get("resources") or {}
    gw = (res.get("components") or {}).get("gateway")
    if not gw:
        return None, 0.0
    settled = res.get("settledWindow")
    if gw.get("settled") and settled:
        return gw["settled"], settled.get("reqPerS", 0.0)
    return gw.get("steady"), request_rate(report)


def idle_gateway_cores(run: rr.Run) -> float | None:
    """The gateway's mean cores before the actors resumed, if sampled."""
    rows = run.series("cpu_cores", ("cadvisor", "cgreader"))
    series: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for r in rows:
        if r["component"] == "gateway" and r.get("container") != "POD":
            series[f"{r.get('pod')}/{r.get('container')}"].append((rr.parse_time(r["t"]), r["value"]))
    resume = run.phases.get("resume", run.phases["steady"])[0]
    firsts = [min(t for t, _ in pts) for pts in series.values() if pts]
    if not firsts:
        return None
    start = max(firsts)
    grid = [start + i for i in range(int(resume - start))]
    values = [v for v in rr.step_sum({k: sorted(v) for k, v in series.items()}, grid) if v is not None]
    return sum(values) / len(values) if values else None


def check_load(runs: list[rr.Run]) -> list[Result]:
    groups: dict[tuple, list[rr.Run]] = defaultdict(list)
    for r in runs:
        c = r.report["config"]
        groups[(c["endpoints"], c["connMode"], c["requestInterval"])].append(r)
    out = []
    for (endpoints, mode, interval), group in sorted(groups.items()):
        marginals = {}
        for r in group:
            window, rps = load_window(r.report)
            if not window or window.get("insufficient") or rps <= 0:
                continue
            idle = idle_gateway_cores(r) or 0.0
            marginals[r] = (window["cpuCores"]["mean"] - idle) / (rps / 1000)
        judged = {r: m for r, m in marginals.items() if r.report["config"]["parallel"] >= 10}
        mid = rr.median(list(judged.values())) if judged else None
        for r, m in marginals.items():
            scope = f"{r.label} ({r.path.name})"
            if r not in judged or len(judged) < 2:
                out.append(Result("load", scope, f"{m:.3f} cores/krps", "B >= 10 and two such runs to compare", True, info=True))
                continue
            ok = abs(m - mid) <= LOAD_BAND * abs(mid)
            out.append(Result("load", scope, f"{m:.3f} cores/krps", f"within {LOAD_BAND:.0%} of median {mid:.3f}", ok))
    return out


def check_connections(runs: list[rr.Run]) -> list[Result]:
    """Relays each run's own envoy-connections verdict.

    The driver compares the gateway's new tunnels with the actors' count and
    knows when the breaker held connections back; re-deriving that here from
    B x C would disagree with it under overflow.
    """
    out = []
    for r in runs:
        scope = f"{r.label} ({r.path.name})"
        found = [v for v in (r.report.get("resources") or {}).get("verify") or [] if v["check"] == "envoy-connections"]
        if not found:
            out.append(Result("connections", scope, "no Envoy readings", "a run with --usage", True, info=True))
        for v in found:
            out.append(Result("connections", scope, v["got"], v["want"], v["pass"], info=bool(v.get("info"))))
    return out


def check_on_off(runs: list[rr.Run]) -> list[Result]:
    groups: dict[tuple, dict[bool, list[rr.Run]]] = defaultdict(lambda: {True: [], False: []})
    for r in runs:
        c = r.report["config"]
        shape = (c["actors"], c["parallel"], c["endpoints"], c["connMode"], c["requestInterval"], c["duration"])
        groups[shape][r.report.get("resources") is not None].append(r)
    out = []
    for shape, by in sorted(groups.items()):
        on, off = by[True], by[False]
        if not on or not off:
            continue
        scope = f"{on[0].label} ({len(on)} on, {len(off)} off)"
        for name, value in (("req/s", lambda r: request_rate(r.report)),
                            ("p99 ms", lambda r: rr.quantile_ms((r.report.get("loop") or {}).get("latency") or {}, 0.99))):
            off_vals = [value(r) for r in off]
            on_mid = rr.median([value(r) for r in on])
            ok = min(off_vals) <= on_mid <= max(off_vals)
            info = len(on) < 3 or len(off) < 3
            out.append(Result("on/off", f"{scope} {name}", f"median {on_mid:.3g}",
                              f"within off range [{min(off_vals):.3g}, {max(off_vals):.3g}]", ok, info=info and not ok))
    return out


def check_overhead(runs: list[rr.Run]) -> list[Result]:
    out = []
    for r in runs:
        for v in (r.report.get("resources") or {}).get("verify") or []:
            if v["check"] == "overhead":
                out.append(Result("overhead", f"{v['scope']} ({r.path.name})", v["got"], v["want"], v["pass"]))
    return out


def run_checks(runs: list[rr.Run]) -> list[Result]:
    return check_load(runs) + check_connections(runs) + check_on_off(runs) + check_overhead(runs)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("reports", nargs="+", help="--output JSON files of egress-tests runs")
    args = p.parse_args(argv)
    results = run_checks([rr.load_run(r) for r in args.reports])
    for res in results:
        print(res.line())
    failed = sum(1 for r in results if not r.passed and not r.info)
    print(f"verify.py: {len(results)} checks, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
