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

"""Tests of plot.py, verify.py and runs.py against a small fixture report.

Run from this directory: python3 -m unittest test_plot
"""

from __future__ import annotations

import copy
import json
import math
import tempfile
import unittest
from pathlib import Path

import plot
import runs as rr
import verify

FIXTURE = Path(__file__).parent / "testdata" / "run-b10.json"


def write_runs(tmp: Path, reports: dict[str, dict]) -> list[rr.Run]:
    paths = []
    for name, report in reports.items():
        p = tmp / f"{name}.json"
        p.write_text(json.dumps(report))
        paths.append(p)
    return [rr.load_run(p) for p in paths]


def scaled(report: dict, parallel: int, cores: float, rps: float) -> dict:
    """A copy of report at another B, gateway steady cores and request rate."""
    r = copy.deepcopy(report)
    r["config"]["parallel"] = parallel
    r["resources"]["components"]["gateway"]["steady"]["cpuCores"]["mean"] = cores
    r["loop"]["requests"] = int(rps * 60)
    begin = r["resources"]["envoy"][0]["counters"]["cluster.mitm_internal.upstream_cx_total"]
    r["resources"]["envoy"][1]["counters"]["cluster.mitm_internal.upstream_cx_total"] = begin + parallel * 10
    return r


class RunsTest(unittest.TestCase):
    def test_parse_time(self):
        cases = {
            "2026-10-06T06:14:07Z": 1791267247.0,
            "2026-10-06T06:14:07.5Z": 1791267247.5,
            "2026-10-06T06:14:07.234678123-07:00": 1791292447.234678123,
            "2026-10-06T06:14:07-07:00": 1791292447.0,
        }
        for s, want in cases.items():
            with self.subTest(s=s):
                self.assertAlmostEqual(rr.parse_time(s), want, places=6)

    def test_step_sum(self):
        series = {"a": [(0, 0), (10, 1.0), (20, 3.0)], "b": [(5, 0), (15, 2.0)]}
        got = rr.step_sum(series, [-1, 0, 7, 12, 17, 25])
        self.assertEqual(got, [None, 1.0, 3.0, 5.0, 3.0, None])

    def test_quantile_ms(self):
        hist = {"counts": [0] * 70 + [99, 1], "count": 100, "maxMicros": 4000}
        # Like the Go histogram: the bucket's upper bound, rounded up to a µs.
        self.assertEqual(rr.quantile_ms(hist, 0.5), math.ceil(10 ** (70 / 20)) / 1000)
        self.assertEqual(rr.quantile_ms(hist, 1.0), math.ceil(10 ** (71 / 20)) / 1000)
        self.assertEqual(rr.quantile_ms({"count": 0}, 0.99), 0.0)

    def test_load_run_needs_steady(self):
        report = json.loads(FIXTURE.read_text())
        report["phases"] = [p for p in report["phases"] if p["name"] != "steady"]
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            with self.assertRaises(ValueError):
                rr.load_run(p)


class PlotTest(unittest.TestCase):
    def test_single_run(self):
        run = rr.load_run(FIXTURE)
        lines = plot.component_lines(run)
        cpu = {comp: y for comp, _, y in lines["cpu"]}
        self.assertEqual(list(cpu), ["gateway", "workers"])
        x = lines["cpu"][0][1]
        self.assertEqual(x[0], -12.0)
        steady = [v for xx, v in zip(x, cpu["gateway"]) if 1 <= xx <= 59]
        self.assertTrue(all(abs(v - 0.9) < 1e-9 for v in steady), steady)
        self.assertEqual(plot.overflow_points(run), ([30.0], [2]))

        fig = plot.build_figure([run])
        names = {t.name for t in fig.data if t.name}
        self.assertTrue({"gateway", "workers", "actors"} <= names, names)
        colors = {t.name: t.line.color for t in fig.data if t.mode == "lines"}
        self.assertEqual(colors["gateway"], rr.COMPONENT_COLORS["gateway"])
        notes = [a.text for a in fig.layout.annotations]
        self.assertIn("steady", notes)
        self.assertIn(plot.EMPTY_NOTES["throttled"], notes)
        self.assertTrue(any(s.type == "rect" for s in fig.layout.shapes), "steady window not shaded")

    def test_overlay_writes_one_page(self):
        base = json.loads(FIXTURE.read_text())
        with tempfile.TemporaryDirectory() as d:
            loaded = write_runs(Path(d), {"a": base, "b": copy.deepcopy(base)})
            fig = plot.build_figure(loaded)
            dashes = {t.line.dash for t in fig.data if t.mode == "lines" and t.name and t.name.startswith("gateway")}
            self.assertEqual(dashes, {"solid", "dash"})
            self.assertTrue(any("(a)" in (t.name or "") for t in fig.data), "same-shape runs not told apart")
            out = Path(d) / "out.html"
            self.assertEqual(plot.main([str(FIXTURE), "-o", str(out)]), 0)
            html = out.read_text()
            self.assertIn("seconds since steady start", html)
            self.assertNotIn('src="https://cdn', html, "page is not self-contained")


class VerifyTest(unittest.TestCase):
    def setUp(self):
        self.base = json.loads(FIXTURE.read_text())
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def results(self, reports: dict[str, dict], check: str) -> dict[str, verify.Result]:
        loaded = write_runs(Path(self.tmp.name), reports)
        return {r.scope: r for r in verify.run_checks(loaded) if r.check == check}

    def test_load(self):
        cases = [
            ("consistent", 0.05 + 8.5, True),
            ("B=100 costs twice as much", 0.05 + 17.0, False),
        ]
        for name, cores100, want in cases:
            with self.subTest(name):
                got = self.results({"b10": scaled(self.base, 10, 0.9, 1000),
                                    "b100": scaled(self.base, 100, cores100, 10000),
                                    "b1": scaled(self.base, 1, 0.1, 100)}, "load")
                judged = [r for r in got.values() if not r.info]
                self.assertEqual(len(judged), 2)
                self.assertEqual(all(r.passed for r in judged), want, got)
                self.assertTrue(any(r.info for r in got.values()), "B=1 should be reported, not judged")

    def test_connections(self):
        newconn = copy.deepcopy(self.base)
        newconn["config"]["connMode"] = "new-conn"
        for name, report, want in [("keepalive B x C", self.base, True), ("new-conn needs one per request", newconn, False)]:
            with self.subTest(name):
                got = self.results({"r": report}, "connections")
                self.assertEqual([r.passed for r in got.values()], [want])

    def test_on_off(self):
        off = copy.deepcopy(self.base)
        del off["resources"]
        slow = scaled(self.base, 10, 0.9, 500)
        for name, on, want in [("same rate", self.base, True), ("half the rate", slow, False)]:
            with self.subTest(name):
                reports = {f"off{i}": off for i in range(3)} | {f"on{i}": on for i in range(3)}
                got = self.results(reports, "on/off")
                rate = [r for s, r in got.items() if s.endswith("req/s")]
                self.assertEqual(len(rate), 1)
                self.assertEqual(rate[0].passed, want, rate[0].line())

    def test_overhead_failure_fails_the_run(self):
        bad = copy.deepcopy(self.base)
        bad["resources"]["verify"][0]["pass"] = False
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "bad.json"
            p.write_text(json.dumps(bad))
            self.assertEqual(verify.main([str(p)]), 1)
            self.assertEqual(verify.main([str(FIXTURE)]), 0)


if __name__ == "__main__":
    unittest.main()
