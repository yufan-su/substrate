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

"""Tests of fit.py on synthetic runs drawn from known coefficients.

Run from this directory: python3 -m unittest test_fit
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
from datetime import datetime, timezone
import unittest
from pathlib import Path

import numpy as np

import fit

FIXTURE = Path(__file__).parent / "testdata" / "run-b10.json"

GRID = [(b, c) for c in (10, 50, 128, 256) for b in (10, 25, 50, 100) if b * c <= 16384]
REPLICATES = [(10, 256), (100, 50), (50, 256)]

# Known coefficients per response, in FORMS term order.
TRUTH = {
    "gateway_cores": {"1": 0.01, "B": 0.0088, "T": 2.0e-5, "C": 1.0e-4},
    "envoy_cores": {"1": 0.006, "B": 0.0057, "T": 1.3e-5, "C": 8.0e-5},
    "extproc_cores": {"1": 0.007, "B": 0.0032},
    "gateway_ws_mib": {"1": 5.0, "T": 0.1, "C": 0.5},
    "envoy_heap_mib": {"1": 10.0, "T": 0.078},
    "worker_cores": {"1": 0.02, "B": 0.023, "T": 1.0e-5},
    "atunnel_cores": {"B": 0.0015, "T": 5.0e-6},
    "worker_ws_mib": {"B": 41.0, "T": 0.2, "C": 1.5},
    "p99_settled_ms": {"1": 4.0, "B": 0.03},
}
# Additive noise per response, about 2% of its value at B=50 C=128, today's
# run-to-run spread on gateway cores.
NOISE = 0.02


def truth(resp: str, b: int, c: int) -> float:
    row = {"B": b, "C": c, "T": b * c}
    return sum(v * fit.term_value(t, row) for t, v in TRUTH[resp].items())


def synthetic(points, seed: int, noise: float = NOISE) -> list[dict]:
    rng = np.random.default_rng(seed)
    rows = []
    for b, c in points:
        row = {"B": b, "C": c, "T": b * c}
        for resp in TRUTH:
            row[resp] = truth(resp, b, c) + rng.normal(0, noise * truth(resp, 50, 128))
        rows.append(row)
    return rows


class FitTest(unittest.TestCase):
    def setUp(self):
        self.rows = synthetic(GRID + REPLICATES, seed=1)
        self.fits = fit.fit_all(self.rows)

    def test_intervals_cover_the_truth(self):
        # Over many noise draws, each full form's 95% intervals hold the true
        # coefficient about 95% of the time.
        seeds = range(300)
        for resp, coefs in TRUTH.items():
            hits = total = 0
            for seed in seeds:
                f = fit.ols(synthetic(GRID + REPLICATES, seed=seed), resp, list(coefs))
                for i, term in enumerate(f.terms):
                    lo, hi = f.interval(i)
                    hits += lo <= coefs[term] <= hi
                    total += 1
            with self.subTest(response=resp):
                self.assertTrue(0.92 <= hits / total <= 0.98, f"{resp}: coverage {hits / total:.3f}")

    def test_estimates_near_the_truth(self):
        # One draw: every estimate, full form or after dropping, lies within
        # three half-widths of its true value.
        for resp, coefs in TRUTH.items():
            for f in (fit.ols(self.rows, resp, list(coefs)), self.fits[resp]):
                for i, term in enumerate(f.terms):
                    with self.subTest(response=resp, term=term, dropped=f.dropped):
                        lo, hi = f.interval(i)
                        self.assertLessEqual(abs(f.coef[i] - coefs[term]), 1.5 * (hi - lo))

    def test_drops_a_term_that_is_zero(self):
        rows = synthetic(GRID, seed=2)
        for r in rows:
            r["extproc_cores"] = 0.007 + 0.0032 * r["B"] * (1 + 0.02 * np.sin(r["C"]))
        f = fit.fit_response(rows, "extproc_cores", ("1", "B", "T"))
        self.assertIn("T", f.dropped)
        self.assertIn("B", f.terms)

    def test_holdouts_pass_and_fail(self):
        holdouts = [(20, 75), (40, 200), (80, 160), (75, 20)]
        good = synthetic(holdouts, seed=3, noise=0.0)
        for h in good:
            with self.subTest(holdout=(h["B"], h["C"]), case="the true values"):
                misses = [s for s in fit.score(self.fits, h) if not s[4]]
                self.assertEqual(misses, [])
        bad = synthetic(holdouts, seed=3, noise=0.0)
        for h in bad:
            h["envoy_cores"] *= 1.5
            h["gateway_ws_mib"] *= 1.3
            h["p99_settled_ms"] *= 2.0
            with self.subTest(holdout=(h["B"], h["C"]), case="perturbed"):
                missed = {s[0] for s in fit.score(self.fits, h) if not s[4]}
                self.assertEqual(missed, {"envoy_cores", "gateway_ws_mib", "p99_settled_ms"})


class PinnedWorkingSetTest(unittest.TestCase):
    def test_gateway_ws_is_informational_above_500_mib(self):
        rows = synthetic(GRID + REPLICATES, seed=1)
        for r in rows:
            r["gateway_ws_base_mib"] = 1100.0 if r["T"] >= 5000 else 50.0
        fits = fit.fit_all(rows)
        out = io.StringIO()
        fit.print_fits(fits, out=out, rows=rows)
        pinned = sum(r["T"] >= 5000 for r in rows)
        self.assertIn(f"  informational: {pinned} of {len(rows)} runs started above 500 MiB", out.getvalue())
        # The pinned rows stay out of the gateway_ws_mib fit itself, so
        # they cannot drag its T coefficient toward zero.
        self.assertEqual(fits["gateway_ws_mib"].n, len(rows) - pinned)
        self.assertEqual(fits["envoy_heap_mib"].n, len(rows))
        self.assertEqual(out.getvalue().count("informational"), 1)
        for name, base, want_judged in [("pinned", 1100.0, False), ("fresh", 50.0, True), ("no baseline", None, True)]:
            with self.subTest(name):
                h = synthetic([(40, 200)], seed=3, noise=0.0)[0] | {"gateway_ws_base_mib": base}
                h["gateway_ws_mib"] = 0.0  # what a pinned pod reads
                judged = {s[0]: s[4] for s in fit.score(fits, h)}
                self.assertEqual("gateway_ws_mib" in judged, want_judged)
                self.assertTrue(judged["envoy_heap_mib"])


class ProvisionalTest(unittest.TestCase):
    def test_provisional_rows_enter_cpu_fits_only(self):
        rows = synthetic(GRID, seed=4)
        # A provisional row that is wildly off on every response.
        bad = {"B": 100, "C": 10, "T": 1000, "provisional": True}
        for resp in TRUTH:
            bad[resp] = 100 * truth(resp, 100, 10)
        with_p = fit.fit_all(rows + [bad])
        without = fit.fit_all(rows + [bad], use_provisional=False)
        clean = fit.fit_all(rows)
        for resp in TRUTH:
            with self.subTest(response=resp):
                self.assertEqual(without[resp].n, clean[resp].n)
                self.assertEqual(with_p[resp].n, clean[resp].n + (1 if resp.endswith("_cores") else 0))
        for resp in ("p99_settled_ms", "gateway_ws_mib", "worker_ws_mib"):
            self.assertNotIn(resp, fit.PROVISIONAL_OK)
        self.assertIn("envoy_cores", fit.PROVISIONAL_OK)

    def test_csv_marks_provisional_rows(self):
        row = fit.extract(FIXTURE) | {"provisional": True}
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "runs.csv"
            fit.write_csv([row], str(out))
            header, line = out.read_text().splitlines()
            self.assertEqual(header.split(",")[1], "provisional")
            self.assertEqual(line.split(",")[1], "True")


class AcceptTest(unittest.TestCase):
    def test_rules(self):
        cases = [
            ("cpu within 15%", "envoy_cores", 1.10, 1.00, True),
            ("cpu off by 20%", "envoy_cores", 1.20, 1.00, False),
            ("cpu small, 18% off but inside the 0.02 floor", "envoy_cores", 0.118, 0.10, True),
            ("cpu small, past the floor", "envoy_cores", 0.13, 0.10, False),
            ("memory within 15%", "gateway_ws_mib", 110, 100, True),
            ("memory off by 20%", "gateway_ws_mib", 120, 100, False),
            ("memory small, 50% off but inside the 4 MiB floor", "gateway_ws_mib", 6, 4, True),
            ("memory small, past the floor", "gateway_ws_mib", 9, 4, False),
            ("p99 two buckets up", "p99_settled_ms", 5.0, 4.0, True),
            ("p99 three buckets up", "p99_settled_ms", 5.7, 4.0, False),
            ("startup is not judged", "first_round_s", 99, 1, True),
        ]
        for name, resp, pred, meas, want in cases:
            with self.subTest(name):
                self.assertEqual(fit.accept(resp, pred, meas), want)


def synthetic_run(series: list[dict]) -> fit.rr.Run:
    report = {"config": {"parallel": 1, "endpoints": 1, "connMode": "x"}, "resources": {"series": series}}
    return fit.rr.Run(path=Path("synthetic.json"), report=report, label="synthetic", phases={"steady": (0.0, 100.0)})


def at(seconds: float) -> str:
    return f"2026-10-06T10:{int(seconds) // 60:02d}:{int(seconds) % 60:02d}Z"


class ExtractTest(unittest.TestCase):
    def test_cpu_sums_containers_read_at_different_times(self):
        # envoy at 1.0 core and ext-proc at 0.5 core, each read on its own
        # second, must add to 1.5 and not average to 0.75.
        series = []
        for i in range(10):
            series.append({"metric": "cpu_cores", "source": "cadvisor", "component": "gateway",
                           "pod": "gw", "container": "envoy", "t": at(2 * i), "value": 1.0})
            series.append({"metric": "cpu_cores", "source": "cadvisor", "component": "gateway",
                           "pod": "gw", "container": "ext-proc", "t": at(2 * i + 1), "value": 0.5})
        run = synthetic_run(series)
        lo = fit.rr.parse_time(at(0))
        pts = fit.cpu_points(run, "gateway", {"envoy", "ext-proc"})
        self.assertAlmostEqual(fit.summed_mean(pts, lo, lo + 100), 1.5)

    def test_ws_delta_is_none_when_no_pod_spans_the_run(self):
        # The gateway pod was replaced: the old pod has only baseline
        # samples and the new pod only steady ones.
        series = [
            {"metric": "working_set_bytes", "source": "cadvisor", "component": "gateway",
             "pod": "old", "container": "POD", "t": at(0), "value": 100 * 2**20},
            {"metric": "working_set_bytes", "source": "cadvisor", "component": "gateway",
             "pod": "new", "container": "POD", "t": at(60), "value": 300 * 2**20},
        ]
        run = synthetic_run(series)
        lo = fit.rr.parse_time(at(0))
        self.assertIsNone(fit.ws_delta_mib(run, "gateway", lo + 50, lo + 100))
        series.append({"metric": "working_set_bytes", "source": "cadvisor", "component": "gateway",
                       "pod": "new", "container": "POD", "t": at(1), "value": 200 * 2**20})
        self.assertAlmostEqual(fit.ws_delta_mib(synthetic_run(series), "gateway", lo + 50, lo + 100), 100.0)

    def test_fixture_row(self):
        row = fit.extract(FIXTURE, base="47ed2247")
        self.assertEqual((row["B"], row["C"], row["T"], row["base"]), (10, 10, 100, "47ed2247"))
        self.assertGreater(row["R_measured"], 0)
        self.assertGreater(row["gateway_cores"], 0)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "runs.csv"
            fit.write_csv([row], str(out))
            header, line = out.read_text().splitlines()
            self.assertEqual(header.split(",")[:5], ["run", "provisional", "B", "C", "T"])
            self.assertTrue(line.startswith("run-b10.json,False,10,10,100,"))

    def test_driver_fields(self):
        # Settled p99 and the memory baseline come from the driver's JSON.
        report = json.loads(FIXTURE.read_text())
        res = report["resources"]
        steady = next(p for p in report["phases"] if p["name"] == "steady")
        res["settledWindow"] = {"start": steady["start"], "rule": "new-connection plateau",
                                "reqPerS": 95.0, "p99": 3_548_000}
        # A gateway pod cgroup at 40 MiB through steady, 10 MiB at pre-idle.
        res["series"] += [{"t": steady[k], "source": "cadvisor", "component": "gateway", "pod": "gw",
                           "container": "POD", "metric": "working_set_bytes", "value": 40 * 2**20}
                          for k in ("start", "end")]
        res["baseline"] = {"gateway": {"workingSetBytes": 8 * 2**20, "podWorkingSetBytes": 10 * 2**20,
                                       "cpuCores": {"mean": 0.01, "max": 0.01}}}
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            row = fit.extract(p)
        self.assertAlmostEqual(row["p99_settled_ms"], 3.548)
        # The pod cgroup's baseline is subtracted, not the containers' sum.
        self.assertAlmostEqual(row["gateway_ws_mib"], 30)
        del res["baseline"]["gateway"]["podWorkingSetBytes"]
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            self.assertIsNone(fit.extract(p)["gateway_ws_mib"])

    def test_envoy_heap(self):
        # Envoy's allocated heap per gateway pod: the settled max minus the
        # pre-idle max, summed over pods.
        report = json.loads(FIXTURE.read_text())
        steady = next(p for p in report["phases"] if p["name"] == "steady")
        t0 = steady["start"]
        pre_start, pre_end = "2020-01-01T00:00:00Z", "2020-01-01T00:00:30Z"
        report["phases"].insert(0, {"name": "pre-idle", "start": pre_start, "end": pre_end})

        def mem(t, pod, mib):
            return {"t": t, "pod": pod, "allocatedBytes": mib * 2**20, "heapSizeBytes": 0, "physicalBytes": 0}
        for name, samples, want in [
            ("one pod", [mem(pre_start, "gw-a", 10), mem(pre_end, "gw-a", 12), mem(t0, "gw-a", 100),
                         mem(steady["end"], "gw-a", 112)], 100),
            ("two pods", [mem(pre_end, "gw-a", 12), mem(t0, "gw-a", 112),
                          mem(pre_end, "gw-b", 20), mem(steady["end"], "gw-b", 70)], 150),
            ("one pod has steady samples but no baseline", [mem(pre_end, "gw-a", 12), mem(t0, "gw-a", 112),
                                                           mem(steady["end"], "gw-b", 70)], None),
            ("no heap samples", [], None),
        ]:
            with self.subTest(name), tempfile.TemporaryDirectory() as d:
                report["resources"]["envoyMemory"] = samples
                p = Path(d) / "r.json"
                p.write_text(json.dumps(report))
                got = fit.extract(p)["envoy_heap_mib"]
                if want is None:
                    self.assertIsNone(got)
                else:
                    self.assertAlmostEqual(got, want)

    def test_resume_p50_counts_own_snapshots_only(self):
        report = json.loads(FIXTURE.read_text())
        report["actors"] = [
            {"name": "egress-0", "resumeLatency": 400e6, "resumeSource": "own"},
            {"name": "egress-1", "resumeLatency": 420e6, "resumeSource": "own"},
            {"name": "egress-2", "resumeLatency": 1600e6, "resumeSource": "tag"},
        ]
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            self.assertAlmostEqual(fit.extract(p)["resume_p50_ms"], 410)

    def test_first_round_only_from_a_measured_start(self):
        # The plateau rule measures the first round; a floored or rule-set
        # start says nothing about it.
        report = json.loads(FIXTURE.read_text())
        steady = next(p for p in report["phases"] if p["name"] == "steady")
        start = fit.rr.parse_time(steady["start"])
        later = datetime.fromtimestamp(start + 12, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for name, window, want in [
            ("plateau", {"start": later, "rule": "new-connection plateau"}, 12.0),
            ("plateau, floored", {"start": later, "rule": "new-connection plateau", "floored": True}, None),
            ("1.5 rounds", {"start": later, "rule": "1.5 rounds"}, None),
        ]:
            with self.subTest(name), tempfile.TemporaryDirectory() as d:
                report["resources"]["settledWindow"] = window
                p = Path(d) / "r.json"
                p.write_text(json.dumps(report))
                got = fit.extract(p)["first_round_s"]
                if want is None:
                    self.assertIsNone(got)
                else:
                    self.assertAlmostEqual(got, want)

    def test_requires_a_scheme(self):
        self.assertEqual(fit.extract(FIXTURE)["scheme"], "http")
        report = json.loads(FIXTURE.read_text())
        del report["config"]["scheme"]
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            with self.assertRaises(KeyError):
                fit.extract(p)

    def test_refuses_mixed_schemes(self):
        report = json.loads(FIXTURE.read_text())
        with tempfile.TemporaryDirectory() as d:
            http, https = Path(d) / "http.json", Path(d) / "https.json"
            report["config"]["scheme"] = "http"
            http.write_text(json.dumps(report))
            report["config"]["scheme"] = "https"
            https.write_text(json.dumps(report))
            csv_path = str(Path(d) / "runs.csv")
            for name, runs, extra, want in [
                ("one scheme", [http, FIXTURE], [], 0),
                ("mixed in the fit", [http, https], [], 2),
                ("mixed through a provisional row", [http], ["--provisional", str(https)], 2),
                ("mixed through a holdout", [http], ["--holdout", str(https)], 2),
            ]:
                with self.subTest(name):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err:
                        got = fit.main([*map(str, runs), *extra, "--csv", csv_path])
                    self.assertEqual(got, want, err.getvalue())
                    if want:
                        self.assertIn("runs mix schemes", err.getvalue())

    def test_no_settled_p99_without_the_driver_field(self):
        self.assertIsNone(fit.extract(FIXTURE)["p99_settled_ms"])


if __name__ == "__main__":
    unittest.main()
