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
import csv
import hashlib
import io
import json
import tempfile
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


class JsonDocumentTest(unittest.TestCase):
    def setUp(self):
        self.rows = synthetic(GRID + REPLICATES, seed=1)
        for i, r in enumerate(self.rows):
            r |= {"run": f"r{i}.json", "scheme": "http", "base": "47ed2247", "interval_ms": 100.0,
                  "R_measured": 9.7 * r["B"], "cleartext_active_max": (12 if r["C"] == 10 else 6) * r["C"],
                  "envoy_heap_floor_mib": 11.0 + i % 3}
        self.rows.append({"run": "p.json", "provisional": True, "scheme": "http", "B": 200, "C": 10, "T": 2000,
                          "R_measured": 1.0, "cleartext_active_max": 1.0, "gateway_cores": 1.8})
        self.fits = fit.fit_all(self.rows)
        self.holdout = synthetic([(80, 160)], seed=3, noise=0.0)[0] | {"run": "h.json", "scheme": "http"}

    def test_document(self):
        with tempfile.TemporaryDirectory() as d:
            src = Path(d) / "r0.json"
            src.write_text("{}")
            doc = fit.fit_document(self.rows, self.fits, [self.holdout], inputs=[str(src)], fit_at="2026-10-07T17:00:00Z")
            out = Path(d) / "fit.json"
            fit.write_json(doc, str(out))
            got = json.loads(out.read_text())
        prov = got["provenance"]
        self.assertEqual((prov["fitAt"], prov["base"], prov["scheme"], prov["intervalMs"], prov["runs"], prov["provisional"]),
                         ("2026-10-07T17:00:00Z", ["47ed2247"], "http", [100.0], len(GRID + REPLICATES), 1))
        self.assertEqual(prov["inputs"], [{"name": "r0.json", "sha256": hashlib.sha256(b"{}").hexdigest()}])
        # The provisional row enters neither the rate, the pool nor the measured ranges.
        self.assertAlmostEqual(got["ratePerActor"], 9.7)
        self.assertEqual(got["poolPerService"], {"lo": 6, "hi": 12})
        self.assertEqual(got["floors"]["envoy_heap_mib"], 12.0)
        self.assertEqual(got["measured"], {"B": [10, 100], "C": [10, 256], "T": [100, 12800]})
        self.assertEqual(got["rssOverHeap"], fit.RSS_OVER_HEAP)
        self.assertEqual(len(got["rows"]), len(self.rows))
        self.assertEqual(set(got["fits"]), set(self.fits))
        f = got["fits"]["gateway_cores"]
        self.assertEqual(f["terms"], self.fits["gateway_cores"].terms)
        self.assertEqual(f["t975"], fit.t975(f["df"]))
        self.assertEqual(len(got["checks"]), len(fit.CHECK_POINTS) * len(self.fits))
        for c in got["checks"]:
            with self.subTest(check=(c["response"], c["B"], c["C"])):
                pred, half = self.fits[c["response"]].predict({"B": c["B"], "C": c["C"], "T": c["B"] * c["C"]})
                self.assertEqual((c["predicted"], c["halfWidth"]), (pred, half))
        h = got["holdouts"][0]
        self.assertEqual((h["run"], h["B"], h["C"]), ("h.json", 80, 160))
        self.assertTrue(all(r["ok"] for r in h["responses"].values()), h)

    def test_no_pool_or_heap_readings(self):
        for r in self.rows:
            r["cleartext_active_max"] = r["envoy_heap_floor_mib"] = None
        doc = fit.fit_document(self.rows, self.fits)
        self.assertIsNone(doc["poolPerService"])
        self.assertIsNone(doc["floors"]["envoy_heap_mib"])

    def test_main_writes_json(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "fit.json"
            with contextlib.redirect_stdout(io.StringIO()) as so:
                got = fit.main([str(FIXTURE), "--csv", str(Path(d) / "runs.csv"), "--json", str(out)])
            self.assertEqual(got, 0)
            self.assertIn(f"wrote {out}", so.getvalue())
            doc = json.loads(out.read_text())
        self.assertEqual(doc["schema"], fit.JSON_SCHEMA)
        self.assertEqual([i["name"] for i in doc["provenance"]["inputs"]], ["run-b10.json"])


class AcceptTest(unittest.TestCase):
    def test_rules(self):
        cases = [
            ("cpu within 15%", "envoy_cores", 1.10, 1.00, True),
            ("cpu off by 20%", "envoy_cores", 1.20, 1.00, False),
            ("cpu small, 18% off but inside the 0.02 floor", "envoy_cores", 0.118, 0.10, True),
            ("cpu small, past the floor", "envoy_cores", 0.13, 0.10, False),
            ("memory within 15%", "gateway_ws_mib", 110, 100, True),
            ("memory off by 20%", "gateway_ws_mib", 120, 100, False),
            ("p99 two buckets up", "p99_settled_ms", 5.0, 4.0, True),
            ("p99 three buckets up", "p99_settled_ms", 5.7, 4.0, False),
            ("startup is not judged", "first_round_s", 99, 1, True),
        ]
        for name, resp, pred, meas, want in cases:
            with self.subTest(name):
                self.assertEqual(fit.accept(resp, pred, meas), want)


class ExtractTest(unittest.TestCase):
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
        res["baseline"] = {"gateway": {"workingSetBytes": 10 * 2**20, "cpuCores": {"mean": 0.01, "max": 0.01}}}
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            row = fit.extract(p)
        self.assertAlmostEqual(row["p99_settled_ms"], 3.548)
        self.assertAlmostEqual(row["gateway_ws_mib"], 30)

    def test_envoy_heap(self):
        # Envoy's /memory allocated bytes per gateway pod: the settled max
        # minus the pre-idle max, summed over pods.
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
            ("no /memory samples", [], None),
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

    def test_envoy_heap_floor_and_cleartext_max(self):
        report = json.loads(FIXTURE.read_text())
        steady = next(p for p in report["phases"] if p["name"] == "steady")
        pre_start, pre_end = "2020-01-01T00:00:00Z", "2020-01-01T00:00:30Z"
        report["phases"].insert(0, {"name": "pre-idle", "start": pre_start, "end": pre_end})
        report["resources"]["envoyMemory"] = [
            {"t": t, "pod": pod, "allocatedBytes": mib * 2**20}
            for t, pod, mib in [(pre_end, "gw-a", 11), (steady["end"], "gw-a", 50), (pre_end, "gw-b", 9), (steady["end"], "gw-b", 20)]]

        def active(t, pod, v):
            return {"t": t, "source": "envoy", "component": "gateway", "pod": pod, "container": "envoy",
                    "metric": fit.CLEARTEXT_ACTIVE, "value": v}
        report["resources"]["series"] += [active(pre_end, "gw-a", 999), active(steady["start"], "gw-a", 70),
                                          active(steady["end"], "gw-a", 40), active(steady["end"], "gw-b", 30)]
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            p.write_text(json.dumps(report))
            row = fit.extract(p)
        self.assertAlmostEqual(row["envoy_heap_mib"], 50)
        self.assertAlmostEqual(row["envoy_heap_floor_mib"], 20)
        # The per-pod maxima over steady, summed; the pre-idle reading is outside.
        self.assertEqual(row["cleartext_active_max"], 100)
        self.assertIsNone(fit.extract(FIXTURE)["cleartext_active_max"])

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

    def test_scheme_defaults_to_http(self):
        self.assertEqual(fit.extract(FIXTURE)["scheme"], "http")

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

    def test_skips_files_that_are_not_run_reports(self):
        with tempfile.TemporaryDirectory() as d:
            gate = Path(d) / "A-c010-b010.gate.json"
            gate.write_text(json.dumps({"waited_s": 0, "residual": 0, "ok": True, "reads": 1, "gatewayPod": "gw"}))
            csv_path = Path(d) / "runs.csv"
            for name, args in [
                ("a fitting run", [str(FIXTURE), str(gate)]),
                ("a provisional row", [str(FIXTURE), "--provisional", str(gate)]),
                ("a holdout", [str(FIXTURE), "--holdout", str(gate)]),
            ]:
                with self.subTest(name):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err:
                        got = fit.main([*args, "--csv", str(csv_path)])
                    self.assertEqual(got, 0, err.getvalue())
                    self.assertEqual(err.getvalue(), f"skipped {gate}: not a run report\n")
                    with open(csv_path) as f:
                        self.assertEqual([r["run"] for r in csv.DictReader(f)], ["run-b10.json"])

    def test_no_settled_p99_without_the_driver_field(self):
        self.assertIsNone(fit.extract(FIXTURE)["p99_settled_ms"])


if __name__ == "__main__":
    unittest.main()
