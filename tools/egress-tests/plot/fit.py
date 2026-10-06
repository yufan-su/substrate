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

"""Fits per-replica cost models to egress-tests runs at fixed pacing.

Usage:
  python3 fit.py runs/*.json [--provisional old.json ...] [--csv runs.csv]
                 [--base SHA] [--holdout h1.json ...] [--predict B C]

Each response is a linear form in B (active actors), T = B x C (open
tunnels) and C (Services per actor), fitted by least squares over the
settled window. Terms whose 95% interval includes zero are dropped one at
a time. Holdouts are scored against the fit: CPU within 15% or 0.02 core,
whichever is larger; memory within 15%; settled p99 within two histogram
buckets. Exits 1 when a holdout misses.

--provisional rows are earlier runs of already-tested shapes. They enter the
CPU fits only: their memory deltas came from reused gateway pods, and their
startup, resume and latency figures predate the warm-up. The coefficients
are printed with and without them.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

import runs as rr

# Response -> terms. "1" is the intercept; "BC" is B x C.
FORMS: dict[str, tuple[str, ...]] = {
    "gateway_cores": ("1", "B", "T", "C"),
    "envoy_cores": ("1", "B", "T", "C"),
    "extproc_cores": ("1", "B"),
    "gateway_ws_mib": ("1", "T", "C"),
    "worker_cores": ("1", "B", "T"),
    "atunnel_cores": ("B", "T"),
    "worker_ws_mib": ("B", "T", "C"),
    # C x 0.1 s of pacing plus a connect cost per Service that grows with B.
    "first_round_s": ("C", "BC"),
    "resume_p50_ms": ("1", "B"),
    # T / (C x 0.1) = 10 B: the DNS burst rate is the actors' request rate.
    "dns_p99_ms": ("1", "B"),
    "p99_settled_ms": ("1", "B"),
}

# Without a --pre-idle baseline (runs before it existed), the first 10 s of
# samples stand in for it.
BASELINE_S = 10.0

# Two-sided 97.5% t quantiles by degrees of freedom; 1.96 beyond 30.
T975 = [0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
        2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
        2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]


def t975(df: int) -> float:
    return T975[df] if 0 < df < len(T975) else 1.96


def term_value(term: str, row: dict) -> float:
    if term == "1":
        return 1.0
    if term == "BC":
        return row["B"] * row["C"]
    return float(row[term])


# ---- extraction -----------------------------------------------------------

def window_mean(points: list[tuple[float, float]], lo: float, hi: float) -> float | None:
    vals = [v for t, v in points if lo <= t <= hi]
    return sum(vals) / len(vals) if vals else None


def source_for(run: rr.Run, metric: str, component: str) -> tuple[str, ...]:
    """The cgroup reader's rows when it read the component, else cAdvisor's."""
    if any(r["component"] == component for r in run.series(metric, ("cgreader",))):
        return ("cgreader",)
    return ("cadvisor",)


def cpu_points(run: rr.Run, component: str, containers: set[str]) -> dict[str, list[tuple[float, float]]]:
    """Per-pod summed CPU of the given containers, one point per reading."""
    by_pod: dict[str, dict[float, float]] = {}
    for r in run.series("cpu_cores", source_for(run, "cpu_cores", component)):
        if r["component"] == component and r.get("container") in containers:
            t = rr.parse_time(r["t"])
            by_pod.setdefault(r.get("pod", ""), {}).setdefault(t, 0.0)
            by_pod[r.get("pod", "")][t] += r["value"]
    return {p: sorted(d.items()) for p, d in by_pod.items()}


def summed_mean(per_pod: dict[str, list[tuple[float, float]]], lo: float, hi: float) -> float | None:
    means = [window_mean(pts, lo, hi) for pts in per_pod.values()]
    means = [m for m in means if m is not None]
    return sum(means) if means else None


def ws_delta_mib(run: rr.Run, component: str, lo: float, hi: float) -> float | None:
    """Steady max minus the baseline, summed over pods, from the pod cgroups.
    The baseline is the driver's pre-idle maximum when the run has one."""
    base = ((run.report.get("resources") or {}).get("baseline") or {}).get(component)
    by_pod: dict[str, list[tuple[float, float]]] = {}
    for r in run.series("working_set_bytes", source_for(run, "working_set_bytes", component)):
        if r["component"] == component and r.get("container") == "POD":
            by_pod.setdefault(r.get("pod", ""), []).append((rr.parse_time(r["t"]), r["value"]))
    if not by_pod:
        return None
    if base and not base.get("insufficient"):
        steady = sum(max((v for t, v in pts if lo <= t <= hi), default=0.0) for pts in by_pod.values())
        return (steady - base["workingSetBytes"]) / 2**20
    start = min(t for pts in by_pod.values() for t, _ in pts)
    total = 0.0
    for pts in by_pod.values():
        base = [v for t, v in pts if t <= start + BASELINE_S]
        steady = [v for t, v in pts if lo <= t <= hi]
        if base and steady:
            total += max(steady) - min(base)
    return total / 2**20


def extract(path: str | Path, base: str = "") -> dict:
    run = rr.load_run(path)
    rep = run.report
    cfg, loop, res = rep["config"], rep.get("loop") or {}, rep.get("resources") or {}
    B, C = cfg["parallel"], cfg["endpoints"]
    s0, s1 = run.phases["steady"]
    settled = res.get("settledWindow")
    lo = rr.parse_time(settled["start"]) if settled else s0
    elapsed = loop.get("elapsed", 0) / 1e9
    row = {
        "run": Path(path).name, "B": B, "C": C, "T": B * C,
        "R_measured": loop.get("requests", 0) / elapsed if elapsed else 0.0,
        "interval_ms": cfg.get("requestInterval", 0) / 1e6,
        "base": base or rep.get("base", ""),
        "breakers": rep.get("breakers", ""),
        "settled_rule": settled.get("rule", "") if settled else "",
        "policies_updated": (rep.get("create") or {}).get("policiesUpdated", 0),
        "warmup": cfg.get("duration", 0) < 60e9,
        "provisional": False,
    }
    gw_ctr = {"envoy", "ext-proc", "sdsmint"}
    row["gateway_cores"] = summed_mean(cpu_points(run, "gateway", gw_ctr), lo, s1)
    row["envoy_cores"] = summed_mean(cpu_points(run, "gateway", {"envoy"}), lo, s1)
    row["extproc_cores"] = summed_mean(cpu_points(run, "gateway", {"ext-proc"}), lo, s1)
    row["worker_cores"] = summed_mean(cpu_points(run, "workers", {"ateom"}), lo, s1)
    row["atunnel_cores"] = summed_mean(cpu_points(run, "workers", {"atunnel"}), lo, s1)
    row["gateway_ws_mib"] = ws_delta_mib(run, "gateway", s0, s1)
    row["worker_ws_mib"] = ws_delta_mib(run, "workers", s0, s1)
    row["first_round_s"] = lo - s0 if settled and settled.get("rule") != "1.5 rounds" else None
    # Own-snapshot resumes only, when the driver reports the source; a first
    # resume from the golden snapshot is a different population.
    actors = [a for a in rep.get("actors") or [] if a.get("resumeLatency")]
    if any("resumeSource" in a for a in actors):
        actors = [a for a in actors if a.get("resumeSource") == "own"]
    resumes = [a["resumeLatency"] / 1e6 for a in actors]
    row["resume_p50_ms"] = rr.median(resumes) if resumes else None
    row["dns_p99_ms"] = rr.quantile_ms(loop["dns"], 0.99) if (loop.get("dns") or {}).get("count") else None
    row["p99_settled_ms"] = settled["p99"] / 1e6 if settled and settled.get("p99") else None
    start_env = [e for e in res.get("envoy") or [] if e.get("label") == "start:begin"]
    row["cleartext_active_at_start"] = sum(
        e["counters"].get("cluster.egress_forward_proxy_cleartext.upstream_cx_active", 0) for e in start_env)
    return row


# ---- fitting --------------------------------------------------------------

@dataclass
class Fit:
    response: str
    terms: list[str]
    coef: np.ndarray
    cov: np.ndarray
    sigma2: float
    df: int
    n: int
    dropped: list[str] = field(default_factory=list)

    def interval(self, i: int) -> tuple[float, float]:
        h = t975(self.df) * math.sqrt(max(self.cov[i, i], 0.0))
        return self.coef[i] - h, self.coef[i] + h

    def predict(self, row: dict) -> tuple[float, float]:
        """Prediction and the half-width of its 95% prediction interval."""
        x = np.array([term_value(t, row) for t in self.terms])
        var = self.sigma2 + float(x @ self.cov @ x)
        return float(x @ self.coef), t975(self.df) * math.sqrt(max(var, 0.0))


def ols(rows: list[dict], response: str, terms: list[str]) -> Fit | None:
    use = [r for r in rows if r.get(response) is not None]
    n, p = len(use), len(terms)
    if n <= p:
        return None
    X = np.array([[term_value(t, r) for t in terms] for r in use])
    y = np.array([r[response] for r in use], dtype=float)
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coef
    sigma2 = float(resid @ resid) / (n - p)
    cov = sigma2 * np.linalg.pinv(X.T @ X)
    return Fit(response, list(terms), coef, cov, sigma2, n - p, n)


def fit_response(rows: list[dict], response: str, terms: tuple[str, ...]) -> Fit | None:
    """Least squares, dropping the weakest term whose interval includes zero
    until every remaining interval excludes it or one term is left."""
    cur, dropped = list(terms), []
    while True:
        f = ols(rows, response, cur)
        if f is None:
            return None
        weak = [(abs(f.coef[i]) / math.sqrt(f.cov[i, i]) if f.cov[i, i] > 0 else math.inf, i)
                for i in range(len(cur)) if f.interval(i)[0] <= 0 <= f.interval(i)[1]]
        if not weak or len(cur) == 1:
            f.dropped = dropped
            return f
        _, i = min(weak)
        dropped.append(cur.pop(i))


def fit_all(rows: list[dict], use_provisional: bool = True) -> dict[str, Fit]:
    fits = {}
    for resp, terms in FORMS.items():
        use = [r for r in rows if not r.get("provisional") or (use_provisional and resp in PROVISIONAL_OK)]
        f = fit_response(use, resp, terms)
        if f is not None:
            fits[resp] = f
    return fits


# ---- acceptance -----------------------------------------------------------

def bucket_index(ms: float) -> int:
    """The histogram bucket holding ms. Quantiles report a bucket's bound
    rounded up to the microsecond, hence the 1 µs allowance."""
    us = ms * 1000
    return next((i for i, b in enumerate(rr.BUCKETS) if b >= us - 1), len(rr.BUCKETS))


def accept(response: str, predicted: float, measured: float) -> bool:
    if response.endswith("_cores"):
        return abs(predicted - measured) <= max(0.15 * abs(measured), 0.02)
    if response.endswith("_mib"):
        return abs(predicted - measured) <= 0.15 * abs(measured)
    if response == "p99_settled_ms":
        return abs(bucket_index(predicted) - bucket_index(measured)) <= 2
    return True  # startup terms are reported, not judged


# Responses provisional rows may enter: CPU only.
PROVISIONAL_OK = [r for r in FORMS if r.endswith("_cores")]

JUDGED = [r for r in FORMS if r.endswith(("_cores", "_mib")) or r == "p99_settled_ms"]


def score(fits: dict[str, Fit], holdout: dict) -> list[tuple[str, float, float, float, bool]]:
    out = []
    for resp in JUDGED:
        f, meas = fits.get(resp), holdout.get(resp)
        if f is None or meas is None:
            continue
        pred, half = f.predict(holdout)
        out.append((resp, pred, half, meas, accept(resp, pred, meas)))
    return out


# ---- output ---------------------------------------------------------------

CSV_FIELDS = ["run", "provisional", "B", "C", "T", "R_measured", "interval_ms", "base", "breakers", "warmup",
              "policies_updated", "cleartext_active_at_start", "settled_rule", *FORMS]


def write_csv(rows: list[dict], path: str) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in CSV_FIELDS})


def print_fits(fits: dict[str, Fit], out=sys.stdout) -> None:
    for resp, f in fits.items():
        dropped = f" (dropped {', '.join(f.dropped)})" if f.dropped else ""
        print(f"{resp}: n={f.n} sigma={math.sqrt(f.sigma2):.4g}{dropped}", file=out)
        for i, t in enumerate(f.terms):
            lo, hi = f.interval(i)
            print(f"  {t:>3} {f.coef[i]: .5g}  [{lo:.5g}, {hi:.5g}]", file=out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("runs", nargs="+", help="--output JSON files of the fitting runs")
    p.add_argument("--provisional", nargs="*", default=[],
                   help="earlier runs of tested shapes: CPU fits only")
    p.add_argument("--csv", default="runs.csv", help="CSV to write, one row per run")
    p.add_argument("--base", default="", help="base commit, when the JSONs do not carry one")
    p.add_argument("--holdout", nargs="*", default=[], help="JSONs to score against the fit")
    p.add_argument("--predict", nargs=2, type=int, metavar=("B", "C"))
    args = p.parse_args(argv)
    rows = [r for r in (extract(f, args.base) for f in args.runs) if not r["warmup"]]
    for f in args.provisional:
        rows.append(extract(f, args.base) | {"provisional": True})
    write_csv(rows, args.csv)
    fits = fit_all(rows)
    if args.provisional:
        print("== without provisional rows")
        print_fits(fit_all(rows, use_provisional=False))
        print(f"== with {len(args.provisional)} provisional rows (CPU only)")
    print_fits(fits)
    if args.predict:
        b, c = args.predict
        q = {"B": b, "C": c, "T": b * c}
        print(f"predict B={b} C={c} (T={b * c}):")
        for resp, f in fits.items():
            pred, half = f.predict(q)
            print(f"  {resp:16} {pred:.4g} ± {half:.2g}")
    missed = False
    for h in args.holdout:
        row = extract(h, args.base)
        print(f"holdout {row['run']} B={row['B']} C={row['C']}:")
        for resp, pred, half, meas, ok in score(fits, row):
            missed |= not ok
            print(f"  {'ok  ' if ok else 'MISS'} {resp:16} predicted {pred:.4g} ± {half:.2g}, measured {meas:.4g}")
    return 1 if missed else 0


if __name__ == "__main__":
    sys.exit(main())
