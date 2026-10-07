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


"""Runs the tunnel-cap campaign: warm-up, base check, fitting grid,
replicates, holdouts, E0 and the edge runs, one after another on one cluster.

Usage:
  python3 campaign.py --context CTX [--kubeconfig FILE] --driver PATH --out DIR
                      [--plan tunnel-cap|ladder] [--ref-dir DIR] [--base-check-ref T3.json]
                      [--no-provisional] [--in-cluster] [--dry-run]

Run it with a Python that has numpy (plot/requirements.txt): the base check
reads its runs through plot/fit.py.

Every run passes --pick first, so the B=100 warm-up gives actors 0-99 their
own snapshots before any measured run.

Right after the warm-up, the base check runs the B=100 C=10 cell and
compares its settled gateway cores with --base-check-ref (T3, measured
before the campaign's setup): within 15% or 0.02 core passes. The verdict
goes to DIR/base-check.json, and the fit passes the provisional rows from
--ref-dir only on a pass. The run also counts as Block A's B=100 C=10 cell.
Provisional rows missing from --ref-dir are left out, with a line saying so;
--no-provisional leaves them all out.

At the fit stop the runner fits DIR/A-c???-b???.json and C-c???-b???.json,
prints the coefficients, and writes DIR/predictions.json: each holdout's
judged responses, predicted with their 95% half-widths. It also writes
DIR/fit.json and the predictor page DIR/predictor.html. An existing
predictions.json is kept; delete it and the go file to refit.

Before every measured run it waits until the gateway's cleartext pool is
empty (upstream_cx_active == 0, read from Envoy's /stats), at most 8 min,
and records the wait and the gateway pod in <run>.gate.json. The gateway
pod is recorded in DIR/block-gateway-pod at the first run of each block; a
run whose gateway pod differs is refused, also after a resume, since only an
OWNER step may restart the gateway. An OWNER step that restarts it starts a
new block.

At every OWNER step it prints the commands and waits: for Enter when stdin
is a TTY, otherwise for DIR/go/<step-id> to appear, polled every 10 s, with
the `! touch` line to run printed. It refuses to start a run when the
previous run's JSON is missing, and a holdout without a written prediction
in DIR/predictions.json. Runs whose JSON already exists are skipped, so a
stopped campaign resumes where it left off.

--in-cluster runs inside the egress-campaign Pod (deploy.sh --campaign):
the driver dials ateapi and the router by Service name with the Pod's
projected token, and the runner's kubectl uses the in-cluster config.
--context, and --kubeconfig when given, still name the owner's cluster, for
the printed commands and the go lines, which become `kubectl exec ...
touch`. On a laptop they reach every kubectl and driver call too, so
nothing falls back to the current context.

Measured runs pass --pre-idle and --wait-for-cleartext-idle to the driver.
DRIVER_HAS_PREIDLE=0 drops them, for a driver built before those flags.
PROGRESS_INTERVAL overrides the 1s polls of every run but the warm-up,
whose halves always poll at 1 s and 5 s; the campaign Pod sets it to 5s.

--plan ladder runs the scale ladder instead: a warm-up of all 1000 actors,
then B x C = 100 x 1000, 300 x 1000, 1000 x 300 and 1000 x 1000, with a
gateway restart between rungs, and a worker reshape to 44 x 7Gi before
the last. Each rung runs for its first round, from the
campaign's fit, plus 3 min to settle, and rungs with B >= 300 poll progress
every 15 s, since each poll asks all B actors at once. It has no base check,
fit or holdouts.
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

CLEARTEXT_ACTIVE = "cluster.egress_forward_proxy_cleartext.upstream_cx_active"
GATE_CAP_S = 8 * 60
GATE_POLL_S = 10
GO_POLL_S = 10
# The block's gateway pod, kept across resumes.
BLOCK_POD_FILE = "block-gateway-pod"
ACTORS = 1000
# Printed in the E0 owner commands when --scripts-dir is not given.
SCRIPTS_PLACEHOLDER = "<scripts-dir>"
PLOT = Path(__file__).resolve().parent.parent / "plot"

# Untested columns first; C=10 is the best-known column.
GRID_C = (256, 128, 50, 10)
GRID_B = (10, 25, 50, 100)
CAP = 16384
REPLICATES = ((10, 256), (100, 50), (50, 256))
HOLDOUTS = ((20, 75), (40, 200), (80, 160), (75, 20))
EDGES = ((64, 256), (100, 160))
# The base check's cell, measured as T3 before the campaign.
BASE_CHECK = (100, 10)
REF_DIR = "/tmp/egress-tests-resources-plan-2026-10-06/tip"
BASE_CHECK_FILE = "t3-b100-c10.json"
PROVISIONAL_FILES = ("t3-b100-c10.json", "t1a-b10-c100.json", "t1b-b10-c100.json", "t2-b11-c100.json")
# A run within this of the reference passes, the holdouts' CPU rule.
BASE_CHECK_RESPONSE = "gateway_cores"
# What the driver dials from inside the cluster, and where the Pod mounts
# its projected ateapi token (campaign.yaml.tmpl).
IN_CLUSTER_API = "api.ate-system.svc:443"
IN_CLUSTER_ROUTER = "http://atenet-router.ate-system.svc"
IN_CLUSTER_TOKEN = "/var/run/ateapi/token"


@dataclass
class Step:
    kind: str  # "run" or "owner"
    name: str = ""
    B: int = 0
    C: int = 0
    duration: str = "3m"
    measured: bool = True
    holdout: bool = False
    base_check: bool = False
    commands: list[str] = field(default_factory=list)
    note: str = ""
    # restarts: the owner step may restart the gateway.
    restarts: bool = False
    # fit: the commands are the fit, built when the step is reached.
    fit: bool = False
    # progress_interval, when set, overrides PROGRESS_INTERVAL for this run.
    progress_interval: str = ""


def owner(name: str, note: str, *commands: str, restarts: bool = False, fit: bool = False) -> Step:
    return Step("owner", name=name, note=note, commands=list(commands), restarts=restarts, fit=fit)


def run(name: str, b: int, c: int, **kw) -> Step:
    return Step("run", name=name, B=b, C=c, **kw)


def cluster_args(ctx: str, kubeconfig: str = "") -> list[str]:
    """--kubeconfig, when given, and --context: never the current context."""
    return (["--kubeconfig", kubeconfig] if kubeconfig else []) + ["--context", ctx]


def plan(ctx: str, kubeconfig: str = "", scripts: str = SCRIPTS_PLACEHOLDER) -> list[Step]:
    k = shlex.join(["kubectl", *cluster_args(ctx, kubeconfig)])
    restart_gw = (f"{k} -n ate-system rollout restart deploy/atenet-egress && "
                  f"{k} -n ate-system rollout status deploy/atenet-egress --timeout=300s")
    roll_workers = (f"{k} -n egress-tests rollout restart deploy/egress-tests && "
                    f"{k} -n egress-tests rollout status deploy/egress-tests --timeout=600s")
    steps = [owner("start", "campaign start: wired, Wi-Fi off; restart the gateway and roll the workers",
                   restart_gw, roll_workers, restarts=True)]
    # The warm-up gives every actor its own snapshot, and its two halves
    # compare the 1 s and 5 s progress polls' cost on the router and workers.
    steps += [run("warmup-1s", 100, 10, duration="45s", measured=False),
              run("warmup-5s", 100, 10, duration="45s", measured=False),
              owner("poll-check", "compare router and worker CPU in warmup-1s vs warmup-5s; if they differ by "
                    "more than 0.02 core, stop and rerun with PROGRESS_INTERVAL=2s")]
    b, c = BASE_CHECK
    steps += [run(f"A-c{c:03d}-b{b:03d}", b, c, base_check=True)]
    steps += [run(f"A-c{c:03d}-b{b:03d}", b, c) for c in GRID_C for b in GRID_B
              if b * c <= CAP and (b, c) != BASE_CHECK]
    steps += [owner("before-C", "before Block C: restart the gateway", restart_gw, restarts=True)]
    steps += [run(f"C-c{c:03d}-b{b:03d}", b, c) for b, c in REPLICATES]
    steps += [owner("fit", "the runner fits and writes OUT/predictions.json below; review both. "
                    "To refit, delete it and the go file",
                    fit=True)]
    steps += [owner("before-D", "before Block D: restart the gateway and roll the workers",
                    restart_gw, roll_workers, restarts=True)]
    steps += [run(f"D-c{c:03d}-b{b:03d}", b, c, holdout=True) for b, c in HOLDOUTS]
    steps += [owner("e0-apply", "E0: mitm_internal breaker to 1024", f"bash {scripts}/e0-apply.sh", restarts=True),
              run("E0-c100-b011", 11, 100, duration="2m"),
              owner("e0-revert", "E0 done: mitm_internal breaker back to 16384", f"bash {scripts}/e0-revert.sh",
                    restarts=True)]
    for b, c in EDGES:
        name = f"E-c{c:03d}-b{b:03d}"
        steps += [run(name, b, c),
                  owner(f"after-{name}", f"after edge B={b} C={c}: restart the gateway", restart_gw, restarts=True)]
    return steps


# The scale ladder's rungs, (B, C).
LADDER = ((100, 1000), (300, 1000), (1000, 300), (1000, 1000))
LADDER_SETTLE_S = 180
LADDER_SLOW_POLLS_FROM_B = 300
LADDER_SLOW_POLLS = "15s"
# Above this many tunnels the 16-pod worker shape runs out of source ports.
LADDER_RESHAPE_ABOVE = 300_000


def first_round_s(b: int, c: int) -> float:
    """The first round's length, from the campaign's fit (B <= 100):
    0.13 s per Service plus 0.0017 s per tunnel."""
    return 0.13 * c + 0.0017 * b * c


def ladder_duration(b: int, c: int) -> str:
    """The first round plus the settle time, rounded up to a whole minute."""
    return f"{-(-int(first_round_s(b, c) + LADDER_SETTLE_S) // 60)}m"


def ladder_plan(ctx: str, kubeconfig: str = "", scripts: str = SCRIPTS_PLACEHOLDER) -> list[Step]:
    del scripts  # the ladder has no E0
    k = shlex.join(["kubectl", *cluster_args(ctx, kubeconfig)])
    restart_gw = (f"{k} -n ate-system rollout restart deploy/atenet-egress && "
                  f"{k} -n ate-system rollout status deploy/atenet-egress --timeout=300s")
    roll_workers = (f"{k} -n egress-tests rollout restart deploy/egress-tests && "
                    f"{k} -n egress-tests rollout status deploy/egress-tests --timeout=600s")
    steps = [owner("start", "ladder start: restart the gateway and roll the workers", restart_gw, roll_workers,
                   restarts=True),
             run("warmup", ACTORS, 10, duration="2m", measured=False, progress_interval=LADDER_SLOW_POLLS)]
    for i, (b, c) in enumerate(LADDER, start=1):
        if i > 1:
            note = f"before rung {i} (B={b} C={c}): restart the gateway"
            if b * c > LADDER_RESHAPE_ABOVE:
                note = (f"before rung {i} (B={b} C={c}): reshape the workers to 44 x 7Gi for the worker pods' "
                        "source ports, then restart the gateway")
            steps += [owner(f"before-rung{i}", note, restart_gw, restarts=True)]
        steps += [run(f"L{i}-c{c:04d}-b{b:04d}", b, c, duration=ladder_duration(b, c),
                      progress_interval=LADDER_SLOW_POLLS if b >= LADDER_SLOW_POLLS_FROM_B else "")]
    return steps


PLANS = {"tunnel-cap": plan, "ladder": ladder_plan}


def driver_args(driver: str, ctx: str, step: Step, out: Path, env: dict, in_cluster: bool = False,
                kubeconfig: str = "") -> list[str]:
    where = (["--api-endpoint", IN_CLUSTER_API, "--api-token-file", IN_CLUSTER_TOKEN, "--router-url", IN_CLUSTER_ROUTER]
             if in_cluster else cluster_args(ctx, kubeconfig))
    args = [driver, "run", *where, "--actors", str(ACTORS), "--pick", "first",
            "--parallel", str(step.B), "--endpoints", str(step.C), "--duration", step.duration,
            "--request-interval", "100ms",
            "--progress-interval", step.progress_interval or env.get("PROGRESS_INTERVAL", "1s")]
    # The warm-up's halves compare the two intervals whatever the override.
    if step.name in ("warmup-1s", "warmup-5s"):
        args[-1] = step.name.removeprefix("warmup-")
    args += ["--resources", "--resources-cgreader", "--resources-verify",
             "--output", str(out / f"{step.name}.json")]
    if env.get("DRIVER_HAS_PREIDLE", "1") != "0" and step.measured:
        args += ["--pre-idle", "30s", "--wait-for-cleartext-idle", "8m"]
    return args


# The fitting runs' JSONs, not their .gate.json files.
FIT_GLOBS = ("A-c???-b???.json", "C-c???-b???.json")


def provisional_rows(out: Path, provisional: list[Path]) -> tuple[list[Path], str]:
    """The provisional rows, only after a passed base check, and a line
    saying why."""
    try:
        verdict = json.loads((out / "base-check.json").read_text())
    except (OSError, ValueError):
        return [], "base check not recorded: fitting without the provisional rows"
    if verdict.get("pass"):
        if not provisional:
            return [], "base check passed, but there are no provisional rows: fitting without them"
        return provisional, "base check passed: provisional rows included"
    return [], "base check FAILED: fitting without the provisional rows"


def fit_commands(out: Path, provisional: list[Path]) -> tuple[list[str], str]:
    """The fit step's command, which the runner also runs itself."""
    rows, why = provisional_rows(out, provisional)
    cmd = (f"{shlex.quote(sys.executable)} {PLOT / 'fit.py'} {out}/{FIT_GLOBS[0]} {out}/{FIT_GLOBS[1]} "
           f"--csv {out}/runs.csv --json {out}/fit.json")
    if rows:
        cmd += " --provisional " + " ".join(str(p) for p in rows)
    return [cmd], why


@dataclass
class Gate:
    waited_s: float
    residual: float
    ok: bool
    reads: int


def wait_for_idle(read_active, sleep=time.sleep, now=time.monotonic,
                  cap_s: float = GATE_CAP_S, poll_s: float = GATE_POLL_S) -> Gate:
    """Polls until the cleartext pool reads 0, or until cap_s has passed."""
    start, reads = now(), 0
    while True:
        active = read_active()
        reads += 1
        waited = now() - start
        if active == 0:
            return Gate(waited, 0, True, reads)
        if waited >= cap_s:
            return Gate(waited, active, False, reads)
        sleep(poll_s)


def kubectl(ctx: str, in_cluster: bool, kubeconfig: str = "") -> list[str]:
    """The runner's own kubectl; in the cluster it uses the Pod's config."""
    return ["kubectl"] if in_cluster else ["kubectl", *cluster_args(ctx, kubeconfig)]


def gateway(kube: list[str]) -> tuple[str, str]:
    """The newest running gateway pod, and its node."""
    out = subprocess.run([*kube, "-n", "ate-system", "get", "pod", "-l", "app=atenet-egress",
                          "--field-selector=status.phase=Running", "--sort-by=.metadata.creationTimestamp",
                          "-o", "jsonpath={.items[-1:].metadata.name} {.items[-1:].spec.nodeName}"],
                         check=True, capture_output=True, text=True).stdout.split()
    return (out + ["", ""])[0], (out + ["", ""])[1]


def gateway_pod(kube: list[str]) -> str:
    return gateway(kube)[0]


def kubectl_active(kube: list[str]) -> float:
    """The gateway's cleartext upstream_cx_active, read through the API server."""
    pod = gateway_pod(kube)
    path = f"/api/v1/namespaces/ate-system/pods/{pod}:15000/proxy/stats?filter=egress_forward_proxy_cleartext.upstream_cx_active"
    body = subprocess.run([*kube, "get", "--raw", path],
                          check=True, capture_output=True, text=True).stdout
    for line in body.splitlines():
        name, _, value = line.partition(": ")
        if name.strip() == CLEARTEXT_ACTIVE:
            return float(value)
    raise RuntimeError(f"{CLEARTEXT_ACTIVE} missing from {pod} /stats")


def fit_module():
    sys.path.insert(0, str(PLOT))
    import fit  # noqa: E402  (needs numpy, so only when the base check or the fit runs)
    return fit


def fit_extract(path: Path) -> dict:
    return fit_module().extract(path)


def predict_holdouts(rows: list[dict], holdouts: list[Step], say) -> dict:
    """Fits rows as plot/fit.py does, prints the coefficients, and returns
    each holdout's judged responses predicted at its (B, C)."""
    fit = fit_module()
    try:
        fit.one_scheme(rows)
    except ValueError as e:
        raise Refused(f"fit: {e}")
    fits = fit.fit_all(rows)
    text = io.StringIO()
    fit.print_fits(fits, out=text, rows=rows)
    for line in text.getvalue().splitlines():
        say(f"      {line}")
    preds = {}
    for h in holdouts:
        q = {"B": h.B, "C": h.C, "T": h.B * h.C}
        responses = {}
        for resp in fit.JUDGED:
            if resp in fits:
                pred, half = fits[resp].predict(q)
                responses[resp] = {"predicted": pred, "halfWidth": half}
        preds[h.name] = {"B": h.B, "C": h.C, "responses": responses}
    if not any(p["responses"] for p in preds.values()):
        raise Refused(f"fit: no judged response could be fitted from {len(rows)} rows")
    return preds


def render_predictor(rows: list[dict], inputs: list[Path], out: Path) -> list[Path]:
    """Writes OUT/fit.json and OUT/predictor.html from the fit over rows."""
    fit = fit_module()
    import predictor  # noqa: E402  (on the path fit_module set)
    doc = fit.fit_document(rows, fit.fit_all(rows), inputs=[str(p) for p in inputs])
    paths = [out / "fit.json", out / "predictor.html"]
    fit.write_json(doc, str(paths[0]))
    paths[1].write_text(predictor.render(doc))
    return paths


def within_cpu_rule(value: float, ref: float) -> bool:
    return abs(value - ref) <= max(0.15 * abs(ref), 0.02)


class Refused(Exception):
    pass


@dataclass
class Runner:
    ctx: str
    driver: str
    out: Path
    env: dict
    dry_run: bool = False
    execute: object = None
    read_active: object = None
    # read_gateway returns the gateway pod and its node.
    read_gateway: object = None
    base_ref: Path = Path(REF_DIR) / BASE_CHECK_FILE
    provisional: list = field(default_factory=lambda: [Path(REF_DIR) / n for n in PROVISIONAL_FILES])
    in_cluster: bool = False
    # The owner's kubeconfig: used on a laptop, and printed in the owner's
    # commands either way.
    kubeconfig: str = ""
    extract: object = fit_extract
    predict: object = predict_holdouts
    render: object = render_predictor
    prompt: object = input
    isatty: object = sys.stdin.isatty
    say: object = print
    sleep: object = time.sleep
    now: object = time.monotonic

    def walk(self, steps: list[Step]) -> None:
        previous: Step | None = None
        block = ""  # the OWNER step that last restarted the gateway
        holdouts = [s for s in steps if s.holdout]
        if self.in_cluster:
            self.say(f"runner pod {self.env.get('POD_NAMESPACE')}/{self.env.get('POD_NAME')} "
                     f"on node {self.env.get('NODE_NAME')}")
        for i, step in enumerate(steps, 1):
            if step.kind == "owner":
                self.wait_owner(i, step, holdouts)
                if step.restarts:
                    block = step.name
                continue
            out = self.out / f"{step.name}.json"
            if previous is not None and not self.dry_run and not (self.out / f"{previous.name}.json").exists():
                raise Refused(f"{previous.name}.json is missing; not starting {step.name}")
            previous = step
            if out.exists() and not self.dry_run:
                self.say(f"[{i}] skip {step.name}: {out.name} exists")
                if step.base_check and not (self.out / "base-check.json").exists():
                    self.base_check(step)
                continue
            if step.holdout:
                self.check_prediction(step)
            args = driver_args(self.driver, self.ctx, step, self.out, self.env, self.in_cluster, self.kubeconfig)
            self.say(f"[{i}] RUN {step.name} (B={step.B} C={step.C} T={step.B * step.C})")
            pod = node = None
            if not self.dry_run:
                pod, node = self.read_gateway()
                self.check_block_pod(block, pod, step)
                self.check_node(pod, node, step)
            if step.measured:
                self.gate(step, pod, node)
            self.say(f"      $ {shlex.join(args)} > {step.name}.txt")
            if not self.dry_run:
                with open(self.out / f"{step.name}.txt", "w") as log:
                    self.execute(args, log)
                if not out.exists():
                    raise Refused(f"{step.name} wrote no {out.name}; see {step.name}.txt")
            if step.base_check:
                self.base_check(step)

    def check_block_pod(self, block: str, pod: str, step: Step) -> None:
        """Refuses a run whose gateway pod differs from its block's first
        run's, as recorded in BLOCK_POD_FILE; records it at a new block."""
        path = self.out / BLOCK_POD_FILE
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            record = {}
        if record.get("block") == block:
            if record.get("pod") != pod:
                raise Refused(f"the gateway pod changed from {record.get('pod')} to {pod} since this block's first "
                              f"run, {record.get('since')}; not starting {step.name}. Only an OWNER step may restart "
                              f"the gateway. To accept {pod} for the rest of the block, delete {path}.")
            return
        path.write_text(json.dumps({"block": block, "pod": pod, "since": step.name}) + "\n")

    def check_node(self, pod: str, node: str, step: Step) -> None:
        """Refuses a run in the cluster while the gateway shares the runner's
        node, where the driver's CPU would land beside it."""
        runner = self.env.get("NODE_NAME")
        if self.in_cluster and runner and node == runner:
            raise Refused(f"the gateway pod {pod} runs on the runner's node {runner}; not starting {step.name}. "
                          "Restart the gateway so it lands elsewhere.")

    def wait_owner(self, i: int, step: Step, holdouts: list[Step] = ()) -> None:
        commands, why = fit_commands(self.out, self.provisional) if step.fit else (step.commands, "")
        self.say(f"[{i}] OWNER: {step.note}")
        if why:
            self.say(f"      {why}")
        for c in commands:
            self.say(f"      $ {c}")
        if step.fit:
            self.fit(holdouts)
        go = self.out.resolve() / "go" / f"{i:02d}-{step.name}"
        if self.dry_run:
            self.say(f"      go: Enter, or without a TTY: {self.go_line(go)}")
            return
        if self.isatty():
            self.prompt("      press Enter when done ")
            return
        go.parent.mkdir(parents=True, exist_ok=True)
        if not go.exists():
            self.say(f"      waiting for {go}; after the proof read, run:")
            self.say(f"      {self.go_line(go)}")
        while not go.exists():
            self.sleep(GO_POLL_S)

    def go_line(self, go: Path) -> str:
        if not self.in_cluster:
            return f"! touch {go}"
        ns, name = self.env.get("POD_NAMESPACE", "egress-tests"), self.env.get("POD_NAME", "egress-campaign")
        return "! " + shlex.join(["kubectl", *cluster_args(self.ctx, self.kubeconfig), "-n", ns, "exec", name,
                                  "--", "touch", str(go)])

    def fit(self, holdouts: list[Step]) -> None:
        path = self.out / "predictions.json"
        rows_from, _ = provisional_rows(self.out, self.provisional)
        if self.dry_run:
            self.say(f"      fit: {' and '.join(FIT_GLOBS)}; writes {path.name} (dry run: not fitted)")
            return
        if path.exists():
            self.say(f"      fit: keeping the existing {path.name}; delete it and the go file to refit")
            return
        runs = sorted(f for g in FIT_GLOBS for f in glob.glob(str(self.out / g)))
        rows = [r for r in (self.extract(Path(f)) for f in runs) if not r.get("warmup")]
        rows += [self.extract(p) | {"provisional": True} for p in rows_from]
        self.say(f"      fit: {len(runs)} runs, {len(rows_from)} provisional rows")
        preds = self.predict(rows, holdouts, self.say)
        path.write_text(json.dumps(preds, indent=1) + "\n")
        try:
            for p in self.render(rows, [Path(f) for f in runs] + list(rows_from), self.out):
                self.say(f"      wrote {p}")
        except (KeyError, ValueError, OSError) as e:
            self.say(f"      predictor page not written: {type(e).__name__}: {e}")
        for name, p in preds.items():
            parts = [f"{r} {v['predicted']:.4g} ± {v['halfWidth']:.2g}" for r, v in p["responses"].items()]
            self.say(f"      predict {name} B={p['B']} C={p['C']}: {'; '.join(parts)}")
        self.say(f"      wrote {path}")

    def gate(self, step: Step, pod: str | None, node: str | None = None) -> None:
        if self.dry_run:
            self.say(f"      gate: wait until {CLEARTEXT_ACTIVE} == 0, at most {GATE_CAP_S // 60} min (dry run: not polled)")
            return
        g = wait_for_idle(self.read_active, self.sleep, self.now)
        record = g.__dict__ | {"gatewayPod": pod, "gatewayNode": node, "runnerNode": self.env.get("NODE_NAME")}
        (self.out / f"{step.name}.gate.json").write_text(json.dumps(record) + "\n")
        state = "idle" if g.ok else f"NOT idle, {g.residual:.0f} left"
        self.say(f"      gate: {state} after {g.waited_s:.0f}s ({g.reads} reads), gateway {pod}")

    def base_check(self, step: Step) -> None:
        if self.dry_run:
            self.say(f"      base check: {BASE_CHECK_RESPONSE} vs {self.base_ref}, within 15% or 0.02 core "
                     "(dry run: not compared)")
            return
        value = self.extract(self.out / f"{step.name}.json").get(BASE_CHECK_RESPONSE)
        ref = self.extract(self.base_ref).get(BASE_CHECK_RESPONSE)
        ok = value is not None and ref is not None and within_cpu_rule(value, ref)
        verdict = {"run": f"{step.name}.json", "ref": str(self.base_ref), "response": BASE_CHECK_RESPONSE,
                   "value": value, "refValue": ref, "rule": "within 15% or 0.02 core of refValue", "pass": ok}
        (self.out / "base-check.json").write_text(json.dumps(verdict, indent=1) + "\n")
        self.say(f"      base check: {BASE_CHECK_RESPONSE} {value} vs {ref}: {'pass' if ok else 'FAIL'}")

    def check_prediction(self, step: Step) -> None:
        path = self.out / "predictions.json"
        try:
            preds = json.loads(path.read_text())
        except (OSError, ValueError):
            preds = {}
        if step.name not in preds:
            if self.dry_run:
                self.say(f"      holdout: needs {step.name} in {path.name} (dry run: not checked)")
                return
            raise Refused(f"no prediction for {step.name} in {path}; write it before running the holdout")


def run_driver(args: list[str], log) -> None:
    rc = subprocess.run(args, stdout=log, stderr=subprocess.STDOUT).returncode
    # The driver exits 1 when a self-check fails but still writes its JSON;
    # the next step's missing-JSON check catches a run that wrote nothing.
    if rc not in (0, 1):
        raise Refused(f"driver exited {rc}: {shlex.join(args)}")


def provisional_files(ref_dir: Path, enabled: bool, say=print) -> list[Path]:
    """The provisional rows' JSONs that exist in ref_dir, saying which are
    left out."""
    if not enabled:
        say("--no-provisional: the fit leaves out the provisional rows")
        return []
    found = [ref_dir / n for n in PROVISIONAL_FILES if (ref_dir / n).exists()]
    missing = [n for n in PROVISIONAL_FILES if not (ref_dir / n).exists()]
    if missing:
        say(f"provisional rows missing from {ref_dir}, left out of the fit: {', '.join(missing)}")
    return found


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--context", required=True)
    p.add_argument("--kubeconfig", default="",
                   help="the owner's kubeconfig, for kubectl and the driver on a laptop and the printed commands")
    p.add_argument("--driver", required=True, help="egress-tests binary")
    p.add_argument("--out", required=True, help="directory for the run JSONs")
    p.add_argument("--plan", choices=sorted(PLANS), default="tunnel-cap", help="which campaign to run")
    p.add_argument("--ref-dir", default=REF_DIR, help="directory holding the base check's and provisional rows' JSONs")
    p.add_argument("--base-check-ref",
                   help=f"the B=100 C=10 run the base check compares with (default: REF_DIR/{BASE_CHECK_FILE})")
    p.add_argument("--scripts-dir", default=SCRIPTS_PLACEHOLDER,
                   help="the owner's directory of e0-apply.sh and e0-revert.sh, for the printed E0 commands")
    p.add_argument("--no-provisional", action="store_true", help="fit without the provisional rows")
    p.add_argument("--in-cluster", action="store_true", help="run inside the egress-campaign Pod")
    p.add_argument("--dry-run", action="store_true", help="print every step and command; run nothing")
    args = p.parse_args(argv)
    out = Path(args.out)
    ref_dir = Path(args.ref_dir)
    ref = Path(args.base_check_ref) if args.base_check_ref else ref_dir / BASE_CHECK_FILE
    steps = PLANS[args.plan](args.context, args.kubeconfig, args.scripts_dir)
    if not args.dry_run and any(s.base_check for s in steps) and not ref.exists():
        print(f"refused: --base-check-ref {ref} does not exist", file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)
    kube = kubectl(args.context, args.in_cluster, args.kubeconfig)
    r = Runner(args.context, args.driver, out, dict(os.environ), dry_run=args.dry_run, execute=run_driver,
               read_active=lambda: kubectl_active(kube), read_gateway=lambda: gateway(kube),
               base_ref=ref, provisional=provisional_files(ref_dir, not args.no_provisional),
               in_cluster=args.in_cluster, kubeconfig=args.kubeconfig)
    try:
        r.walk(steps)
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
