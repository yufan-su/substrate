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

"""Tests of campaign.py. Run from this directory: python3 -m unittest test_campaign"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import campaign as cp

CTX = "gke_test"
HAS_NUMPY = importlib.util.find_spec("numpy") is not None


class PlanTest(unittest.TestCase):
    def test_order_and_counts(self):
        steps = cp.plan(CTX)
        runs = [s for s in steps if s.kind == "run"]
        names = [s.name for s in runs]
        self.assertEqual(names[:3], ["warmup-1s", "warmup-5s", "A-c010-b100"])
        self.assertEqual([s.name for s in runs if s.base_check], ["A-c010-b100"])
        block_a = [s for s in runs if s.name.startswith("A-")][1:]
        self.assertEqual([(s.C, s.B) for s in block_a],
                         [(c, b) for c in (256, 128, 50, 10) for b in (10, 25, 50, 100)
                          if b * c <= 16384 and (b, c) != (100, 10)],
                         "Block A is not C=256, 128, 50, 10 with B ascending")
        self.assertEqual(len(set(names)), len(names))
        measured = [s for s in runs if s.measured and not s.name.startswith("E0")]
        self.assertEqual(len(measured), 1 + 14 + 3 + 4 + 2)

        self.assertEqual([s.name for s in runs if s.holdout], ["D-c075-b020", "D-c200-b040", "D-c160-b080", "D-c020-b075"])
        self.assertEqual(names[-2:], ["E-c256-b064", "E-c160-b100"])
        e0 = names.index("E0-c100-b011")
        self.assertLess(names.index("D-c020-b075"), e0)
        self.assertLess(e0, names.index("E-c256-b064"))
        owners = {s.name: s for s in steps if s.kind == "owner"}
        self.assertEqual({n for n, s in owners.items() if not s.restarts}, {"poll-check", "fit"})

    def test_e0_commands_name_the_scripts_dir(self):
        for scripts, want in [(cp.SCRIPTS_PLACEHOLDER, "bash <scripts-dir>/e0-apply.sh"),
                              ("~/s", "bash ~/s/e0-apply.sh")]:
            with self.subTest(scripts=scripts):
                e0 = next(s for s in cp.plan(CTX, scripts=scripts) if s.name == "e0-apply")
                self.assertEqual(e0.commands, [want])

    def test_driver_args(self):
        step = cp.run("A-c010-b010", 10, 10)
        for name, env, want_preidle, interval in [
            ("default", {}, True, "1s"),
            ("explicitly on", {"DRIVER_HAS_PREIDLE": "1"}, True, "1s"),
            ("driver without --pre-idle", {"DRIVER_HAS_PREIDLE": "0"}, False, "1s"),
            ("overridden polls", {"PROGRESS_INTERVAL": "2s"}, True, "2s"),
        ]:
            with self.subTest(name):
                args = cp.driver_args("egress-tests", CTX, step, Path("/o"), env)
                self.assertEqual(args[args.index("--progress-interval") + 1], interval)
                self.assertEqual("--pre-idle" in args, want_preidle)
                self.assertEqual("--wait-for-cleartext-idle" in args, want_preidle)
                self.assertEqual(args[args.index("--context") + 1], CTX)
                self.assertEqual(args[args.index("--actors") + 1], "1000")
                self.assertEqual(args[args.index("--pick") + 1], "first")
                self.assertEqual(args[args.index("--output") + 1], "/o/A-c010-b010.json")
        for name, env, want in [("warmup-1s", {}, "1s"), ("warmup-5s", {}, "5s"),
                                ("warmup-1s", {"PROGRESS_INTERVAL": "5s"}, "1s"),
                                ("warmup-5s", {"PROGRESS_INTERVAL": "2s"}, "5s")]:
            with self.subTest(f"{name} with {env}"):
                warm = cp.driver_args("egress-tests", CTX, cp.run(name, 100, 10, measured=False), Path("/o"), env)
                self.assertEqual(warm[warm.index("--progress-interval") + 1], want)
                self.assertNotIn("--pre-idle", warm)

    def test_driver_args_with_a_kubeconfig(self):
        args = cp.driver_args("egress-tests", CTX, cp.run("A-c010-b010", 10, 10), Path("/o"), {}, kubeconfig="/k/c")
        self.assertEqual(args[2:6], ["--kubeconfig", "/k/c", "--context", CTX])

    def test_owner_commands_carry_the_kubeconfig(self):
        for kubeconfig, want in [("", f"kubectl --context {CTX} -n ate-system rollout restart"),
                                 ("/k/c 2", f"kubectl --kubeconfig '/k/c 2' --context {CTX} -n ate-system rollout restart")]:
            with self.subTest(kubeconfig=kubeconfig):
                start = cp.plan(CTX, kubeconfig)[0]
                self.assertTrue(start.commands[0].startswith(want), start.commands[0])

    def test_driver_args_in_cluster(self):
        args = cp.driver_args("egress-tests", CTX, cp.run("A-c010-b010", 10, 10), Path("/out"), {}, in_cluster=True)
        self.assertNotIn("--context", args)
        for flag, want in [("--api-endpoint", "api.ate-system.svc:443"),
                           ("--api-token-file", "/var/run/ateapi/token"),
                           ("--router-url", "http://atenet-router.ate-system.svc"),
                           ("--output", "/out/A-c010-b010.json")]:
            with self.subTest(flag):
                self.assertEqual(args[args.index(flag) + 1], want)


class KubectlTest(unittest.TestCase):
    def test_argv(self):
        for name, in_cluster, kubeconfig, want in [
                ("laptop", False, "", ["kubectl", "--context", CTX]),
                ("laptop with a kubeconfig", False, "/k/c", ["kubectl", "--kubeconfig", "/k/c", "--context", CTX]),
                ("in cluster", True, "/k/c", ["kubectl"])]:
            with self.subTest(name):
                kube = cp.kubectl(CTX, in_cluster, kubeconfig)
                self.assertEqual(kube, want)
                with mock.patch.object(cp.subprocess, "run",
                                       return_value=subprocess.CompletedProcess([], 0, "gw-1 node-a\n")) as run:
                    self.assertEqual(cp.gateway(kube), ("gw-1", "node-a"))
                    self.assertEqual(cp.gateway_pod(kube), "gw-1")
                self.assertEqual(run.call_args.args[0][:len(want) + 1], want + ["-n"])


class GateTest(unittest.TestCase):
    def gate(self, values, cap_s=60, poll_s=10):
        clock = [0.0]
        it = iter(values)

        def sleep(s):
            clock[0] += s
        return cp.wait_for_idle(lambda: next(it), sleep, lambda: clock[0], cap_s, poll_s)

    def test_cases(self):
        for name, values, want in [
            ("already idle", [0], cp.Gate(0, 0, True, 1)),
            ("drains", [900, 300, 0], cp.Gate(20, 0, True, 3)),
            ("never drains", [5] * 10, cp.Gate(60, 5, False, 7)),
        ]:
            with self.subTest(name):
                self.assertEqual(self.gate(values), want)


class BaseCheckRuleTest(unittest.TestCase):
    def test_rule(self):
        for name, value, ref, want in [
            ("equal", 0.50, 0.50, True),
            ("14% up", 0.57, 0.50, True),
            ("16% up", 0.58, 0.50, False),
            ("16% down", 0.42, 0.50, False),
            ("small, 0.02 core off", 0.12, 0.10, True),
            ("small, past 0.02 core", 0.125, 0.10, False),
        ]:
            with self.subTest(name):
                self.assertEqual(cp.within_cpu_rule(value, ref), want)


class ProvisionalFilesTest(unittest.TestCase):
    def test_cases(self):
        for name, present, enabled, want in [
            ("all four", cp.PROVISIONAL_FILES, True, list(cp.PROVISIONAL_FILES)),
            ("two missing", cp.PROVISIONAL_FILES[:2], True, list(cp.PROVISIONAL_FILES[:2])),
            ("none", (), True, []),
            ("--no-provisional", cp.PROVISIONAL_FILES, False, []),
        ]:
            with self.subTest(name), tempfile.TemporaryDirectory() as d:
                ref = Path(d)
                for n in present:
                    (ref / n).write_text("{}")
                said = []
                got = cp.provisional_files(ref, enabled, said.append)
                self.assertEqual(got, [ref / n for n in want])
                missing = [n for n in cp.PROVISIONAL_FILES if n not in present]
                if enabled and missing:
                    self.assertIn(", ".join(missing), said[0])
                if not enabled or not missing:
                    self.assertEqual(len(said), 0 if enabled else 1)


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        self.said, self.ran, self.prompts, self.sleeps = [], [], [], []
        self.pods = ["gw-1"]
        self.gateway_node = "node-gw"
        # gateway cores the stub extract reports, by file name
        self.cores = {"A-c010-b100.json": 0.50, "t3.json": 0.52}
        self.fit_rows = []
        self.rendered = []

    def render(self, rows, inputs, out):
        self.rendered.append([p.name for p in inputs])
        return [out / "fit.json", out / "predictor.html"]

    def predict(self, rows, holdouts, say):
        self.fit_rows.append(rows)
        return {h.name: {"B": h.B, "C": h.C, "responses": {"gateway_cores": {"predicted": 1.0, "halfWidth": 0.1}}}
                for h in holdouts}

    def runner(self, write_json=True, tty=True, **kw):
        def execute(args, log):
            self.ran.append(args)
            if write_json:
                Path(args[args.index("--output") + 1]).write_text("{}")

        def sleep(s):
            self.sleeps.append(s)

        def extract(path):
            return {"gateway_cores": self.cores.get(Path(path).name)}
        kw.setdefault("provisional", [Path("/ref/t3.json"), Path("/ref/t1.json")])
        return cp.Runner(CTX, "egress-tests", self.out, {}, execute=execute, read_active=lambda: 0,
                         read_gateway=lambda: (self.pods[0], self.gateway_node), base_ref=Path("/ref/t3.json"), extract=extract,
                         predict=self.predict, render=self.render, prompt=self.prompts.append, isatty=lambda: tty, say=self.said.append,
                         sleep=sleep, **kw)

    def predict_holdouts(self):
        names = [s.name for s in cp.plan(CTX) if s.kind == "run" and s.holdout]
        (self.out / "predictions.json").write_text(json.dumps({n: {} for n in names}))

    def test_walk_runs_every_step_and_the_fit_writes_predictions(self):
        self.runner().walk(cp.plan(CTX))
        self.assertEqual(len(self.ran), 2 + 1 + 14 + 3 + 4 + 1 + 2)
        gate = json.loads((self.out / "A-c256-b010.gate.json").read_text())
        self.assertEqual((gate["ok"], gate["gatewayPod"]), (True, "gw-1"))
        self.assertFalse((self.out / "warmup-1s.gate.json").exists(), "the warm-up is gated")
        # The fit read the 14 + 1 Block A and 3 Block C JSONs, not the warm-up or the .gate.json files.
        self.assertEqual(len(self.fit_rows), 1)
        self.assertEqual(len([r for r in self.fit_rows[0] if not r.get("provisional")]), 15 + 3)
        preds = json.loads((self.out / "predictions.json").read_text())
        self.assertEqual(sorted(preds), sorted(s.name for s in cp.plan(CTX) if s.holdout))
        self.assertIn("      predict D-c075-b020 B=20 C=75: gateway_cores 1 ± 0.1", self.said)
        # The page hashes the fitting runs and the provisional rows, not the .gate.json files.
        self.assertEqual(len(self.rendered), 1)
        self.assertEqual(len(self.rendered[0]), 15 + 3 + 2)
        self.assertFalse([n for n in self.rendered[0] if n.endswith(".gate.json")])
        self.assertIn(f"      wrote {self.out / 'predictor.html'}", self.said)

    def test_an_existing_prediction_file_is_kept_and_refuses_holdouts_missing_from_it(self):
        (self.out / "predictions.json").write_text("{}")
        with self.assertRaisesRegex(cp.Refused, "no prediction for D-c075-b020"):
            self.runner().walk(cp.plan(CTX))
        self.assertEqual(self.fit_rows, [])
        self.assertEqual(json.loads((self.out / "predictions.json").read_text()), {})
        self.assertEqual(len(self.ran), 2 + 1 + 14 + 3)

    def test_holdouts_run_with_predictions_and_resume_skips_done_runs(self):
        names = [s.name for s in cp.plan(CTX) if s.kind == "run"]
        first_d = names.index("D-c075-b020")
        for n in names[:first_d]:
            (self.out / f"{n}.json").write_text("{}")
        self.predict_holdouts()
        self.runner().walk(cp.plan(CTX))
        self.assertEqual([a[a.index("--output") + 1].split("/")[-1] for a in self.ran],
                         [f"{n}.json" for n in names[first_d:]])
        self.assertTrue((self.out / "base-check.json").exists(), "a skipped base check is still judged")

    def test_base_check_decides_the_provisional_rows(self):
        for name, cores, want_pass in [("within 15%", 0.52, True), ("off by 30%", 0.72, False)]:
            with self.subTest(name):
                self.setUp()
                self.cores["t3.json"] = cores
                self.runner().walk(cp.plan(CTX))
                verdict = json.loads((self.out / "base-check.json").read_text())
                self.assertEqual((verdict["pass"], verdict["value"], verdict["refValue"]), (want_pass, 0.50, cores))
                fit_line = next(s for s in self.said if "fit.py" in s)
                self.assertEqual("--provisional" in fit_line, want_pass)
                self.assertNotIn(".gate.json", fit_line)
                self.assertIn("passed" if want_pass else "FAILED", "\n".join(self.said))
                provisional = [r for r in self.fit_rows[0] if r.get("provisional")]
                self.assertEqual(len(provisional), 2 if want_pass else 0)

    def test_a_passed_base_check_without_provisional_rows_fits_without_them(self):
        r = self.runner(provisional=[])
        r.walk(cp.plan(CTX))
        fit_line = next(s for s in self.said if "fit.py" in s)
        self.assertNotIn("--provisional", fit_line)
        self.assertIn("      base check passed, but there are no provisional rows: fitting without them", self.said)
        self.assertEqual([r for r in self.fit_rows[0] if r.get("provisional")], [])

    def test_refuses_a_gateway_restart_inside_a_block(self):
        steps = [cp.run("A-c010-b010", 10, 10), cp.run("A-c010-b025", 25, 10)]
        r = self.runner()
        r.execute = lambda args, log: (Path(args[args.index("--output") + 1]).write_text("{}"),
                                       self.pods.__setitem__(0, "gw-2"))
        with self.assertRaisesRegex(cp.Refused, "gateway pod changed from gw-1 to gw-2.*not starting A-c010-b025"):
            r.walk(steps)

    def test_a_resume_refuses_a_gateway_restart_inside_a_block(self):
        steps = [cp.owner("start", "start", restarts=True), cp.run("A-c010-b010", 10, 10),
                 cp.run("A-c010-b025", 25, 10)]
        r = self.runner()
        (self.out / "A-c010-b010.json").write_text("{}")
        record = {"block": "start", "pod": "gw-1", "since": "A-c010-b010"}
        (self.out / cp.BLOCK_POD_FILE).write_text(json.dumps(record))
        self.pods[0] = "gw-2"
        with self.assertRaisesRegex(cp.Refused, "gateway pod changed from gw-1 to gw-2 since this block's first run, "
                                                "A-c010-b010; not starting A-c010-b025.*delete .*block-gateway-pod"):
            r.walk(steps)
        # Deleting the record accepts the new pod for the rest of the block.
        (self.out / cp.BLOCK_POD_FILE).unlink()
        self.runner().walk(steps)
        self.assertEqual(json.loads((self.out / cp.BLOCK_POD_FILE).read_text()),
                         {"block": "start", "pod": "gw-2", "since": "A-c010-b025"})

    def test_a_resume_after_an_owner_restart_starts_a_new_block(self):
        steps = [cp.owner("start", "start", restarts=True), cp.run("A-c010-b010", 10, 10),
                 cp.owner("before-C", "restart", restarts=True), cp.run("C-c010-b010", 10, 10)]
        (self.out / "A-c010-b010.json").write_text("{}")
        (self.out / cp.BLOCK_POD_FILE).write_text(json.dumps({"block": "start", "pod": "gw-1"}))
        self.pods[0] = "gw-2"
        self.runner().walk(steps)
        self.assertEqual(len(self.ran), 1)
        self.assertEqual(json.loads((self.out / cp.BLOCK_POD_FILE).read_text())["block"], "before-C")

    def test_an_owner_restart_starts_a_new_block(self):
        steps = [cp.run("A-c010-b010", 10, 10), cp.owner("restart", "restart", restarts=True),
                 cp.run("A-c010-b025", 25, 10)]
        r = self.runner()
        r.prompt = lambda _: self.pods.__setitem__(0, "gw-2")
        r.walk(steps)
        self.assertEqual(len(self.ran), 2)
        self.assertEqual(json.loads((self.out / "A-c010-b025.gate.json").read_text())["gatewayPod"], "gw-2")

    def test_an_owner_step_without_a_restart_keeps_the_block(self):
        steps = [cp.run("A-c010-b010", 10, 10), cp.owner("look", "look"), cp.run("A-c010-b025", 25, 10)]
        r = self.runner()
        r.prompt = lambda _: self.pods.__setitem__(0, "gw-2")
        with self.assertRaisesRegex(cp.Refused, "gateway pod changed"):
            r.walk(steps)

    def test_owner_waits_for_enter_on_a_tty(self):
        self.runner(tty=True).walk([cp.owner("start", "start", "cmd")])
        self.assertEqual(len(self.prompts), 1)
        self.assertFalse((self.out / "go").exists())

    def test_owner_waits_for_the_go_file_without_a_tty(self):
        go = self.out.resolve() / "go" / "01-start"
        r = self.runner(tty=False)
        polls = []

        def sleep(s):
            polls.append(s)
            if len(polls) == 2:
                go.touch()
        r.sleep = sleep
        r.walk([cp.owner("start", "start", "cmd")])
        self.assertEqual(self.prompts, [])
        self.assertEqual(polls, [cp.GO_POLL_S, cp.GO_POLL_S])
        self.assertIn(f"      ! touch {go}", self.said)

    def test_in_cluster_go_line_and_gate_node(self):
        env = {"POD_NAME": "egress-campaign", "POD_NAMESPACE": "egress-tests", "NODE_NAME": "node-a"}
        go = self.out.resolve() / "go" / "01-start"
        r = self.runner(tty=False, in_cluster=True, kubeconfig="/k/c")
        r.env = env
        r.sleep = lambda s: go.touch()
        r.walk([cp.owner("start", "start", "cmd"), cp.run("A-c010-b010", 10, 10)])
        self.assertIn(f"      ! kubectl --kubeconfig /k/c --context {CTX} -n egress-tests exec egress-campaign -- touch {go}",
                      self.said)
        self.assertNotIn(f"      ! touch {go}", self.said)
        self.assertEqual(self.said[0], "runner pod egress-tests/egress-campaign on node node-a")
        self.assertNotIn("--context", self.ran[0])
        gate = json.loads((self.out / "A-c010-b010.gate.json").read_text())
        self.assertEqual((gate["runnerNode"], gate["gatewayNode"]), ("node-a", "node-gw"))

    def test_in_cluster_refuses_a_gateway_on_the_runners_node(self):
        for name, in_cluster, want_refused in [("in cluster", True, True), ("laptop", False, False)]:
            with self.subTest(name):
                self.setUp()
                self.gateway_node = "node-a"
                r = self.runner(in_cluster=in_cluster)
                r.env = {"NODE_NAME": "node-a"}
                steps = [cp.run("A-c010-b010", 10, 10)]
                if want_refused:
                    with self.assertRaisesRegex(cp.Refused, "gateway pod gw-1 runs on the runner's node node-a; "
                                                            "not starting A-c010-b010"):
                        r.walk(steps)
                    self.assertEqual(self.ran, [])
                else:
                    r.walk(steps)
                    self.assertEqual(len(self.ran), 1)

    def test_an_existing_go_file_passes_at_once(self):
        go = self.out.resolve() / "go" / "01-start"
        go.parent.mkdir()
        go.touch()
        r = self.runner(tty=False)
        r.walk([cp.owner("start", "start", "cmd")])
        self.assertEqual(self.sleeps, [])

    def test_refuses_after_a_run_without_json(self):
        r = self.runner(write_json=False)
        with self.assertRaisesRegex(cp.Refused, "warmup-1s wrote no warmup-1s.json"):
            r.walk(cp.plan(CTX))
        self.assertEqual(len(self.ran), 1)

    def test_refuses_when_the_previous_json_is_missing(self):
        # The first run's JSON disappears during the owner step in between.
        steps = [cp.run("A-c010-b010", 10, 10), cp.owner("restart", "restart"), cp.run("A-c010-b025", 25, 10)]
        r = self.runner()
        r.prompt = lambda _: (self.out / "A-c010-b010.json").unlink()
        with self.assertRaisesRegex(cp.Refused, "A-c010-b010.json is missing; not starting A-c010-b025"):
            r.walk(steps)
        self.assertEqual(len(self.ran), 1)

    def test_dry_run_executes_nothing(self):
        r = self.runner()
        r.dry_run = True
        r.walk(cp.plan(CTX))
        self.assertEqual(self.ran, [])
        text = "\n".join(self.said)
        self.assertIn("OUT/predictions.json below; review both", text)
        self.assertIn("(dry run: not fitted)", text)
        self.assertIn("e0-apply.sh", text)
        self.assertIn("base check: gateway_cores vs /ref/t3.json", text)
        self.assertIn("! touch", text)
        self.assertEqual(text.count("] RUN "), 2 + 1 + 14 + 3 + 4 + 1 + 2)


@unittest.skipUnless(HAS_NUMPY, "the fit needs numpy (plot/requirements.txt)")
class FitStopTest(unittest.TestCase):
    """The fit stop with plot/fit.py's real fit over synthetic rows."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)

    @staticmethod
    def cores(b, c, i, provisional=False):
        # Linear in B and T with a small alternating residual; provisional
        # rows sit 0.05 core higher, so their presence moves the prediction.
        return 0.1 + 0.002 * b + 0.00001 * b * c + (0.001 if i % 2 else -0.001) + (0.05 if provisional else 0)

    def walk(self, base_pass):
        cells = {}
        steps = []
        for i, (b, c) in enumerate([(10, 10), (25, 10), (50, 10), (100, 10), (10, 50), (50, 50), (100, 50),
                                     (10, 256), (25, 256), (50, 128)]):
            name = f"A-c{c:03d}-b{b:03d}"
            cells[name] = self.cores(b, c, i)
            steps.append(cp.run(name, b, c, measured=False))
        refs = {"p1.json": self.cores(100, 10, 0, True), "p2.json": self.cores(10, 100, 1, True)}
        ref_dir = self.out / "ref"
        ref_dir.mkdir(exist_ok=True)
        for n in refs:
            (ref_dir / n).write_text("{}")
        if base_pass is not None:
            (self.out / "base-check.json").write_text(json.dumps({"pass": base_pass}))
        holdouts = [cp.run("D-c075-b020", 20, 75, holdout=True), cp.run("D-c160-b080", 80, 160, holdout=True)]
        steps += [cp.owner("fit", "fit", fit=True), *holdouts]

        def extract(path):
            name = Path(path).name
            if name in refs:
                b, c = (100, 10) if name == "p1.json" else (10, 100)
                return {"run": name, "scheme": "http", "B": b, "C": c, "T": b * c, "gateway_cores": refs[name]}
            step = next(s for s in steps if s.name + ".json" == name)
            return {"run": name, "scheme": "http", "B": step.B, "C": step.C, "T": step.B * step.C,
                    "gateway_cores": cells[step.name]}

        def execute(args, log):
            out = Path(args[args.index("--output") + 1])
            out.write_text("{}")
            out.with_suffix(".gate.json").write_text("{}")  # must not enter the fit
        said = []
        r = cp.Runner(CTX, "egress-tests", self.out, {}, execute=execute, read_active=lambda: 0,
                      read_gateway=lambda: ("gw", "node-gw"),
                      provisional=[ref_dir / "p1.json", ref_dir / "p2.json"],
                      extract=extract, prompt=lambda _: None, isatty=lambda: True, say=said.append)
        r.walk(steps)
        rows = [extract(Path(f"{n}.json")) for n in cells]
        return json.loads((self.out / "predictions.json").read_text()), rows, [extract(Path(n)) for n in refs], said

    def expected(self, rows):
        fit = cp.fit_module()
        f = fit.fit_all(rows)["gateway_cores"]
        return {n: f.predict({"B": b, "C": c, "T": b * c}) for n, b, c in [("D-c075-b020", 20, 75),
                                                                            ("D-c160-b080", 80, 160)]}

    def test_predictions(self):
        for name, base_pass, with_refs in [("passed base check", True, True),
                                           ("failed base check", False, False),
                                           ("no base check", None, False)]:
            with self.subTest(name):
                self.setUp()
                preds, rows, refs, said = self.walk(base_pass)
                want = self.expected(rows + ([r | {"provisional": True} for r in refs] if with_refs else []))
                self.assertEqual(sorted(preds), sorted(want))
                for h, (pred, half) in want.items():
                    got = preds[h]["responses"]["gateway_cores"]
                    self.assertAlmostEqual(got["predicted"], pred, places=12)
                    self.assertAlmostEqual(got["halfWidth"], half, places=12)
                    self.assertEqual(set(preds[h]["responses"]), {"gateway_cores"})
                b, c = preds["D-c075-b020"]["B"], preds["D-c075-b020"]["C"]
                truth = self.cores(b, c, 0) + 0.001
                tolerance = 0.02 if with_refs else 0.005
                self.assertAlmostEqual(preds["D-c075-b020"]["responses"]["gateway_cores"]["predicted"], truth,
                                       delta=tolerance)
                self.assertTrue(any(s.startswith("      gateway_cores: n=") for s in said), said)
                # fit.json holds the same fit, and hashes every row's file.
                doc = json.loads((self.out / "fit.json").read_text())
                fitted = rows + ([r | {"provisional": True} for r in refs] if with_refs else [])
                want_fit = cp.fit_module().fit_all(fitted)["gateway_cores"]
                coef = doc["fits"]["gateway_cores"]["coef"]
                self.assertEqual(len(coef), len(want_fit.coef))
                for got, want in zip(coef, want_fit.coef):
                    self.assertAlmostEqual(got, want, places=12)
                self.assertEqual(len(doc["provenance"]["inputs"]), len(fitted))
                self.assertIn('<script type="application/json" id="fit">', (self.out / "predictor.html").read_text())

    def test_a_rerun_keeps_the_predictions(self):
        preds, *_ = self.walk(True)
        (self.out / "predictions.json").write_text(json.dumps({"kept": True}))
        for n in ("D-c075-b020", "D-c160-b080"):
            (self.out / f"{n}.json").unlink()
        with self.assertRaisesRegex(cp.Refused, "no prediction for D-c075-b020"):
            self.walk(True)
        self.assertEqual(json.loads((self.out / "predictions.json").read_text()), {"kept": True})

    def test_refuses_when_nothing_fits(self):
        r = cp.Runner(CTX, "egress-tests", self.out, {}, say=lambda _: None, prompt=lambda _: None,
                      isatty=lambda: True, provisional=[])
        with self.assertRaisesRegex(cp.Refused, "no judged response could be fitted"):
            r.fit([cp.run("D-c075-b020", 20, 75, holdout=True)])
        self.assertFalse((self.out / "predictions.json").exists())


if __name__ == "__main__":
    unittest.main()
