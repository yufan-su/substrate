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

import contextlib
import functools
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import campaign as cp

CTX = "gke_test"
HAS_NUMPY = importlib.util.find_spec("numpy") is not None


def numpy_required() -> bool:
    """Whether a missing numpy fails the fit tests rather than skipping
    them. A skip and a pass share an exit code, so CI must fail."""
    return os.environ.get("CI") == "true" or os.environ.get("REQUIRE_NUMPY") == "true"


def good_report(b: int, measured: bool = True) -> str:
    """A report of a run that went as planned, as check_report reads it."""
    source = "own" if measured else "tag"
    return json.dumps({"start": {"succeeded": b}, "stop": {"failed": 0}, "suspend": {"failed": 0},
                       "actors": [{"resumeSource": source} for _ in range(b)]})


def step_named(name: str) -> cp.Step:
    return next(s for s in cp.plan(CTX) if s.name == name)


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
        for name, env, interval in [
            ("default", {}, "1s"),
            ("overridden polls", {"PROGRESS_INTERVAL": "2s"}, "2s"),
        ]:
            with self.subTest(name):
                args = cp.driver_args("egress-tests", CTX, step, Path("/o"), env)
                self.assertEqual(args[args.index("--progress-interval") + 1], interval)
                self.assertIn("--pre-idle", args)
                self.assertIn("--wait-for-cleartext-idle", args)
                self.assertIn("--usage", args)
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


class LadderPlanTest(unittest.TestCase):
    def test_rungs(self):
        steps = cp.ladder_plan(CTX)
        runs = [s for s in steps if s.kind == "run"]
        self.assertEqual([(s.name, s.B, s.C, s.duration, s.progress_interval, s.measured) for s in runs], [
            ("warmup", 1000, 10, "2m", "15s", False),
            # First round 0.13 C + 0.0017 B C, plus 3 min, rounded up to a minute.
            ("L1-c1000-b0100", 100, 1000, "8m", "", True),
            ("L2-c1000-b0300", 300, 1000, "14m", "15s", True),
            ("L3-c0300-b1000", 1000, 300, "13m", "15s", True),
            ("L4-c1000-b1000", 1000, 1000, "34m", "15s", True),
        ])
        owners = [s for s in steps if s.kind == "owner"]
        self.assertEqual([s.name for s in owners], ["start", "before-rung2", "before-rung3", "before-rung4"])
        self.assertTrue(all(s.restarts for s in owners))
        self.assertEqual(["80 x 7Gi" in s.note for s in owners], [False, False, False, True])
        self.assertFalse(any(s.base_check or s.holdout or s.fit for s in steps))

    def test_a_step_interval_overrides_the_env(self):
        step = cp.run("L2-c1000-b0300", 300, 1000, progress_interval="15s")
        args = cp.driver_args("egress-tests", CTX, step, Path("/o"), {"PROGRESS_INTERVAL": "5s"})
        self.assertEqual(args[args.index("--progress-interval") + 1], "15s")
        self.assertEqual(args[args.index("--duration") + 1], "3m")

    def test_main_needs_ref_dir_for_the_tunnel_cap_plan_only(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(cp.Runner, "walk") as walk, \
                contextlib.redirect_stderr(io.StringIO()) as err:
            argv = ["--context", CTX, "--driver", "egress-tests", "--out", d]
            self.assertEqual(cp.main(argv), 1)
            self.assertIn("needs --ref-dir", err.getvalue())
            walk.assert_not_called()
            self.assertEqual(cp.main(argv + ["--plan", "ladder"]), 0)
            walk.assert_called_once()
            self.assertIn("deploy.sh --deploy --endpoints 1000 --cgreader", walk.call_args.args[0][0].note)

    def test_main_runs_the_ladder_without_a_base_check_ref(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(cp.Runner, "walk") as walk:
            argv = ["--context", CTX, "--driver", "egress-tests", "--out", d, "--ref-dir", d + "/none"]
            self.assertEqual(cp.main(argv + ["--plan", "ladder"]), 0)
            self.assertEqual([s.name for s in walk.call_args.args[0]], [s.name for s in cp.ladder_plan(CTX)])
            walk.reset_mock()
            self.assertEqual(cp.main(argv), 1, "the tunnel-cap plan needs its base check ref")
            walk.assert_not_called()


class KubectlTest(unittest.TestCase):
    def test_argv(self):
        t = "--request-timeout=30s"
        for name, in_cluster, kubeconfig, want in [
                ("laptop", False, "", ["kubectl", t, "--context", CTX]),
                ("laptop with a kubeconfig", False, "/k/c", ["kubectl", t, "--kubeconfig", "/k/c", "--context", CTX]),
                ("in cluster", True, "/k/c", ["kubectl", t])]:
            with self.subTest(name):
                kube = cp.kubectl(CTX, in_cluster, kubeconfig)
                self.assertEqual(kube, want)
                with mock.patch.object(cp.subprocess, "run",
                                       return_value=subprocess.CompletedProcess([], 0, "gw-1 node-a\n")) as run:
                    self.assertEqual(cp.gateway(kube), ("gw-1", "node-a"))
                    self.assertEqual(cp.gateway_pod(kube), "gw-1")
                self.assertEqual(run.call_args.args[0][:len(want) + 1], want + ["-n"])

    def test_kubectl_calls_have_a_timeout(self):
        with mock.patch.object(cp.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "gw-1 node-a\n")) as run:
            cp.gateway(["kubectl"])
        self.assertEqual(run.call_args.kwargs["timeout"], cp.KUBECTL_TIMEOUT_S)

    def test_retry(self):
        sleeps = []
        calls = []

        def flaky(failures):
            def call():
                calls.append(1)
                if len(calls) <= failures:
                    raise subprocess.CalledProcessError(1, ["kubectl"], stderr="Unable to connect to the server")
                return "ok"
            return call
        self.assertEqual(cp.retry(flaky(2), "x", sleep=sleeps.append), "ok")
        self.assertEqual(sleeps, [cp.KUBECTL_RETRY_PAUSE_S] * 2)
        calls.clear()
        with self.assertRaisesRegex(cp.Refused, "reading x failed 3 times, last: Unable to connect"):
            cp.retry(flaky(3), "x", sleep=sleeps.append)
        with self.assertRaisesRegex(cp.Refused, "failed 3 times"):
            cp.retry(mock.Mock(side_effect=subprocess.TimeoutExpired(["kubectl"], 60)), "x", sleep=sleeps.append)

    def test_no_running_gateway_pod_is_refused(self):
        with mock.patch.object(cp.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "\n")):
            with self.assertRaisesRegex(cp.Refused, "no running gateway pod"):
                cp.gateway(["kubectl"])

    def test_cleartext_pool_is_read_from_the_metrics_listener(self):
        body = ('# TYPE envoy_cluster_upstream_cx_active gauge\n'
                'envoy_cluster_upstream_cx_active{envoy_cluster_name="egress_forward_proxy_cleartext"} 42\n')
        answers = iter([subprocess.CompletedProcess([], 0, "gw-1 node-a\n"), subprocess.CompletedProcess([], 0, body)])
        with mock.patch.object(cp.subprocess, "run", side_effect=lambda *a, **k: next(answers)) as run:
            self.assertEqual(cp.kubectl_active(["kubectl"]), 42.0)
        path = run.call_args.args[0][-1]
        self.assertTrue(path.startswith("/api/v1/namespaces/ate-system/pods/gw-1:15090/proxy/stats/prometheus?filter="), path)
        self.assertEqual(path.split("filter=")[1], "%5Ecluster%5C.egress_forward_proxy_cleartext%5C.upstream_cx_active%24")
        # A body without the stat is an error, not an empty pool.
        answers = iter([subprocess.CompletedProcess([], 0, "gw-1 node-a\n"),
                        subprocess.CompletedProcess([], 0, "envoy_cluster_upstream_cx_total{} 1\n")])
        with mock.patch.object(cp.subprocess, "run", side_effect=lambda *a, **k: next(answers)):
            with self.assertRaisesRegex(RuntimeError, "missing from gw-1"):
                cp.kubectl_active(["kubectl"])


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


class DriverTimeoutTest(unittest.TestCase):
    def test_duration(self):
        for text, want in [("3m", 180), ("45s", 45), ("2m30s", 150), ("100ms", 0.1), ("1h", 3600)]:
            with self.subTest(text):
                self.assertAlmostEqual(cp.duration_s(text), want)

    def test_driver_timeout_is_the_duration_plus_the_margin(self):
        args = cp.driver_args("egress-tests", CTX, cp.run("A-c010-b010", 10, 10, duration="3m"), Path("/o"), {})
        self.assertEqual(cp.driver_timeout_s(args), 180 + cp.DRIVER_MARGIN_S)

    def test_a_stuck_driver_is_killed_and_refused(self):
        args = cp.driver_args("egress-tests", CTX, cp.run("A-c010-b010", 10, 10), Path("/o"), {})
        with mock.patch.object(cp.subprocess, "run", side_effect=subprocess.TimeoutExpired(args, 1)) as run:
            with self.assertRaisesRegex(cp.Refused, "did not finish within 33 min and was killed"):
                cp.run_driver(args, None)
        self.assertEqual(run.call_args.kwargs["timeout"], 180 + cp.DRIVER_MARGIN_S)
        for rc, want_refused in [(0, False), (1, False), (2, True), (-9, True)]:
            with self.subTest(rc=rc):
                with mock.patch.object(cp.subprocess, "run", return_value=subprocess.CompletedProcess(args, rc)):
                    if want_refused:
                        with self.assertRaisesRegex(cp.Refused, f"driver exited {rc}"):
                            cp.run_driver(args, None)
                    else:
                        cp.run_driver(args, None)


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

    def done(self, name: str) -> None:
        """Writes the report of a run that went as planned."""
        step = step_named(name)
        (self.out / f"{name}.json").write_text(good_report(step.B, step.measured))

    def runner(self, write_json=True, tty=True, **kw):
        def execute(args, log):
            self.ran.append(args)
            if write_json:
                b = int(args[args.index("--parallel") + 1])
                Path(args[args.index("--output") + 1]).write_text(good_report(b, "--pre-idle" in args))

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

    def test_a_failing_predictor_page_does_not_stop_the_campaign(self):
        def render(rows, inputs, out):
            raise ModuleNotFoundError("No module named 'predictor'")
        r = self.runner()
        r.render = render
        r.walk(cp.plan(CTX))
        self.assertEqual(len(self.ran), 2 + 1 + 14 + 3 + 4 + 1 + 2)
        self.assertTrue((self.out / "predictions.json").exists())
        self.assertIn("      predictor page not written: ModuleNotFoundError: No module named 'predictor'", self.said)

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
            self.done(n)
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
        r.execute = lambda args, log: (Path(args[args.index("--output") + 1]).write_text(good_report(10)),
                                       self.pods.__setitem__(0, "gw-2"))
        with self.assertRaisesRegex(cp.Refused, "gateway pod changed from gw-1 to gw-2.*not starting A-c010-b025"):
            r.walk(steps)

    def test_a_resume_refuses_a_gateway_restart_inside_a_block(self):
        steps = [cp.owner("start", "start", restarts=True), cp.run("A-c010-b010", 10, 10),
                 cp.run("A-c010-b025", 25, 10)]
        r = self.runner()
        self.done("A-c010-b010")
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
        self.done("A-c010-b010")
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
        steps = [cp.owner("start", "start", "cmd")]
        self.runner(tty=True).walk(steps)
        self.assertEqual(len(self.prompts), 1)
        self.assertTrue((self.out / "go" / "01-start").exists(), "Enter records the step as done")
        # A resume on a TTY passes the done step without prompting, and
        # without showing its commands again.
        self.said.clear()
        self.runner(tty=True).walk(steps)
        self.assertEqual(len(self.prompts), 1)
        self.assertNotIn("      $ cmd", self.said)

    def test_a_tty_resume_does_not_prompt_for_done_steps(self):
        self.runner(tty=True).walk(cp.plan(CTX))
        owners = len([s for s in cp.plan(CTX) if s.kind == "owner"])
        self.assertEqual(len(self.prompts), owners)
        self.runner(tty=True).walk(cp.plan(CTX))
        self.assertEqual(len(self.prompts), owners, "a finished campaign prompted again")

    def test_refuses_a_run_on_a_pool_that_does_not_drain(self):
        clock = [0.0]
        r = self.runner()
        r.read_active = lambda: 900
        r.now = lambda: clock[0]
        r.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
        with self.assertRaisesRegex(cp.Refused, "cleartext pool still holds 900 connections.*not starting A-c010-b010"):
            r.walk([cp.run("A-c010-b010", 10, 10)])
        self.assertEqual(self.ran, [])
        self.assertFalse(json.loads((self.out / "A-c010-b010.gate.json").read_text())["ok"])

    def test_refuses_a_run_whose_report_says_it_failed(self):
        good = json.loads(good_report(10))
        for name, report, want in [
            ("interrupted", good | {"interrupted": True}, "it was interrupted"),
            ("fewer loops started", good | {"start": {"succeeded": 9}}, "9 of 10 loops started"),
            ("suspends failed", good | {"suspend": {"failed": 2}}, "2 suspend calls failed"),
            ("stops failed", good | {"stop": {"failed": 1}}, "1 stop calls failed"),
            ("golden-snapshot resumes", good | {"actors": [{"resumeSource": "tag"}] * 10}, "resumed from tag"),
            ("truncated", None, "unreadable report"),
        ]:
            with self.subTest(name):
                self.setUp()
                r = self.runner()
                r.execute = lambda args, log, rep=report: Path(args[args.index("--output") + 1]).write_text(
                    json.dumps(rep) if rep is not None else '{"start": {"succ')
                with self.assertRaisesRegex(cp.Refused, f"{want}.*[Dd]elete A-c010-b010.json"):
                    r.walk([cp.run("A-c010-b010", 10, 10)])
                # The same report is refused on resume, not skipped as done.
                with self.assertRaisesRegex(cp.Refused, want):
                    self.runner().walk([cp.run("A-c010-b010", 10, 10)])
        # The warm-up resumes from the golden snapshot by design.
        self.setUp()
        r = self.runner()
        r.execute = lambda args, log: Path(args[args.index("--output") + 1]).write_text(good_report(100, measured=False))
        r.walk([cp.run("warmup-1s", 100, 10, duration="45s", measured=False)])
        self.assertIn("[1] RUN warmup-1s (B=100 C=10 T=1000)", self.said)

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


class FitStopTest(unittest.TestCase):
    """The fit stop with plot/fit.py's real fit over synthetic rows."""

    def setUp(self):
        if not HAS_NUMPY:
            if numpy_required():
                self.fail("numpy is required here (plot/requirements.txt)")
            self.skipTest("the fit needs numpy (plot/requirements.txt)")
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
            out.write_text(good_report(int(args[args.index("--parallel") + 1]), measured="--pre-idle" in args))
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


class MatrixPlanTest(unittest.TestCase):
    def test_cells_for_each_scheme(self):
        steps = cp.matrix_plan(CTX)
        runs = [s for s in steps if s.kind == "run"]
        self.assertEqual([(s.name, s.B, s.C, s.scheme, s.duration, s.progress_interval) for s in runs], [
            ("M-http-c1000-b0250", 250, 1000, "http", "13m", ""),
            ("M-https-c1000-b0250", 250, 1000, "https", "13m", ""),
            ("M-http-c1000-b0750", 750, 1000, "http", "27m", "15s"),
            ("M-https-c1000-b0750", 750, 1000, "https", "27m", "15s"),
            ("M-http-c1000-b1000", 1000, 1000, "http", "34m", "15s"),
            ("M-https-c1000-b1000", 1000, 1000, "https", "34m", "15s"),
        ])
        owners = [s for s in steps if s.kind == "owner"]
        self.assertEqual([s.name for s in owners], ["start"] + [f"before-{r.name}" for r in runs[1:]])
        self.assertTrue(all(s.restarts for s in owners))
        self.assertIn("deploy.sh --deploy --https --endpoints 1000 --cgreader", owners[0].note)
        self.assertEqual(steps[-1], runs[-1])
        self.assertTrue(all(s.measured for s in runs))
        self.assertFalse(any(s.base_check or s.holdout or s.fit for s in steps))

    def test_https_runs_name_every_endpoint_in_the_policy(self):
        step = cp.run("M-https-c1000-b0250", 250, 1000, scheme="https")
        args = cp.driver_args("egress-tests", CTX, step, Path("/o"), {})
        self.assertEqual(args[args.index("--scheme") + 1], "https")
        self.assertEqual(args[args.index("--policy-hosts") + 1], "exact")
        http = cp.driver_args("egress-tests", CTX, cp.run("M-http-c1000-b0250", 250, 1000, scheme="http"), Path("/o"), {})
        self.assertEqual(http[http.index("--scheme") + 1], "http")
        self.assertNotIn("--policy-hosts", http)
        self.assertNotIn("--scheme", cp.driver_args("egress-tests", CTX, cp.run("L1", 100, 1000), Path("/o"), {}))

    def test_more_actors_than_the_default_when_b_asks(self):
        args = cp.driver_args("egress-tests", CTX, cp.run("M-http-c0010-b1500", 1500, 10, scheme="http"), Path("/o"), {})
        self.assertEqual(args[args.index("--actors") + 1], "1500")
        self.assertEqual(args[args.index("--parallel") + 1], "1500")

    def test_parse_matrix_and_schemes(self):
        self.assertEqual(cp.parse_matrix("250x1000, 10X10"), [(250, 1000), (10, 10)])
        for bad in ("250", "0x10", "ax10", "10x", ""):
            with self.assertRaises(ValueError):
                cp.parse_matrix(bad)
        self.assertEqual(cp.parse_schemes("HTTPS,http"), ["https", "http"])
        with self.assertRaises(ValueError):
            cp.parse_schemes("ftp")

    def test_main_builds_the_plan_from_the_flags(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(cp.Runner, "walk") as walk, \
                contextlib.redirect_stderr(io.StringIO()) as err:
            argv = ["--context", CTX, "--driver", "egress-tests", "--out", d, "--plan", "matrix"]
            self.assertEqual(cp.main(argv + ["--matrix", "10x10,20x10", "--schemes", "http"]), 0)
            names = [s.name for s in walk.call_args.args[0]]
            self.assertEqual(names, ["start", "M-http-c0010-b0010", "before-M-http-c0010-b0020", "M-http-c0010-b0020"])
            self.assertEqual(cp.main(argv + ["--matrix", "nonsense"]), 1)
            self.assertIn("refused: matrix cell", err.getvalue())


class TableTest(unittest.TestCase):
    """Every plan's walk ends with the table over the reports it has."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name)
        self.said, self.tabulated = [], []
        self.steps = [cp.run("M-http-c0010-b0010", 10, 10, scheme="http"),
                      cp.run("M-https-c0010-b0010", 10, 10, scheme="https")]

    def tearDown(self):
        self.tmp.cleanup()

    def tabulate(self, reports, out, say, fail=False):
        self.tabulated.append(reports)
        if fail:
            raise OSError("disk full")
        say("      inside")
        return [out / "MATRIX.md", out / "matrix.json"]

    def runner(self, **kw):
        kw.setdefault("tabulate", self.tabulate)
        return cp.Runner(CTX, "egress-tests", self.out, {}, say=self.said.append, prompt=lambda _: "", isatty=lambda: True, **kw)

    def test_tabulates_the_reports_that_exist(self):
        (self.out / "M-http-c0010-b0010.json").write_text("{}")
        self.runner().table(self.steps)
        self.assertEqual(self.tabulated, [[self.out / "M-http-c0010-b0010.json"]])
        self.assertEqual(self.said, ["TABLE MATRIX.md: 1 of 2 runs; missing M-https-c0010-b0010.json", "      inside",
                                     f"      wrote {self.out / 'MATRIX.md'}", f"      wrote {self.out / 'matrix.json'}"])

    def test_nothing_to_tabulate_is_said_not_raised(self):
        self.runner().table(self.steps)
        self.assertEqual(self.tabulated, [])
        self.assertEqual(self.said, ["TABLE MATRIX.md: 0 of 2 runs; missing M-http-c0010-b0010.json, M-https-c0010-b0010.json"])

    def test_dry_run_writes_nothing(self):
        r = self.runner(dry_run=True)
        r.walk(self.steps)
        self.assertEqual(self.tabulated, [])
        self.assertEqual(self.said[-1], "TABLE MATRIX.md: 2 runs (dry run: not written)")

    def test_a_table_error_is_the_walks_error_only_when_the_walk_passed(self):
        (self.out / "M-http-c0010-b0010.json").write_text("{}")
        r = self.runner(tabulate=functools.partial(self.tabulate, fail=True))
        with self.assertRaises(OSError):
            r.table(self.steps)
        r.table(self.steps, failing=True)
        self.assertEqual(self.said[-1], "      MATRIX.md not written: OSError: disk full")

    def test_walk_tabulates_after_a_failed_run_and_keeps_the_runs_error(self):
        def execute(args, log):
            if args[args.index("--scheme") + 1] == "https":
                raise cp.Refused("the https run broke")
            Path(args[args.index("--output") + 1]).write_text("{}")
        r = self.runner(execute=execute, read_active=lambda: 0, read_gateway=lambda: ("gw-1", "n1"), sleep=lambda s: None)
        with mock.patch.object(cp, "check_report"):
            with self.assertRaisesRegex(cp.Refused, "the https run broke"):
                r.walk(self.steps)
        self.assertEqual(self.tabulated, [[self.out / "M-http-c0010-b0010.json"]])

    def test_render_matrix_leaves_out_a_report_it_cannot_read(self):
        (self.out / "A-c010-b010.json").write_text("{}")
        self.assertEqual(cp.render_matrix([self.out / "A-c010-b010.json"], self.out, self.said.append), [])
        self.assertFalse((self.out / "MATRIX.md").exists())
        self.assertEqual(len(self.said), 1)
        self.assertIn("A-c010-b010.json left out of MATRIX.md: KeyError", self.said[0])
