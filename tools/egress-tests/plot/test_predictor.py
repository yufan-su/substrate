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

"""Tests of predictor.py.

Run from this directory: python3 -m unittest test_predictor
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import fit
import predictor
import test_fit


def document() -> dict:
    rows = test_fit.synthetic(test_fit.GRID + test_fit.REPLICATES, seed=1)
    for i, r in enumerate(rows):
        r |= {"run": f"A-{i}</script><b>.json", "scheme": "http", "interval_ms": 100.0, "R_measured": 9.7 * r["B"],
              "cleartext_active_max": 8 * r["C"], "envoy_heap_floor_mib": 11.0}
    return fit.fit_document(rows, fit.fit_all(rows), fit_at="2026-10-07T17:00:00Z")


def embedded(page: str) -> dict:
    m = re.search(r'<script type="application/json" id="fit">(.*?)</script>', page, re.S)
    return json.loads(m.group(1))


class RenderTest(unittest.TestCase):
    def test_embeds_the_document(self):
        doc = document()
        page = predictor.render(doc)
        self.assertEqual(embedded(page), doc)
        self.assertNotIn(predictor.PLACEHOLDER, page)
        # A run name cannot close the script element or open a tag.
        self.assertEqual(page.count("</script>"), 2)
        self.assertNotIn("<b>", page)

    def test_refuses(self):
        doc = document()
        for name, d, template, want in [
            ("another schema", doc | {"schema": fit.JSON_SCHEMA + 1}, None, "schema"),
            ("no fits", doc | {"fits": {}}, None, "no fits"),
            ("no placeholder", doc, "<html></html>", "exactly once"),
            ("two placeholders", doc, predictor.PLACEHOLDER * 2, "exactly once"),
        ]:
            with self.subTest(name), self.assertRaisesRegex(ValueError, want):
                predictor.render(d, template)

    def test_main(self):
        with tempfile.TemporaryDirectory() as d:
            src, out = Path(d) / "fit.json", Path(d) / "predictor.html"
            fit.write_json(document(), str(src))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(predictor.main([str(src), "-o", str(out)]), 0)
            self.assertEqual(embedded(out.read_text()), json.loads(src.read_text()))
            src.write_text("{}")
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertEqual(predictor.main([str(src), "-o", str(out)]), 1)
            self.assertIn("schema", err.getvalue())


# node, or the JavaScriptCore shell every macOS has.
JSC = "/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc"
JS = shutil.which("node") or (JSC if Path(JSC).exists() else None)


def js_required() -> bool:
    """Whether a missing JavaScript shell fails the page tests rather than
    skipping them. A skip and a pass share an exit code, so CI must fail."""
    return os.environ.get("CI") == "true" or os.environ.get("REQUIRE_JS") == "true"


class PageScriptTest(unittest.TestCase):
    """Runs the page's script, without its DOM, under a JavaScript shell."""

    def setUp(self):
        if not JS:
            if js_required():
                self.fail("node or jsc is required here")
            self.skipTest("needs node or jsc")

    def run_js(self, doc: dict, body: str):
        script = re.findall(r"<script>(.*?)</script>", predictor.render(doc), re.S)[0]
        self.assertTrue(script.rstrip().endswith("init();"))
        script = script.rstrip().removesuffix("init();")
        prelude = f"const document = {{getElementById: () => ({{textContent: {json.dumps(json.dumps(doc))}}})}};\n"
        emit = "(typeof console !== 'undefined' ? console.log : print)"
        src = prelude + script + f"\n{emit}(JSON.stringify((() => {{ {body} }})()));\n"
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "page.js"
            p.write_text(src)
            out = subprocess.run([JS, str(p)], capture_output=True, text=True, check=True)
        return json.loads(out.stdout)

    def test_self_check_passes(self):
        self.assertEqual(self.run_js(document(), "return selfCheck();"), [])

    def test_self_check_catches_a_disagreement(self):
        # A page whose arithmetic drifts from fit.py's must say so: a
        # recorded prediction moved by one part in a million fails it.
        doc = document()
        doc["checks"][0]["predicted"] *= 1 + 1e-6
        c = doc["checks"][0]
        self.assertEqual(self.run_js(doc, "return selfCheck();"), [f"{c['response']} at B={c['B']} C={c['C']}"])
        doc = document()
        doc["fits"]["gateway_cores"]["terms"].append("Z")
        doc["fits"]["gateway_cores"]["coef"].append(1.0)
        doc["fits"]["gateway_cores"]["cov"] = [[0.0] * 5 for _ in range(5)]
        self.assertIn("gateway_cores at B=10 C=10", self.run_js(doc, "return selfCheck();"))

    def test_strings_from_the_document_are_escaped(self):
        # Run names reach the tables and the tooltip as text, not markup.
        html = self.run_js(document(), "return tables();")
        self.assertNotIn("<b>", html)
        self.assertIn("&lt;b&gt;", html)
        self.assertEqual(self.run_js(document(), "return esc('<a href=\"x\">&</a>');"),
                         "&lt;a href=&quot;x&quot;&gt;&amp;&lt;/a&gt;")

    def test_rate_and_ratio_come_from_the_document(self):
        doc = document()
        self.assertEqual(self.run_js(doc, "return [rateFromFit, rssOverHeap];"), [True, fit.RSS_OVER_HEAP])
        doc["ratePerActor"] = None
        self.assertEqual(self.run_js(doc, "return [rateFromFit, rate];"), [False, 9.7])

    def test_presets_and_ranges(self):
        doc = document()
        self.assertEqual(self.run_js(doc, "return Object.keys(PRESETS);"), ["ladder", "campaign", "stock"])
        self.assertEqual(self.run_js(doc, "return [PRESETS.ladder, SLIDER, [state.M, state.P]];"),
                         [{"M": 1100000, "P": 1100000}, {"B": [1, 1000], "C": [1, 1000]}, [1100000, 1100000]])
        self.assertEqual(self.run_js(doc, "return ticks(1000);"), [0, 250, 500, 750, 1000])

    def test_clamp(self):
        doc = document()  # pool per Service 8
        for name, m, p, b, want in [
            ("ladder, B=1000: the slider's 1000", 1100000, 1100000, 1000, 1000),
            ("ladder, B=1000, M=300000: mitm binds", 300000, 1100000, 1000, 300),
            ("campaign, B=50: mitm binds", 16384, 16384, 50, 327),
            ("campaign, B=10: the slider's 1000", 16384, 16384, 10, 1000),
            ("stock, B=5: cleartext binds", 1024, 1024, 5, 128),
            ("stock, B=10: mitm binds", 1024, 1024, 10, 102),
        ]:
            with self.subTest(name):
                got = self.run_js(doc, f"state.M = {m}; state.P = {p}; return cMax({b});")
                self.assertEqual(got, want)

    def test_feasible(self):
        doc = document()
        for name, b, c, want in [
            ("inside", 50, 256, True),
            ("past mitm_internal", 100, 256, False),
            ("past the cleartext pool", 1, 2049, False),
        ]:
            with self.subTest(name):
                self.assertEqual(self.run_js(doc, f"Object.assign(state, PRESETS.campaign); return feasible({b}, {c});"), want)
        self.assertTrue(self.run_js(doc, "return feasible(1000, 1000);"))


if __name__ == "__main__":
    unittest.main()
