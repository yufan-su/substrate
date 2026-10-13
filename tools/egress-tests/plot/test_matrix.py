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

import json
import math
import tempfile
import unittest
from pathlib import Path

import matrix

T0 = "2026-10-13T07:00:00Z"


def at(seconds: float) -> str:
    whole = int(seconds)
    frac = int(round((seconds - whole) * 1e9))
    return f"2026-10-13T07:{whole // 60:02d}:{whole % 60:02d}.{frac:09d}Z"


def histogram(samples_by_bucket: dict) -> list:
    counts = [0] * (len(matrix.BOUNDS) + 1)
    for bucket, n in samples_by_bucket.items():
        counts[bucket] = n
    return counts


def report(scheme: str, b: int, c: int, *, pods: int = 2, settled_at: float = 60, end: float = 180,
           opening_cpu: float = 1.0, settled_cpu: float = 0.5, settled_cores_per_krps: float = 0.42) -> dict:
    """A report whose gateway pods burn opening_cpu cores each before the
    settled window and settled_cpu after, at 1000 req/s throughout, with 100
    requests in bucket 70 and 1 in bucket 90 over the settled window."""
    samples, memory = [], []
    for pod in range(pods):
        name = f"gw-{pod}"
        for t in range(0, int(end) + 1, 10):
            cpu = opening_cpu * min(t, settled_at) + settled_cpu * max(0, t - settled_at)
            samples.append({"t": at(t), "component": "gateway", "pod": name, "container": "POD", "cpuSeconds": cpu,
                            "workingSetBytes": 1})
            samples.append({"t": at(t), "component": "gateway", "pod": name, "container": "envoy", "cpuSeconds": cpu,
                            "workingSetBytes": 1})
            memory.append({"t": at(t), "pod": name, "allocatedBytes": (pod + 1) * 2 ** 30 * (t / end), "heapSizeBytes": 0})
    before = histogram({60: 1000 * int(settled_at)})
    after = [x + y for x, y in zip(before, histogram({70: 100, 90: 1}))]
    timeline = [{"t": at(0), "requests": 0, "latency": {"counts": histogram({}), "maxMicros": 0}},
                {"t": at(settled_at), "requests": 1000 * int(settled_at), "latency": {"counts": before, "maxMicros": 5000}},
                {"t": at(end), "requests": 1000 * int(end), "latency": {"counts": after, "maxMicros": 50000}}]
    return {
        "config": {"actors": max(b, 1000), "parallel": b, "endpoints": c, "scheme": scheme},
        "phases": [{"name": "steady", "start": at(0), "end": at(end)}],
        "loopTimeline": timeline,
        "resources": {"settledWindow": {"start": at(settled_at)}, "samples": samples, "envoyMemory": memory,
                      "components": {"gateway": {"pods": pods, "settled": {"coresPerKrps": settled_cores_per_krps}}}},
    }


class ExtractTest(unittest.TestCase):
    def test_row_values(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "M-https-c0100-b0010.json"
            p.write_text(json.dumps(report("https", 10, 100)))
            row = matrix.extract(p)
        self.assertEqual((row["B"], row["C"], row["scheme"], row["tunnels"], row["replicas"]), (10, 100, "https", 1000, 2))
        # Two pods at 1.0 core each over the opening 60 s at 1000 req/s: 2 cores per krps.
        self.assertAlmostEqual(row["openingCoresPerKrps"], 2.0, places=6)
        self.assertEqual(row["settledCoresPerKrps"], 0.42)
        # 101 settled samples: the 100th is in bucket 70, the 101st in bucket 90.
        self.assertEqual(row["settledP99Micros"], math.ceil(matrix.BOUNDS[70]))
        self.assertEqual(row["settledP999Micros"], math.ceil(matrix.BOUNDS[90]))
        # Allocated bytes 1 GiB + 2 GiB at the last second.
        self.assertEqual(row["envoyHeapBytes"], 3 * 2 ** 30)

    def test_quantile_follows_the_drivers_bucket_rule(self):
        counts = histogram({10: 5, 20: 4, len(matrix.BOUNDS): 1})
        self.assertEqual(matrix.quantile(counts, 0.5, 10 ** 9), math.ceil(matrix.BOUNDS[10]))
        self.assertEqual(matrix.quantile(counts, 0.9, 10 ** 9), math.ceil(matrix.BOUNDS[20]))
        self.assertEqual(matrix.quantile(counts, 0.999, 777), 777, "the overflow bucket is the longest sample")
        self.assertTrue(math.isnan(matrix.quantile([0] * 3, 0.5, 1)))

    def test_heap_total_needs_every_replica_in_the_second(self):
        memory = [{"t": at(0), "pod": "a", "allocatedBytes": 10}, {"t": at(0), "pod": "b", "allocatedBytes": 20},
                  {"t": at(1), "pod": "a", "allocatedBytes": 100}]
        self.assertEqual(matrix.heap_total_max(memory), (30, 2))


class RenderTest(unittest.TestCase):
    def test_http_before_https_within_a_cell_and_cells_by_tunnels(self):
        rows = []
        with tempfile.TemporaryDirectory() as d:
            for scheme, b, c in (("https", 20, 10), ("http", 20, 10), ("https", 10, 10)):
                p = Path(d) / f"M-{scheme}-c{c:04d}-b{b:04d}.json"
                p.write_text(json.dumps(report(scheme, b, c)))
                rows.append(matrix.extract(p))
        text = matrix.render(rows)
        lines = text.splitlines()
        self.assertEqual(lines[0], "| " + " | ".join(matrix.COLUMNS) + " |")
        self.assertEqual([l.split(" | ")[0:2] for l in lines[2:]],
                         [["| 10 x 10", "HTTPS"], ["| 20 x 10", "HTTP"], ["| 20 x 10", "HTTPS"]])
        self.assertIn("| 200 | 2 | 2.00 | 0.42 | 3.0 GiB |", lines[3])

    def test_main_writes_markdown_and_json(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "M-http-c0010-b0010.json"
            p.write_text(json.dumps(report("http", 10, 10)))
            md, js = Path(d) / "MATRIX.md", Path(d) / "matrix.json"
            self.assertEqual(matrix.main([str(p), "--out", str(md), "--json", str(js), "--title", "T"]), 0)
            self.assertTrue(md.read_text().startswith("# T\n\n"))
            self.assertIn("| 10 x 10 | HTTP | 100 | 2 |", md.read_text())
            self.assertEqual(json.loads(js.read_text())[0]["run"], "M-http-c0010-b0010")


if __name__ == "__main__":
    unittest.main()
