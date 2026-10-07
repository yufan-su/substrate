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

"""Renders the egress-path cost predictor page from fit.py --json.

Usage:
  python3 predictor.py fit.json [-o predictor.html]

The page is one self-contained HTML file: sliders for B (active actors) and
C (Services), the fitted CPU, memory and latency at that point with 95%
prediction intervals, and the measured runs on the B x C plane. B x C is
clamped to the mitm_internal breaker and the gateway-to-target pool to the
cleartext breaker. It needs no network.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import fit

TEMPLATE = Path(__file__).parent / "predictor.html.tmpl"
PLACEHOLDER = "__FIT_JSON__"


def render(doc: dict, template: str | None = None) -> str:
    if doc.get("schema") != fit.JSON_SCHEMA:
        raise ValueError(f"fit JSON schema {doc.get('schema')!r}, want {fit.JSON_SCHEMA}")
    if not doc.get("fits"):
        raise ValueError("fit JSON holds no fits")
    template = TEMPLATE.read_text() if template is None else template
    if template.count(PLACEHOLDER) != 1:
        raise ValueError(f"template must hold {PLACEHOLDER} exactly once")
    # Escaped so no string in the JSON can close the <script> element.
    payload = (json.dumps(doc, allow_nan=False, separators=(",", ":"))
               .replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026"))
    return template.replace(PLACEHOLDER, payload)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("fit_json", help="the output of fit.py --json")
    p.add_argument("-o", "--output", default="predictor.html")
    args = p.parse_args(argv)
    try:
        page = render(json.loads(Path(args.fit_json).read_text()))
    except (OSError, ValueError) as e:
        print(f"predictor.py: {e}", file=sys.stderr)
        return 1
    Path(args.output).write_text(page)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
