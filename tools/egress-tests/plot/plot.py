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

"""Plots one or more egress-tests --output reports as one HTML page.

Usage: python3 plot.py run1.json [run2.json ...] -o runs.html

One panel per metric, stacked on a shared x axis of seconds since the
steady window started, so runs line up. Color is the component, the same in
every panel and run; the dash style is the run. Phase boundaries are
vertical lines and the steady window is shaded.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import runs as rr

DASHES = ("solid", "dash", "dot")
# Above this many lines a panel shows a legend instead of end labels.
MAX_DIRECT_LABELS = 6
STEP = 1.0  # seconds between resampled points
# Phase labels closer than this share of the x span are dropped.
MIN_LABEL_GAP = 0.06
# End labels sit at least this share of the panel's y span apart.
MIN_END_LABEL_GAP = 0.08

EMPTY_NOTES = {
    "throttled": "no CPU periods counted: the containers have no CPU limit",
    "envoy": "no Envoy readings (run without --usage)",
    "rps": "no progress polls (--progress-interval 0)",
    "p99": "no progress polls (--progress-interval 0)",
}

PANELS = (
    ("cpu", "CPU (cores)"),
    ("memory", "working set (MiB)"),
    ("throttled", "CFS throttled ratio"),
    ("envoy", "gateway mitm_internal connections"),
    ("rps", "requests per second"),
    ("p99", "p99 latency (ms)"),
)


def grid_for(run: rr.Run) -> list[float]:
    start = min(s for s, _ in run.phases.values())
    end = max(e for _, e in run.phases.values())
    n = int((end - start) / STEP) + 1
    return [start + i * STEP for i in range(n)]


def by_component(rows: list[dict], include_split: bool = False) -> dict[str, dict[str, list[tuple[float, float]]]]:
    """Groups series rows as component -> pod/container -> points."""
    out: dict[str, dict[str, list[tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        ctr = r.get("container", "")
        if ctr == "POD" or (ctr in rr.SPLIT_CONTAINERS) != include_split:
            continue
        out[r["component"]][f"{r.get('pod', '')}/{ctr}"].append((rr.parse_time(r["t"]), r["value"]))
    for series in out.values():
        for pts in series.values():
            pts.sort()
    return out


def component_lines(run: rr.Run) -> dict[str, list[tuple[str, list[float], list[float | None]]]]:
    """Per panel, the lines of one run: (component, x, y)."""
    grid = grid_for(run)
    x = [g - run.steady_start for g in grid]
    lines: dict[str, list] = defaultdict(list)
    cg = ("cadvisor", "cgreader")
    for comp, series in sorted(by_component(run.series("cpu_cores", cg)).items(), key=order):
        lines["cpu"].append((comp, x, rr.step_sum(series, grid)))
    for comp, series in sorted(by_component(run.series("working_set_bytes", cg)).items(), key=order):
        lines["memory"].append((comp, x, [v / 2**20 if v is not None else None for v in rr.last_value(series, grid)]))
    for comp, series in sorted(by_component(run.series("throttled_ratio", cg)).items(), key=order):
        ys = [rr.step_sum({k: v}, grid) for k, v in series.items()]
        lines["throttled"].append((comp, x, [max((y[i] for y in ys if y[i] is not None), default=None) for i in range(len(grid))]))
    active = by_component(run.series("cluster.mitm_internal.upstream_cx_active", ("envoy",)))
    if active:
        lines["envoy"].append(("gateway", x, rr.last_value(active["gateway"], grid)))
    for metric, panel in (("req_per_s", "rps"), ("p99_ms", "p99")):
        rows = run.series(metric, ("loop",))
        if rows:
            pts = sorted((rr.parse_time(r["t"]) - run.steady_start, r["value"]) for r in rows)
            lines[panel].append(("actors", [p[0] for p in pts], [p[1] for p in pts]))
    return lines


def order(item) -> int:
    names = list(rr.COMPONENT_COLORS)
    return names.index(item[0]) if item[0] in names else len(names)


def overflow_points(run: rr.Run) -> tuple[list[float], list[float]]:
    """Times and sizes of increases of the overflow counter."""
    rows = sorted(run.series("cluster.mitm_internal.upstream_cx_overflow", ("envoy",)), key=lambda r: r["t"])
    xs, ys, last = [], [], None
    for r in rows:
        if last is not None and r["value"] > last:
            xs.append(rr.parse_time(r["t"]) - run.steady_start)
            ys.append(r["value"] - last)
        last = r["value"]
    return xs, ys


def place_labels(ys: list[float], top: float) -> list[float] | None:
    """Spreads end labels at heights ys at least MIN_END_LABEL_GAP x top
    apart, keeping their order, or returns None when they no longer fit
    under top."""
    gap = MIN_END_LABEL_GAP * top
    placed = [0.0] * len(ys)
    prev = None
    for i in sorted(range(len(ys)), key=lambda i: ys[i]):
        y = ys[i] if prev is None else max(ys[i], prev + gap)
        if y > top:
            return None
        placed[i] = prev = y
    return placed


def end_point(x: list[float], y: list[float | None]) -> tuple[float, float] | None:
    return next(((xx, yy) for xx, yy in zip(reversed(x), reversed(y)) if yy is not None), None)


def build_figure(loaded: list[rr.Run]) -> go.Figure:
    fig = make_subplots(rows=len(PANELS), cols=1, shared_xaxes=True, vertical_spacing=0.03,
                        subplot_titles=[title for _, title in PANELS])
    multi = len(loaded) > 1
    if multi and len({r.label for r in loaded}) < len(loaded):
        # Runs with the same shape are told apart by their file names.
        for r in loaded:
            r.label = f"{r.label} ({r.path.stem})"
    in_legend: set[str] = set()
    per_panel: dict[str, list] = defaultdict(list)
    for i, run in enumerate(loaded):
        for panel, lines in component_lines(run).items():
            for comp, x, y in lines:
                per_panel[panel].append((run, i, comp, x, y))
    for row, (panel, _) in enumerate(PANELS, start=1):
        entries = per_panel.get(panel, [])
        if not entries:
            # An empty trace keeps the panel's axes, which the note anchors to.
            fig.add_trace(go.Scatter(x=[0], y=[0], mode="markers", marker={"opacity": 0}, showlegend=False,
                                     hoverinfo="skip"), row=row, col=1)
            axis = "" if row == 1 else str(row)
            fig.add_annotation(text=EMPTY_NOTES.get(panel, "no data"), xref=f"x{axis} domain", yref=f"y{axis} domain",
                               x=0.5, y=0.5, showarrow=False, font={"color": "#52514e"})
        direct = len(entries) <= MAX_DIRECT_LABELS
        ends = [end_point(x, y) for _, _, _, x, y in entries]
        heights = None
        if direct:
            top = max((v for *_, y in entries for v in y if v is not None), default=0.0)
            heights = place_labels([e[1] for e in ends if e], top) if top > 0 else [e[1] for e in ends if e]
            # Labels that cannot be spread apart give way to a legend.
            direct = heights is not None
        heights = iter(heights or [])
        for (run, i, comp, x, y), end in zip(entries, ends):
            name = f"{comp} · {run.label}" if multi else comp
            show = not direct and name not in in_legend
            if show:
                in_legend.add(name)
            fig.add_trace(go.Scatter(
                x=x, y=y, mode="lines", name=name, legendgroup=name, showlegend=show,
                line={"color": rr.COMPONENT_COLORS.get(comp, "#52514e"), "width": 2, "dash": DASHES[i % len(DASHES)]},
                connectgaps=False, hovertemplate=f"{name}<br>%{{x:.0f}} s: %{{y:.3g}}<extra></extra>",
            ), row=row, col=1)
            if direct and end:
                fig.add_annotation(x=end[0], y=next(heights), text=name, showarrow=False, xanchor="left", xshift=4,
                                   font={"size": 11, "color": "#52514e"}, row=row, col=1)
        if panel == "envoy":
            for i, run in enumerate(loaded):
                xs, ys = overflow_points(run)
                if xs:
                    fig.add_trace(go.Scatter(x=xs, y=ys, mode="markers", name=f"overflow · {run.label}",
                                             marker={"symbol": "x", "size": 9, "color": "#e34948"}), row=row, col=1)
    add_phases(fig, loaded[0])
    fig.update_xaxes(title_text="seconds since steady start", row=len(PANELS), col=1)
    fig.update_xaxes(showgrid=True, gridcolor="#ececea", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#ececea", zeroline=False, rangemode="tozero")
    title = loaded[0].label if not multi else "; ".join(r.label for r in loaded)
    fig.update_layout(title=f"egress-tests: {title}", height=260 * len(PANELS), plot_bgcolor="#fcfcfb",
                      paper_bgcolor="#fcfcfb", hovermode="x unified", margin={"r": 180},
                      font={"color": "#0b0b0b"})
    return fig


def add_phases(fig: go.Figure, run: rr.Run) -> None:
    """Phase boundaries of the first run, steady shaded in every panel."""
    s0 = run.steady_start
    start, end = run.phases["steady"]
    fig.add_vrect(x0=start - s0, x1=end - s0, fillcolor="#c3c2b7", opacity=0.25, line_width=0, row="all", col=1)
    span = max(e for _, e in run.phases.values()) - min(s for s, _ in run.phases.values())
    labeled = None
    for name in rr.PHASES:
        if name not in run.phases:
            continue
        x = run.phases[name][0] - s0
        fig.add_vline(x=x, line={"color": "#8a8985", "width": 1, "dash": "dot"}, row="all", col=1)
        # Short phases sit on top of each other; label only those with room.
        if name == "steady" or labeled is None or x - labeled >= MIN_LABEL_GAP * span:
            fig.add_annotation(x=x, y=1.0, yref="paper", text=name, showarrow=False, textangle=-90,
                               xanchor="right", yanchor="top", font={"size": 10, "color": "#52514e"})
            labeled = x


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("reports", nargs="+", help="--output JSON files of egress-tests runs, at most three")
    p.add_argument("-o", "--out", required=True, help="HTML file to write")
    args = p.parse_args(argv)
    if len(args.reports) > len(DASHES):
        p.error(f"at most {len(DASHES)} runs fit on one page")
    loaded = [rr.load_run(r) for r in args.reports]
    build_figure(loaded).write_html(args.out, include_plotlyjs=True, full_html=True)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
