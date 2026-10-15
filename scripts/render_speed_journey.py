"""Render sanitized speed-log facts into the static, no-JS-readable journey.
No raw evidence is read and no measurements are executed by this script.
"""
from pathlib import Path
from html import escape
import json

ROOT = Path(__file__).resolve().parents[1]
def render(data):
    assert data['schema'] == 'assay-speed-journey-notes/v2'
    baseline = data['starting_benchmark']
    points = [dict(baseline, id='starting-benchmark', documented_at=baseline['documented_date'])] + data['milestones']
    assert len(points) == 7 and all(0 <= point['total'] <= 4.5 for point in points)
    svg = ['<svg class="milestone-chart" viewBox="0 0 760 350" role="group" aria-labelledby="milestone-chart-title milestone-chart-desc"><title id="milestone-chart-title">Speed milestones, October 11–14, 2026</title><desc id="milestone-chart-desc">Total Assay to Polars time ratio, lower is faster, on one shared zero-to-4.5 scale. A teal diamond marks the separate M2 Pro starting benchmark; coral circles mark Development Mini diagnostics. Points are not connected or a controlled same-host trend. Seven milestones in documented order; spacing is not elapsed time. Each point opens its lesson.</desc>']
    y = lambda ratio: 280 - ratio / 4.5 * 240
    for ratio in (0, 1, 2, 3, 4, 4.5):
        svg.append(f'<g class="milestone-grid"><line x1="55" x2="720" y1="{y(ratio):.1f}" y2="{y(ratio):.1f}"/><text x="43" y="{y(ratio)+4:.1f}" text-anchor="end">{ratio:g}×</text></g>')
    svg.append(f'<line class="milestone-goal" x1="55" x2="720" y1="{y(.95):.1f}" y2="{y(.95):.1f}"/><text class="milestone-goal-label" x="62" y="{y(.95)+19:.1f}">0.95× goal</text>')
    choices, lessons = [], []
    for i, point in enumerate(points):
        historical = i == 0
        x = 80 + 100*i; py = y(point['total']); date = point['documented_at']; id = point['id']
        ratio = f'{point["total"]:.4f}' if historical else f'{point["total"]:.3f}'
        scope = 'separate M2 Pro historical run' if historical else 'Development Mini diagnostic'
        label = f"{point['title']}, {date[:10]}, {ratio} times Polars total, {scope}"
        mark = f'<path class="milestone-dot milestone-dot--historical" d="M{x} {py-7:.1f} l7 7 l-7 7 l-7 -7 Z"/>' if historical else f'<circle class="milestone-dot" cx="{x}" cy="{py:.1f}" r="5"/>'
        subdate = 'Report date' if historical else date[11:16] + ' UTC'
        svg.append(f'<g class="milestone-choice" data-milestone="{id}" role="button" tabindex="0" aria-pressed="false" aria-controls="lesson-{id}" aria-label="{escape(label)}"><circle class="milestone-hit" cx="{x}" cy="{py:.1f}" r="24"/>{mark}<text x="{x}" y="{py-19:.1f}" text-anchor="middle">{ratio}×</text></g><text class="milestone-date" x="{x}" y="308" text-anchor="middle">Oct {date[8:10]}</text><text class="milestone-date" x="{x}" y="328" text-anchor="middle">{subdate}</text>')
        title = 'M2 Pro starting benchmark' if historical else point['title']
        classes = 'journey-point starting-choice' if historical else 'journey-point'
        choices.append(f'<li><button class="{classes}" type="button" data-milestone="{id}" aria-controls="lesson-{id}" aria-pressed="false">{i+1}. {escape(title)}</button></li>')
        if historical:
            lessons.append(f'<div class="journey-lesson" id="lesson-{id}"><h4>{escape(title)}</h4><p>October 11 · documented historical report · separate M2 Pro measurement.</p><p><strong>Total: {ratio}× Polars.</strong> Geometric mean: {baseline["geomean"]:.4f}×.</p><p>Alpha 2-equivalent native source · Apple M2 Pro · roughly 10 GB · all 22 queries · 10 requested workers. Three rounds, one warmup then five hot samples per engine/query/round; SQL to materialized Arrow. Totals sum query-round medians.</p><p class="note">Best-effort host with background activity; native CLI, not installed Python wheel/API timing. The historical M4 4.1364× result is a different run. This point is not a controlled same-host baseline for the Development Mini milestones.</p><p><a href="{baseline["source"]}">Historical M2 Pro receipt and method</a></p></div>')
        else:
            geo = f'{point["geomean"]:.3f}×' if point['geomean'] is not None else 'not reported for this final run'
            lessons.append(f'<div class="journey-lesson" id="lesson-{id}"><h4>{escape(point["title"])}</h4><p><time datetime="{date}">{date[:10]} {date[11:16]} UTC</time> · documented milestone · development diagnostic.</p><p><strong>Total: {ratio}× Polars.</strong> Geometric mean: {geo}.</p><p>{escape(point["lesson"])}</p><p class="note">Development Mini · SF10 · 22 queries · 10 requested workers · reported single round; historical Polars reference medians. Historical sampling compatibility is unverified; this is not a paired percentage improvement against main.</p></div>')
    svg.append('</svg>')
    return '<!-- speed-log:start -->' + '''<div id="speed-log" class="speed-log"><h3>Speed-log milestones · October 11–14</h3><p>Roughly 10 GB · 22 queries · 10 requested workers. The chart shows <strong>total time relative to Polars</strong>; lower is faster. Hover, focus or tap a point for its lesson and separate geometric mean.</p><p class="note">Dates mark when milestones were documented, not exact measurement completion. Points are equally spaced in recorded order. All seven points share the same 0–4.5× scale. Different hosts and methods remain separate, unconnected measurements, not a controlled trend or release qualification.</p><p class="milestone-legend"><span class="milestone-legend--historical">◆ Separate M2 Pro historical run</span><span class="milestone-legend--development">● Development Mini diagnostics</span></p><p class="note milestone-scroll-hint">On a narrow screen, swipe or scroll the chart to see all seven points. The buttons below select the same milestones.</p><div class="milestone-scroll" role="region" aria-label="Speed-log chart; scroll horizontally on small screens" tabindex="0">'''+''.join(svg)+'''</div><ol class="journey-points milestone-lessons">'''+''.join(choices)+'''</ol><div class="milestone-receipts">'''+''.join(lessons)+'''</div><details><summary>Development timing method and limits</summary><p>These are reported single-round development results against historical Polars references. Total compares sums of query medians; geometric means remain separately reported. The retained input documentation says historical sampling compatibility still needed review. Corrected collector requirements do not establish that these historical runs excluded warmup and retained five complete timed samples, so no corrected-protocol compatibility is claimed.</p><p>The development gate allowed noise-triggered timing retries; these summaries do not establish an unbiased sampling history. Host conditions varied; a single round cannot establish a controlled improvement trend. The exact hardware model is not attested in the retained gate summaries. Values are transcribed at their reported precision, not recomputed from raw samples.</p></details></div><div class="provisional-milestone page-block"><h3>October 15 · separate M4 protocol</h3><p class="eyebrow">Provisional · one attempt · not accepted</p><p><strong>0.970× Polars total; 1.046× geometric mean.</strong> Both must reach 0.95×; the goal remains unmet. This point is separate from the Development Mini gate chart and the Test Mini evidence.</p><p>Combined source · Apple M4 development host · 16 GiB RAM · 10 requested workers · all 22 SF10 queries. Three rotated rounds, one warmup and five timed samples per engine/query/round. Totals sum query-round medians; geometric mean uses paired ratios.</p><details><summary>Provisional run: lesson, intervals and configuration</summary><p>Less total time can coexist with a slower geometric mean. Marginal 95% intervals: total 0.967–0.973×; geometric mean 1.041–1.048×, bootstrapped over three round clusters. Fifteen samples are not fifteen independent host trials.</p><p>macOS 26.6.2, Rust 1.90.0, no custom RUSTFLAGS; Assay 4 GiB accounted budget, Polars streaming. Resource settings are not equivalent whole-process caps. Quiet-host admission sampled six five-second intervals; the harness did not continuously record background activity. This is a documented development attempt, not accepted public performance evidence.</p></details></div><p><a href="evidence/speed-journey-notes.json">Sanitized milestone data, dates and methods</a>.</p><!-- speed-log:end -->'''

def main():
    path = ROOT/'site/index.html'; page = path.read_text()
    if '<!-- speed-log:start -->' in page:
        start=page.index('<!-- speed-log:start -->');end=page.index('<!-- speed-log:end -->')+len('<!-- speed-log:end -->')
    else:
        start=page.index('<h3>Lessons from the speed log</h3>');end=page.index('<p><a href="contribute.html#full-suite-evidence">',start)
    fragment=render(json.loads((ROOT/'site/evidence/speed-journey-notes.json').read_text()))
    path.write_text(page[:start]+fragment+page[end:])
if __name__=='__main__':main()
