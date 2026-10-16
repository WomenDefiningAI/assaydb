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
    lessons = []
    notes = {
        'starting-benchmark': ('An early speed test.', 'This test used an M2 Pro, a different computer from the later development tests.'),
        'dense-groups': ('We reduced repeated grouping work.', 'Giving each group a small ID meant less work for every row.'),
        'distinct-radix': ('Counting unique values got faster.', 'Sorting values together helped us avoid checking the same values repeatedly.'),
        'scan-reuse': ('We stopped reading the same data twice.', 'We kept a small filtered result so the next step could reuse it.'),
        'cardinality': ('We used real row counts.', 'Counting the rows first helped us choose a better way to run the query.'),
        'scan-claiming': ('Workers shared the reading work.', 'Workers could pick up the next available task instead of waiting for a slow worker.'),
        'continuations': ('Workers waited less.', 'Workers could start their next step without waiting for the coordinator.')
    }
    for i, point in enumerate(points):
        historical = i == 0
        x = 80 + 100*i; py = y(point['total']); date = point['documented_at']; id = point['id']
        ratio = f'{point["total"]:.4f}' if historical else f'{point["total"]:.3f}'
        note_title, note_lesson = notes[id]
        note_date = f'We recorded this result on October {int(date[8:10])}, 2026.'
        note_result = f'Assay took {ratio} times as long as Polars in this test.'
        scope = 'separate M2 Pro historical run' if historical else 'Development Mini diagnostic'
        label = f"{point['title']}, {date[:10]}, {ratio} times Polars total, {scope}"
        mark = f'<path class="milestone-dot milestone-dot--historical" d="M{x} {py-7:.1f} l7 7 l-7 7 l-7 -7 Z"/>' if historical else f'<circle class="milestone-dot" cx="{x}" cy="{py:.1f}" r="5"/>'
        subdate = 'Report date' if historical else date[11:16] + ' UTC'
        svg.append(f'<g class="milestone-choice" data-milestone="{id}" role="button" tabindex="0" aria-expanded="false" aria-haspopup="dialog" aria-controls="milestone-callout" aria-label="{escape(label)}" data-title="{escape(note_title)}" data-date="{escape(note_date)}" data-result="{escape(note_result)}" data-lesson="{escape(note_lesson)}"><circle class="milestone-hit" cx="{x}" cy="{py:.1f}" r="24"/>{mark}<text x="{x}" y="{py-19:.1f}" text-anchor="middle">{ratio}×</text></g><text class="milestone-date" x="{x}" y="308" text-anchor="middle">Oct {date[8:10]}</text><text class="milestone-date" x="{x}" y="328" text-anchor="middle">{subdate}</text>')
        title = 'M2 Pro starting benchmark' if historical else point['title']
        classes = 'journey-point starting-choice' if historical else 'journey-point'
        if historical:
            lessons.append(f'<div class="journey-lesson" id="lesson-{id}"><h4>{escape(title)}</h4><p>October 11 · documented historical report · separate M2 Pro measurement.</p><p><strong>Total: {ratio}× Polars.</strong> Geometric mean: {baseline["geomean"]:.4f}×.</p><p>Alpha 2-equivalent native source · Apple M2 Pro · roughly 10 GB · all 22 queries · 10 requested workers. Three rounds, one warmup then five hot samples per engine/query/round; SQL to materialized Arrow. Totals sum query-round medians.</p><p class="note">Best-effort host with background activity; native CLI, not installed Python wheel/API timing. The historical M4 4.1364× result is a different run. This point is not a controlled same-host baseline for the Development Mini milestones.</p><p><a href="{baseline["source"]}">Historical M2 Pro receipt and method</a></p></div>')
        else:
            geo = f'{point["geomean"]:.3f}×' if point['geomean'] is not None else 'not reported for this final run'
            lessons.append(f'<div class="journey-lesson" id="lesson-{id}"><h4>{escape(point["title"])}</h4><p><time datetime="{date}">{date[:10]} {date[11:16]} UTC</time> · documented milestone · development diagnostic.</p><p><strong>Total: {ratio}× Polars.</strong> Geometric mean: {geo}.</p><p>{escape(point["lesson"])}</p><p class="note">Development Mini · SF10 · 22 queries · 10 requested workers · reported single round; historical Polars reference medians. Historical sampling compatibility is unverified; this is not a paired percentage improvement against main.</p></div>')
    svg.append('</svg>')
    return '<!-- speed-log:start -->' + '''<div id="speed-log" class="speed-log"><h3>How speed changed.</h3><p>The chart compares how long Assay and Polars took to run the same 22 queries on about 10 GB of data. Lower numbers mean less time. Move over a point, focus it with your keyboard, or tap it to learn what changed.</p><p class="note">These results show our progress. We ran them at different times and sometimes on different computers, so they aren’t a direct comparison.</p><p class="milestone-legend"><span class="milestone-legend--historical">◆ The teal diamond was measured on an M2 Pro.</span><span class="milestone-legend--development">● The coral circles were measured on the Development Mini.</span></p><p class="note milestone-scroll-hint">Swipe or scroll sideways to see all seven points.</p><div class="milestone-scroll" role="region" aria-label="Speed chart; scroll sideways on small screens" tabindex="0">'''+''.join(svg)+'''</div><div id="milestone-callout" class="milestone-callout" role="dialog" aria-modal="false" aria-labelledby="milestone-callout-title" hidden><button type="button" class="milestone-close" aria-label="Close this chart note">Close</button><h4 id="milestone-callout-title"></h4><p data-callout-date></p><p data-callout-result></p><p data-callout-lesson></p></div><details id="speed-methods"><summary>How we tested</summary><p>The dates show when we documented each result, not its exact completion time. Points are equally spaced in recorded order. All seven use the same 0–4.5× total-time scale, but different computers and methods mean they do not show a controlled improvement. We do not connect them with a line.</p><p>The Alpha 3 target is 0.95× Polars for both total time and geometric mean. Total compares the time for the whole workload. Geometric mean gives each query equal weight when comparing time ratios. Neither measure alone establishes that we met the goal.</p><p>The Development Mini results are reported single-round diagnostics against historical Polars references. Their sampling-compatibility audit was outstanding. Corrected collector code does not establish that these older runs excluded warmup or retained five complete timed samples. We make no corrected-protocol compatibility claim. Noise-triggered retries were allowed; these summaries do not establish an unbiased sampling history.</p><p>The M2 Pro starting point uses a different three-round native-CLI protocol. It had background activity. It is distinct from the historical M4 result of 4.1364×. The chart notes below retain the exact measurement scopes and limits.</p><details><summary>Read every chart note</summary>'''+''.join(lessons)+'''</details><details><summary>A separate provisional test on October 15</summary><p>One M4 development-host attempt reported 0.970× Polars total time and 1.046× geometric mean. It is not accepted performance evidence and does not meet the 0.95× goal. It is separate from the chart’s Development Mini diagnostics and the Test Mini evidence.</p><p>The run used all 22 SF10 queries, 10 requested workers, three rotated rounds, one warmup and five timed samples per engine/query/round. Marginal 95% intervals were 0.967–0.973× total and 1.041–1.048× geometric mean, bootstrapped over three round clusters. Fifteen samples are not fifteen independent host trials.</p><p>The host was an Apple M4 with 16 GiB RAM, macOS 26.6.2 and Rust 1.90.0, with no custom RUSTFLAGS. Assay used a 4 GiB accounted budget and Polars used streaming. Resource settings are not equivalent whole-process caps. Quiet-host admission sampled six five-second intervals; the harness did not continuously record background activity.</p></details><p><a href="evidence/speed-journey-notes.json">Read the milestone data and methods.</a> <a href="https://github.com/WomenDefiningAI/assaydb/blob/main/docs/ENGINE-SNAPSHOT-QUALIFICATION.md">Read the reviewed PR evidence.</a></p></details></div><!-- speed-log:end -->'''

def main():
    path = ROOT/'site/index.html'; page = path.read_text()
    if '<!-- speed-log:start -->' in page:
        start=page.index('<!-- speed-log:start -->');end=page.index('<!-- speed-log:end -->')+len('<!-- speed-log:end -->')
    else:
        start=page.index('<h3>Lessons from the speed log</h3>');end=page.index('<p><a href="https://github.com/WomenDefiningAI/assaydb/blob/main/docs/ENGINE-SNAPSHOT-QUALIFICATION.md">',start)
    fragment=render(json.loads((ROOT/'site/evidence/speed-journey-notes.json').read_text()))
    path.write_text(page[:start]+fragment+page[end:])
if __name__=='__main__':main()
