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
        'starting-benchmark': ('We measured an early version.', 'This test used an M2 Pro, a different computer from the later development tests.'),
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
            lessons.append(f'<div class="journey-lesson" id="lesson-{id}"><h4>{escape(title)}</h4><p>October 11 · documented historical report · separate M2 Pro measurement.</p><p><strong>Total: {ratio}× Polars.</strong> Geometric mean: {baseline["geomean"]:.4f}×.</p><p>This M2 Pro test used command-line code matched to Alpha 2, about 10 GB, all 22 queries and 10 workers. Each of three rounds included a practice run and five timed runs per query and engine. Timing ends when the complete answer is ready; totals add up the middle times for each query and round.</p><p class="note">Other activity was running on the computer. This measures the command-line engine, not the Python package. The M4 result of 4.1364× is a different test. Do not treat this as a matching baseline for the Development Mini.</p><p><a href="{baseline["source"]}">Historical M2 Pro receipt and method</a></p></div>')
        else:
            geo = f'{point["geomean"]:.3f}×' if point['geomean'] is not None else 'not reported for this final run'
            lessons.append(f'<div class="journey-lesson" id="lesson-{id}"><h4>{escape(note_title)}</h4><p><time datetime="{date}">{date[:10]} {date[11:16]} UTC</time> · documented milestone · development diagnostic.</p><p><strong>Total: {ratio}× Polars.</strong> Geometric mean: {geo}.</p><p>{escape(note_lesson)}</p><p class="note">This Development Mini test used about 10 GB, all 22 queries and 10 workers. It reports one round against older Polars timings. Matching sampling methods have not been verified, so it does not measure improvement against the main version.</p></div>')
    svg.append('</svg>')
    return '<!-- speed-log:start -->' + '''<div id="speed-log" class="speed-log"><h3>How speed changed.</h3><p>The chart compares how long Assay and Polars took to run the same 22 queries on about 10 GB of data. A value of 1× means equal time; 2× means Assay took twice as long. Lower is faster. Move over a point, focus it with your keyboard, or tap it to learn what changed.</p><p class="note">These results show our progress. We ran them at different times and sometimes on different computers, so they aren’t a direct comparison.</p><p class="milestone-legend"><span class="milestone-legend--historical">◆ The teal diamond was measured on an M2 Pro.</span><span class="milestone-legend--development">● The coral circles were measured on the Development Mini.</span></p><p class="note milestone-scroll-hint">Swipe or scroll sideways to see all seven points.</p><div class="milestone-scroll" role="region" aria-label="Speed chart; scroll sideways on small screens" tabindex="0">'''+''.join(svg)+'''</div><div id="milestone-callout" class="milestone-callout" role="dialog" aria-modal="false" aria-labelledby="milestone-callout-title" hidden><button type="button" class="milestone-close" aria-label="Close this chart note">Close</button><h4 id="milestone-callout-title"></h4><p data-callout-date></p><p data-callout-result></p><p data-callout-lesson></p></div><details id="speed-methods"><summary>How we tested</summary><p>Dates show when we recorded each result, not exactly when the test finished. The points are equally spaced in that order and share one scale. We leave them unconnected because the computers and methods differ.</p><p>The Alpha 3 goal is 0.95× Polars on two measures. Total time adds up the query times. The geometric mean combines query-by-query time ratios, giving each query equal weight. We must meet both targets.</p><p>The Development Mini points each report one round compared with older Polars timings. We have not verified that those runs used matching sampling rules, excluded practice runs, or kept all five timed repetitions. Fixing the timing code later does not settle that question. Busy-host retries were allowed and may affect the results.</p><p>The M2 Pro starting point used three rounds of the command-line engine while other activity was running. It is a different test from the historical M4 result of 4.1364×. Exact methods and limits are linked below.</p><details><summary>Read every chart note</summary>'''+''.join(lessons)+'''</details><details><summary>A separate provisional test on October 15</summary><p>A separate M4 test reported 0.970× Polars total time and 1.046× geometric mean. This is a provisional result, not accepted performance evidence. It does not meet the 0.95× goal and is separate from the chart points and the Test Mini results.</p><p>The test used all 22 queries, 10 workers and three rounds that changed the engine order. Each round included one practice run and five timed runs per query and engine. Estimated 95% intervals were 0.967–0.973× for total time and 1.041–1.048× for the geometric mean. These estimates use three rounds; the fifteen timings are not fifteen independent tests of the computer.</p><p>The M4 had 16 GiB of memory. Assay tracked a 4 GiB budget; Polars processed data in smaller batches. Those settings are not equivalent limits on all memory used. Checks for a quiet computer covered six five-second periods, not the whole run. The linked record includes software versions and other settings.</p></details><p><a href="evidence/speed-journey-notes.json">Read the milestone data and methods.</a> <a href="https://github.com/WomenDefiningAI/assaydb/blob/main/docs/ENGINE-SNAPSHOT-QUALIFICATION.md">Read the reviewed PR evidence.</a></p></details></div><!-- speed-log:end -->'''

def main():
    path = ROOT/'site/index.html'; page = path.read_text()
    if '<!-- speed-log:start -->' in page:
        start=page.index('<!-- speed-log:start -->');end=page.index('<!-- speed-log:end -->')+len('<!-- speed-log:end -->')
    else:
        start=page.index('<h3>Lessons from the speed log</h3>');end=page.index('<p><a href="https://github.com/WomenDefiningAI/assaydb/blob/main/docs/ENGINE-SNAPSHOT-QUALIFICATION.md">',start)
    fragment=render(json.loads((ROOT/'site/evidence/speed-journey-notes.json').read_text()))
    path.write_text(page[:start]+fragment+page[end:])
if __name__=='__main__':main()
