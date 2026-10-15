"""Journey interactions and receipt preservation; no engine runs or live GitHub API."""
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = os.environ.get('WEBSITE_TEST_URL', 'http://127.0.0.1:8080')
shots = Path(os.environ.get('WEBSITE_SCREENSHOTS', '/tmp/assay-site-previews'))
shots.mkdir(parents=True, exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=os.environ.get('CHROMIUM', '/usr/bin/chromium'), args=['--no-sandbox'])
    for width in (1440, 390, 320):
        page = browser.new_page(viewport={'width': width, 'height': 1000}, reduced_motion='reduce')
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.route('https://api.github.com/**', lambda r: r.fulfill(status=200, content_type='application/json', body='[]'))
        page.goto(BASE + '/index.html')
        page.wait_for_selector('#build-checkpoints[data-enhanced="true"]')
        assert page.locator('#speed-journey').is_hidden()
        assert '5,935,512 / 5,939,714' in page.locator('#build-receipt').inner_text()
        page.locator('[data-checkpoint="0"]').focus()
        page.keyboard.press('End')
        assert page.locator('[data-checkpoint="5"]').get_attribute('aria-pressed') == 'true'
        page.screenshot(path=str(shots / f'accuracy-{width}.png'), full_page=True)
        page.locator('#journey-accuracy').focus()
        page.keyboard.press('ArrowRight')
        assert page.locator('#journey-speed').get_attribute('aria-selected') == 'true'
        assert page.locator('#build-checkpoints').is_hidden()
        assert page.locator('#speed-journey').is_visible()
        site = Path(__file__).resolve().parents[2] / 'site'
        historic = json.loads((site / 'evidence/completed-speed-macbook-run-001.json').read_text())
        reviewed = json.loads((site / 'evidence/full-suite/latest.json').read_text())
        expected = [historic['lanes'][f'sf10-w{w}']['comparisons']['assaydb_polars']['total']['point'] for w in (1, 10)]
        expected += [reviewed['timing']['phases'][f'{role}-timing-w{w}']['polars']['total'] for role in ('baseline', 'candidate') for w in (1, 10)]
        assert page.locator('.journey-lane strong').all_text_contents() == [f'{value:.4f}×' for value in expected]

        page.locator('.timing-contexts > summary').click()
        points = page.locator('.timing-contexts .journey-points').first.locator('button')
        points.nth(0).hover()
        assert page.locator('#lesson-alpha2').is_visible()
        points.nth(1).focus()
        assert page.locator('#lesson-baseline').is_visible()
        points.nth(2).click()
        assert page.locator('#lesson-reviewed').is_visible()
        page.locator('.timing-contexts > summary').click()
        notes = page.locator('.milestone-lessons button')
        # Choice geometry must stay stable for every selection: no staggered rows,
        # stretched siblings or blank grid slots from expanding a selected card.
        geometry = "els => els.map(e => ({x:e.offsetLeft,y:e.offsetTop,w:e.offsetWidth,h:e.offsetHeight}))"
        initial = page.locator('.milestone-lessons > li').evaluate_all(geometry)
        assert all(row['h'] == 48 for row in initial), initial
        if width > 640:
            assert initial[0]['w'] == 700
            assert all(initial[i]['y'] == initial[i+1]['y'] for i in (1,3,5)), initial
            assert initial[1]['y'] - initial[0]['y'] == 60
            assert initial[3]['y'] - initial[1]['y'] == 60
        else:
            assert all(row['x'] == initial[0]['x'] for row in initial), initial
            assert all(initial[i+1]['y'] - initial[i]['y'] == 60 for i in range(6)), initial
        for option in notes.all():
            option.focus()
            assert page.locator('.milestone-lessons > li').evaluate_all(geometry) == initial
            assert page.locator('.milestone-receipts .journey-lesson:visible').count() == 1
        chart_bounds = page.locator('.milestone-scroll').bounding_box()
        choices_bounds = page.locator('.milestone-lessons').bounding_box()
        assert abs(chart_bounds['x'] - choices_bounds['x']) < 1
        assert abs(chart_bounds['width'] - choices_bounds['width']) < 1

        notes.first.focus()
        page.keyboard.press('End')
        assert notes.last.get_attribute('aria-pressed') == 'true'
        chart = page.locator('.milestone-chart [data-milestone]')
        chart.first.hover()
        assert page.locator('#lesson-starting-benchmark').is_visible()
        chart.first.focus()
        page.keyboard.press('End')
        assert chart.last.get_attribute('aria-pressed') == 'true'
        assert page.locator('#lesson-continuations').is_visible()
        # Generated fallback and numeric data must remain byte-for-byte synchronized.
        import importlib.util
        spec = importlib.util.spec_from_file_location('journey_render', site.parent / 'scripts/render_speed_journey.py')
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        data = json.loads((site / 'evidence/speed-journey-notes.json').read_text())
        assert module.render(data) in (site / 'index.html').read_text()
        assert [p['total'] for p in data['milestones']] == [1.302, 1.255, 1.234, 1.187, 1.110, 1.087]
        assert data['group']['protocol'] is None
        assert data['starting_benchmark']['total'] == historic['lanes']['sf10-w10']['comparisons']['assaydb_polars']['total']['point']
        assert page.locator('.journey-start').count() == 0
        assert page.locator('.milestone-chart [data-milestone]').count() == 7
        assert '4.1598×' in page.locator('.milestone-chart [data-milestone=starting-benchmark]').text_content()
        assert page.locator('.milestone-dot--historical').evaluate('(e)=>e.tagName.toLowerCase()') == 'path'
        assert page.locator('.milestone-legend').is_visible()
        assert page.locator('.milestone-chart').bounding_box()['width'] == 700
        page.evaluate("document.activeElement.blur(); document.querySelector('.milestone-scroll').scrollLeft = 0")
        page.screenshot(path=str(shots / f'speed-{width}.png'), full_page=True)
        page.locator('#speed-log').screenshot(path=str(shots / f'speed-log-{width}.png'))
        page.locator('#journey-speed').focus()
        page.keyboard.press('Home')
        assert page.locator('#build-checkpoints svg').bounding_box()['width'] > 200
        page.locator('#tab-sf10').click()
        assert page.locator('#speed-sf10').is_visible()
        page.locator('#tab-sf10').press('Home')
        assert page.locator('#speed-sf1').is_visible()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        for name in ('contribute', 'verification', 'reference'):
            page.goto(BASE + '/' + name + '.html')
            page.evaluate('document.fonts.ready')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), (name, width)
            page.screenshot(path=str(shots / f'{name}-{width}.png'), full_page=True)
        assert not errors, errors
        page.close()
    touch = browser.new_context(viewport={'width':390,'height':844}, is_mobile=True, has_touch=True)
    page = touch.new_page()
    page.goto(BASE + '/index.html')
    page.locator('#journey-speed').tap()
    page.locator('.milestone-lessons button').first.tap()
    assert page.locator('#lesson-starting-benchmark').is_visible()
    touch.close()
    context = browser.new_context(java_script_enabled=False)
    page = context.new_page()
    page.goto(BASE + '/index.html')
    assert page.locator('#build-checkpoints').is_visible()
    assert page.locator('#speed-journey').is_visible()
    page.locator('.timing-contexts > summary').click()
    for lesson in page.locator('.journey-lesson').all():
        assert lesson.is_visible()
    browser.close()
print('PASS: journey tabs; hover/focus/tap; arrows/Home/End; receipt; 3 widths; reduced motion; no JS; screenshots')
