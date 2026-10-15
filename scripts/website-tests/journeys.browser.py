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
        for selector in ('.timing-contexts', '.milestone-lessons', '.milestone-receipts', '.provisional-milestone'):
            assert page.locator(selector).count() == 0
        assert page.locator('#speed-methods').get_attribute('open') is None
        assert 'These results show our progress. We ran them at different times and sometimes on different computers, so they aren’t a direct comparison.' in page.locator('#speed-log').inner_text()
        chart = page.locator('.milestone-chart [data-milestone]')
        popup = page.locator('#milestone-callout')
        assert popup.is_hidden()
        chart.first.hover()
        assert popup.is_visible()
        assert '4.1598 times' in popup.inner_text()
        page.keyboard.press('Escape')
        assert popup.is_hidden()
        chart.first.focus()
        page.keyboard.press('End')
        assert chart.last.get_attribute('aria-expanded') == 'true'
        assert '1.087 times' in popup.inner_text()
        page.keyboard.press('Tab')
        assert page.locator('.milestone-close').evaluate('(e)=>e === document.activeElement')
        page.keyboard.press('Enter')
        assert popup.is_hidden()
        assert chart.last.evaluate('(e)=>e === document.activeElement')
        for point in chart.all():
            point.scroll_into_view_if_needed()
            point.focus()
            point.press('Enter')
            box = popup.bounding_box()
            assert 0 <= box['x'] and box['x'] + box['width'] <= width
            assert 0 <= box['y'] and box['y'] + box['height'] <= 1000
            assert box['width'] <= 320
            assert popup.evaluate('(e)=>getComputedStyle(e).position') == 'fixed'
            assert point.get_attribute('data-lesson').endswith('.')
            page.keyboard.press('Escape')
        chart.first.click()
        page.locator('#speed-journey-title').click()
        assert popup.is_hidden()
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
        chart.first.hover()
        page.screenshot(path=str(shots / f'speed-start-hover-{width}.png'))
        page.keyboard.press('Escape')
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
    page.locator('.milestone-chart [data-milestone]').first.tap()
    assert page.locator('#milestone-callout').is_visible()
    page.locator('.milestone-close').tap()
    assert page.locator('#milestone-callout').is_hidden()
    page.locator('.milestone-chart [data-milestone]').first.tap()
    page.locator('#speed-journey-title').tap()
    assert page.locator('#milestone-callout').is_hidden()
    touch.close()
    context = browser.new_context(java_script_enabled=False)
    page = context.new_page()
    page.goto(BASE + '/index.html')
    assert page.locator('#build-checkpoints').is_visible()
    assert page.locator('#speed-journey').is_visible()
    page.locator('#speed-methods > summary').click()
    page.get_by_text('Read every chart note', exact=True).click()
    for lesson in page.locator('.journey-lesson').all():
        assert lesson.is_visible()
    browser.close()
print('PASS: journey tabs; hover/focus/tap; arrows/Home/End; receipt; 3 widths; reduced motion; no JS; screenshots')
