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

        points = page.locator('#speed-journey > .journey-points').first.locator('button')
        points.nth(0).hover()
        assert page.locator('#lesson-alpha2').is_visible()
        points.nth(1).focus()
        assert page.locator('#lesson-baseline').is_visible()
        points.nth(2).click()
        assert page.locator('#lesson-reviewed').is_visible()
        notes = page.locator('.journey-notes button')
        notes.first.focus()
        page.keyboard.press('End')
        assert notes.last.get_attribute('aria-pressed') == 'true'
        page.screenshot(path=str(shots / f'speed-{width}.png'), full_page=True)
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
    page.locator('.journey-notes button').first.tap()
    assert page.locator('#lesson-dense-groups').is_visible()
    touch.close()
    context = browser.new_context(java_script_enabled=False)
    page = context.new_page()
    page.goto(BASE + '/index.html')
    assert page.locator('#build-checkpoints').is_visible()
    assert page.locator('#speed-journey').is_visible()
    for lesson in page.locator('.journey-lesson').all():
        assert lesson.is_visible()
    browser.close()
print('PASS: journey tabs; hover/focus/tap; arrows/Home/End; receipt; 3 widths; reduced motion; no JS; screenshots')
