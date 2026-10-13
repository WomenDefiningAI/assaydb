"""Local browser checks. Requires Python Playwright and Chromium; no live GitHub calls.
Run with site/ served at http://127.0.0.1:8080/.
"""
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = os.environ.get('WEBSITE_TEST_URL', 'http://127.0.0.1:8080')
API = 'https://api.github.com/repos/WomenDefiningAI/assaydb/'
HOSTILE = '<img src=x onerror="window.injected=true"> & <script>window.injected=true</script>'
PR = dict(number=42, title=HOSTILE + ' alice@example.com', state='closed', merged_at='2026-10-14T10:00:00Z',
          base=dict(ref='main', repo=dict(id=1418037309, full_name='WomenDefiningAI/assaydb')),
          user=dict(login='alice', type='User'), html_url='javascript:window.injected=true')
COMMITS = [dict(sha=letter*40, parents=[{}], author=dict(login=login, type=kind))
           for letter, login, kind in [('a', 'bob', 'User'), ('b', 'builder[bot]', 'Bot')]]
COMMITS.append(dict(sha='c'*40, parents=[{}], author=None, commit=dict(author=dict(email='private@example.com', name='Private Name'))))

def route_data(route):
    url = route.request.url
    data = ([dict(filename='tests/engine.rs')] if '/files?' in url else
            COMMITS if '/commits?' in url else [PR])
    route.fulfill(status=200, content_type='application/json', body=json.dumps(data))

with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=os.environ.get('CHROMIUM', '/usr/bin/chromium'), args=['--no-sandbox'])
    for width in (1440, 390, 320):
        page = browser.new_page(viewport=dict(width=width, height=1000))
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.route(API + '**', route_data)
        for name in ('index', 'verification', 'reference', 'contribute'):
            page.goto(f'{BASE}/{name}.html')
            page.evaluate('document.fonts.ready')
            assert page.locator('a[href="https://github.com/helenlkupp"]').count() == (2 if name == 'index' else 1)
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), (name, width)
        page.wait_for_function("document.querySelector('#engine-contributions').getAttribute('aria-busy') === 'false'")
        assert page.locator('#contribution-list > li').count() == 1
        assert HOSTILE in page.locator('#contribution-list h3').inner_text()
        assert 'alice@example.com' not in page.locator('#contribution-list').inner_text()
        assert page.locator('#contribution-list img, #contribution-list script').count() == 0
        assert page.evaluate('window.injected === undefined')
        assert page.locator('#contribution-list h3 a').get_attribute('href') == 'https://github.com/WomenDefiningAI/assaydb/pull/42'
        assert page.locator('#contribution-list time').inner_text() == '2026-10-14 (UTC)'
        text = page.locator('#contribution-list').inner_text()
        assert 'Some commits have no linked GitHub author account.' in text
        assert 'Private Name' not in text and 'private@example.com' not in text
        assert 'Other commit authors: @bob' in text and 'Bot commit authors: @builder[bot] (bot)' in text
        assert page.locator('a[href="https://github.com/WomenDefiningAI/assaydb/commit/' + 'a'*40 + '"]').count() == 1
        assert page.locator('a[href="https://github.com/WomenDefiningAI/assaydb/commit/' + 'b'*40 + '"]').count() == 1
        assert page.locator('#contribution-list a[href*="%5Bbot%5D"]').count() == 0
        page.locator('#engine-contributions details').evaluate('(node) => node.open = true')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        if os.environ.get('WEBSITE_SCREENSHOTS'):
            folder = Path(os.environ['WEBSITE_SCREENSHOTS']); folder.mkdir(parents=True, exist_ok=True)
            page.locator('#engine-contributions').screenshot(path=str(folder / f'contributors-{width}.png'))
        assert not errors, errors
        page.close()

    # Delayed request leaves a clear loading state; failure is not an empty result.
    page = browser.new_page()
    pending = []
    page.route(API + '**', lambda route: pending.append(route))
    page.goto(BASE + '/contribute.html')
    page.wait_for_function("document.querySelector('#contribution-status').textContent.startsWith('Checking')")
    assert page.locator('#contribution-retry').is_disabled()
    assert pending
    pending.pop().fulfill(status=403, body='{}')
    page.wait_for_function("document.querySelector('#engine-contributions').getAttribute('aria-busy') === 'false'")
    assert 'No contribution count is available' in page.locator('#contribution-status').inner_text()
    assert page.locator('#contribution-retry').is_enabled()
    page.unroute(API + '**')
    page.route(API + '**', lambda route: route.fulfill(status=200, content_type='application/json', body='[]'))
    page.locator('#contribution-retry').click()
    page.wait_for_function("document.querySelector('#contribution-status').textContent.startsWith('No merged')")
    assert page.locator('#contribution-list > li').count() == 0
    assert page.locator('#contribution-retry').is_enabled()
    page.close()

    # Bot PR authors remain labeled text, without guessed human profile URLs.
    page = browser.new_page()
    for login in ('dependabot[bot]', 'github-actions[bot]'):
        page.route(API + '**', lambda route: route.fulfill(status=200, content_type='application/json', body=json.dumps(
            [dict(filename='src/engine.rs')] if '/files?' in route.request.url else
            [] if '/commits?' in route.request.url else [{**PR, 'user': dict(login=login, type='Bot')}])) )
        page.goto(BASE + '/contribute.html')
        page.wait_for_function("document.querySelector('#engine-contributions').getAttribute('aria-busy') === 'false'")
        assert f'PR author: @{login} (bot)' in page.locator('#contribution-list').inner_text()
        assert page.locator('#contribution-list a[href*="%5Bbot%5D"]').count() == 0
        assert page.locator('#contribution-list h3 a').get_attribute('href').endswith('/pull/42')
        page.unroute(API + '**')
    page.close()

    # Unavailable/deleted accounts and partial author evidence are explicit.
    page = browser.new_page()
    def missing(route):
        if '/commits?' in route.request.url:
            route.fulfill(status=503, body='{}')
        else:
            data = [dict(filename='src/engine.rs')] if '/files?' in route.request.url else [{**PR, 'user': None}]
            route.fulfill(status=200, content_type='application/json', body=json.dumps(data))
    page.route(API + '**', missing)
    page.goto(BASE + '/contribute.html')
    page.wait_for_function("document.querySelector('#engine-contributions').getAttribute('aria-busy') === 'false'")
    text = page.locator('#contribution-list').inner_text()
    assert 'GitHub account unavailable' in text and 'commit-author list is incomplete' in text
    page.close()

    page = browser.new_page(java_script_enabled=False)
    page.goto(BASE + '/contribute.html')
    assert 'Enable JavaScript' in page.locator('#contribution-status').inner_text()
    assert page.locator('#contribution-retry').is_hidden()
    assert page.get_by_role('link', name='Browse merged pull requests').is_visible()
    browser.close()
print('PASS: four pages at 1440/390/320px; literal hostile titles; exact PR/commit/profile links; bots; loading/error/retry/empty; missing accounts; no JavaScript')
