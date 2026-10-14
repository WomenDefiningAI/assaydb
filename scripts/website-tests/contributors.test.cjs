'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const api = require('../../site/js/contributors.js');
const user = (login = 'alice', type = 'User') => ({login, type});
const pr = (number = 2, overrides = {}) => ({number, title: 'An engine change', state: 'closed', merged_at: '2026-10-14T10:00:00Z',
  base: {ref: 'main', repo: {id: 1418037309, full_name: 'WomenDefiningAI/assaydb'}}, user: user(), ...overrides});
const commit = (login, overrides = {}) => ({sha: 'a'.repeat(40), parents: [{}], author: user(login), ...overrides});
const response = (items, next = false) => ({ok: true, json: async () => items,
  headers: {get: () => next ? '<https://untrusted.example/ignored>; rel="next"' : null}});
function fixture(entries) {
  const calls = [];
  const fetcher = async (url, options) => {
    calls.push(url);
    const parsed = new URL(url);
    assert.equal(parsed.origin, 'https://api.github.com');
    assert.ok(parsed.pathname.startsWith('/repos/WomenDefiningAI/assaydb/pulls'));
    assert.equal(options.credentials, 'omit');
    assert.equal(options.referrerPolicy, 'no-referrer');
    assert.equal(options.headers.Authorization, undefined);
    const next = entries.shift();
    assert.ok(next, 'Unexpected request');
    if (next.path) assert.equal(parsed.pathname.split('/pulls')[1], next.path);
    if (next.page) assert.equal(parsed.searchParams.get('page'), String(next.page));
    if (next.error) throw Error('Network unavailable');
    if (next.status) return {ok: false};
    return response(next.items, next.next);
  };
  return {fetcher, calls};
}

test('scope includes code, correctness, benchmark methods and proof source; excludes website/setup', () => {
  for (const path of ['src/engine.rs', 'tests/null_zero.rs', 'tests/fixtures/query.sql', 'tests/fixtures/answers.json',
    'bindings/python/src/lib.rs', 'bindings/python/python/assaydb/__init__.py', 'bindings/python/tests/test_session.py',
    'benches/join.rs', 'benchmarks/compare.py', 'benchmarks/query.sql', 'docs/BENCHMARK-INPUTS.md',
    'proofs/max.lean', 'verification/harness.rs', 'proofs/query.smt2']) assert.equal(api.enginePath(path), true, path);
  for (const path of ['site/contribute.html', 'site/js/contributors.js', 'scripts/website-tests/contributors.test.cjs',
    'vercel.json', '.github/workflows/deploy.yml', 'Cargo.toml', 'Cargo.lock', 'README.md', 'proofs/README.md',
    'benchmarks/setup.sh', 'bindings/python/scripts/stage_sdist.py', 'bindings/python/pyproject.toml', null]) assert.equal(api.enginePath(path), false, path);
});
test('only merged public main PRs with valid identifiers qualify; no title-based category', () => {
  assert.equal(api.contribution(pr(2, {title: 'Website work'})).number, 2);
  for (const overrides of [{state: 'open'}, {merged_at: null}, {base: {ref: 'other'}},
    {base: {ref: 'main', repo: {id: 1, full_name: 'WomenDefiningAI/assaydb'}}}]) assert.equal(api.contribution(pr(2, overrides)), null);
  assert.throws(() => api.contribution(pr(-1)));
  assert.throws(() => api.contribution(pr(2, {merged_at: 'invalid'})));
  assert.equal(api.contribution(pr(2, {title: 'Fix by alice@example.com'})).title, 'Fix by [email omitted]');
});
test('GitHub-linked authors only; deduplicate case-insensitively; separate bots and omit merges/committers', () => {
  const result = api.commitAuthors([commit('alice'), commit('bob'), commit('BOB'), commit('worker[bot]'),
    commit('build', {author: user('build', 'Bot')}), commit('merger', {parents: [{}, {}]}),
    commit('missing', {author: null, committer: user('committer')}), commit('bad', {sha: 'javascript:alert(1)'}), null], user());
  assert.deepEqual(result.map(a => [a.login, a.kind]), [['bob', 'user'], ['worker[bot]', 'bot'], ['build', 'bot']]);
  assert.equal(api.account({login: '<img src=x onerror=alert(1)>', type: 'User'}), null);
  assert.equal(api.account(null), null);
  assert.equal(api.account(user('unknown', 'Organization')).kind, 'unknown');
});
test('honest empty repository and website-only merged PR; imported releases are never seeded', async () => {
  assert.deepEqual(await api.load(fixture([{items: []}]).fetcher), {rows: [], partial: false});
  const f = fixture([{items: [pr(1, {title: 'Improve engine'})]}, {items: [{filename: 'site/index.html'}, {filename: 'README.md'}, {filename: 'vercel.json'}]}]);
  assert.deepEqual(await api.load(f.fetcher), {rows: [], partial: false});
  assert.equal(f.calls.length, 2);
});
test('mixed PR and renamed engine file qualify across file pages; commit pages, duplicate PRs and ordering', async () => {
  const f = fixture([
    {items: [pr(2, {title: 'Website and engine'}), pr(2)], next: true, page: 1},
    {items: [{filename: 'site/index.html'}], next: true, path: '/2/files', page: 1},
    {items: [{filename: 'archive/removed.rs', previous_filename: 'src/engine.rs'}], path: '/2/files', page: 2},
    {items: [commit('alice'), commit('bob')], next: true, path: '/2/commits', page: 1},
    {items: [commit('BOB'), commit('worker[bot]')], path: '/2/commits', page: 2},
    {items: [pr(2), pr(3, {merged_at: '2026-10-15T10:00:00Z'})], page: 2},
    {items: [{filename: 'proofs/max.lean'}], path: '/3/files'}, {items: [], path: '/3/commits'}]);
  const result = await api.load(f.fetcher);
  assert.equal(result.partial, false);
  assert.deepEqual(result.rows.map(r => r.number), [3, 2]);
  assert.deepEqual(result.rows[1].authors.map(u => u.login), ['bob', 'worker[bot]']);
  assert.equal(f.calls.length, 8);
});
test('closed-unmerged or foreign PRs do not even request files', async () => {
  const f = fixture([{items: [pr(2, {merged_at: null}), pr(3, {base: {ref: 'other'}})]}]);
  assert.deepEqual(await api.load(f.fetcher), {rows: [], partial: false});
  assert.equal(f.calls.length, 1);
});
test('HTTP, network and malformed responses never become a false zero', async () => {
  for (const entry of [{status: 403}, {status: 429}, {error: true}, {items: {}}, {items: [null]}]) {
    assert.deepEqual(await api.load(fixture([entry]).fetcher), {rows: [], partial: true});
  }
  assert.deepEqual(await api.load(fixture([{items: [pr()]}, {items: [{}]}]).fetcher), {rows: [], partial: true});
});
test('partial scan retains verified rows; missing commit data is labeled independently', async () => {
  const f = fixture([{items: [pr(2), pr(3)]}, {items: [{filename: 'tests/engine.rs'}]}, {status: 403}, {status: 403}]);
  const result = await api.load(f.fetcher);
  assert.equal(result.partial, true);
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].authorsComplete, false);
});
test('request, page, entry and time limits report incomplete results', async () => {
  assert.equal((await api.load(fixture([{items: [pr()]}]).fetcher, {requests: 1})).partial, true);
  assert.equal((await api.load(fixture([{items: [], next: true}]).fetcher, {pages: 1})).partial, true);
  const entries = [{items: [pr(2), pr(3)]}, {items: [{filename: 'src/engine.rs'}]}, {items: []}];
  assert.equal((await api.load(fixture(entries).fetcher, {entries: 1})).partial, true);
  assert.equal((await api.load(() => {throw Error('Must not request');}, {duration: 0})).partial, true);
});
test('slow responses abort and produce error fallback', async () => {
  const fetcher = (_, options) => new Promise((_, reject) => options.signal.addEventListener('abort', () => reject(Error('Aborted'))));
  assert.deepEqual(await api.load(fetcher, {timeout: 5}), {rows: [], partial: true});
});
test('GitHub file and commit endpoint caps do not imply completeness', async () => {
  const files = [{items: [pr()]}];
  for (let i = 1; i <= 30; i++) files.push({items: Array(100).fill({filename: 'site/index.html'}), next: i < 30});
  assert.deepEqual(await api.load(fixture(files).fetcher, {requests: 40}), {rows: [], partial: true});
  const commits = [{items: [pr()]}, {items: [{filename: 'src/engine.rs'}]},
    {items: Array(100).fill(commit('bob')), next: true}, {items: Array(100).fill(commit('bob')), next: true}, {items: Array(50).fill(commit('bob'))}];
  const result = await api.load(fixture(commits).fetcher);
  assert.equal(result.partial, false);
  assert.equal(result.rows[0].authorsComplete, false);
  assert.equal(result.rows[0].authors.length, 1);
});

test('unlinked non-merge authors are noted without disclosing raw identity fields', async () => {
  const f = fixture([{items: [pr()]}, {items: [{filename: 'src/engine.rs'}]},
    {items: [commit('missing', {author: null, commit: {author: {name: 'Private name', email: 'private@example.com'}}})]}]);
  const result = await api.load(f.fetcher);
  assert.equal(result.rows[0].unlinkedAuthors, true);
  assert.equal(result.rows[0].authorsComplete, true);
  assert.deepEqual(result.rows[0].authors, []);
  assert.ok(!JSON.stringify(result).includes('private@example.com'));
  assert.ok(!JSON.stringify(result).includes('Private name'));
});
