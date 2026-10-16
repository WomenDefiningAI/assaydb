/* Public GitHub activity only. No tokens, PR-body HTML, or commit email fields. */
(function (root) {
  'use strict';
  const repository = 'WomenDefiningAI/assaydb';
  const repoURL = 'https://github.com/' + repository;
  const apiURL = 'https://api.github.com/repos/' + repository;
  const limits = {requests: 30, pages: 3, entries: 12, timeout: 8000, duration: 25000};

  function enginePath(path) {
    if (typeof path !== 'string') return false;
    return /^(src|tests)\/.+\.rs$/.test(path) ||
      /^tests\/fixtures\/.+\.(sql|json|tsv)$/.test(path) ||
      /^(benches|benchmarks)\/.+\.(rs|py|sql)$/.test(path) ||
      /^testing\/(candidate_boundary|collector_adapter|result_contract|runtime_context|thread_metric|timing|rotated_timing|rotated_statistics)\.py$/.test(path) ||
      /^testing\/tests\/test_(candidate_boundary|collector_adapter|result_contract|runtime_context|thread_metric|timing|public_sources|no_regression|rotated_timing)\.py$/.test(path) ||
      /^testing\/(analysis\/.+\.sql|hunt55k\/.+\.py)$/.test(path) ||
      /^(proofs|verification)\/.+\.(rs|lean|v|smt2)$/.test(path) ||
      path === 'docs/BENCHMARK-INPUTS.md' ||
      /^bindings\/python\/(src\/.+\.rs|python\/.+\.(py|pyi)|tests\/.+\.py)$/.test(path);
  }
  function account(user) {
    if (!user || typeof user.login !== 'string' ||
        !/^[a-z0-9](?:[a-z0-9-]{0,38})(?:\[bot\])?$/i.test(user.login)) return null;
    const kind = user.type === 'Bot' || /\[bot\]$/i.test(user.login) ? 'bot' :
      user.type === 'User' ? 'user' : 'unknown';
    return {login: user.login, kind};
  }
  function contribution(pr) {
    if (!pr || !Number.isSafeInteger(pr.number) || pr.number <= 0) throw Error('Invalid PR');
    if (pr.base?.repo?.id !== 1418037309 || pr.base?.repo?.full_name !== repository || pr.base?.ref !== 'main') return null;
    if (pr.state !== 'closed' || !pr.merged_at) return null;
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(pr.merged_at) || !Number.isFinite(Date.parse(pr.merged_at))) throw Error('Invalid merge date');
    if (typeof pr.title !== 'string') throw Error('Invalid title');
    return {number: pr.number, title: pr.title.replace(/[\w.+-]+@[\w.-]+\.[a-z]{2,}/gi, '[email omitted]'),
      mergedAt: pr.merged_at, mergeCommit: /^[a-f0-9]{40}$/.test(pr.merge_commit_sha) ? pr.merge_commit_sha : null, author: account(pr.user), authors: [], authorsComplete: true, unlinkedAuthors: false};
  }
  function commitAuthors(commits, author) {
    const seen = new Set(author ? [author.login.toLowerCase()] : []), result = [];
    for (const commit of commits) {
      // Merge commits and committers are not evidence of authoring the change.
      if (!commit || !Array.isArray(commit.parents) || commit.parents.length > 1 || !/^[a-f0-9]{40}$/.test(commit.sha)) continue;
      const user = account(commit.author);
      if (!user || seen.has(user.login.toLowerCase())) continue;
      seen.add(user.login.toLowerCase()); result.push({...user, sha: commit.sha});
    }
    return result;
  }
  function hasNext(response) {
    // Use only the pagination signal, never a URL supplied in a response.
    return /;\s*rel="next"/.test(response.headers.get('link') || '');
  }
  async function load(fetcher, options = {}) {
    const bounds = {...limits, ...options}, started = Date.now();
    let requests = 0, partial = false;
    const rows = [], seen = new Set();
    async function page(path, number) {
      if (++requests > bounds.requests || Date.now() - started >= bounds.duration) throw Error('Scan limit');
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), Math.min(bounds.timeout, bounds.duration - (Date.now() - started)));
      try {
        const response = await fetcher(apiURL + path + (path.includes('?') ? '&' : '?') + 'per_page=100&page=' + number,
          {headers: {Accept: 'application/vnd.github+json'}, credentials: 'omit', referrerPolicy: 'no-referrer', signal: controller.signal});
        if (!response.ok) throw Error('GitHub unavailable');
        const items = await response.json();
        if (!Array.isArray(items)) throw Error('Invalid response');
        return {items, next: hasNext(response)};
      } finally { clearTimeout(timer); }
    }
    async function touchesEngine(number) {
      // GitHub caps this endpoint at 3,000 files. Hitting the cap is incomplete.
      for (let n = 1; n <= 30; n++) {
        const data = await page('/pulls/' + number + '/files', n);
        if (data.items.some(f => !f || typeof f.filename !== 'string')) throw Error('Invalid files');
        if (data.items.some(f => enginePath(f.filename) || enginePath(f.previous_filename))) return true;
        if (!data.next) {
          if (n === 30 && data.items.length === 100) throw Error('File limit');
          return false;
        }
      }
      throw Error('File limit');
    }
    async function authors(row) {
      const commits = [];
      try {
        for (let n = 1; n <= 3; n++) {
          const data = await page('/pulls/' + row.number + '/commits', n);
          commits.push(...data.items);
          if (!data.next) {
            row.authorsComplete = commits.length < 250;
            break;
          }
          if (n === 3) row.authorsComplete = false;
        }
      } catch (_) { row.authorsComplete = false; }
      row.authors = commitAuthors(commits, row.author);
      row.unlinkedAuthors = commits.some(c => c && Array.isArray(c.parents) && c.parents.length <= 1 &&
        /^[a-f0-9]{40}$/.test(c.sha) && !account(c.author));
    }
    try {
      outer: for (let n = 1; n <= bounds.pages; n++) {
        const data = await page('/pulls?state=closed&base=main&sort=updated&direction=desc', n);
        for (let i = 0; i < data.items.length; i++) {
          const row = contribution(data.items[i]);
          if (!row || seen.has(row.number)) continue;
          seen.add(row.number);
          if (!await touchesEngine(row.number)) continue;
          await authors(row); rows.push(row);
          if (rows.length >= bounds.entries) {
            partial = data.next || i < data.items.length - 1;
            break outer;
          }
        }
        if (!data.next) break;
        if (n === bounds.pages) partial = true;
      }
    } catch (_) { partial = true; }
    rows.sort((a, b) => b.mergedAt.localeCompare(a.mergedAt) || b.number - a.number);
    return {rows, partial};
  }
  // Only explicit reviewed display data can supply metrics, never PR prose.
  function displayMetrics(data) {
    const result = new Map();
    if (!['assay-contribution-display/v1', 'assay-contribution-display/v2'].includes(data?.schema) || !Array.isArray(data.records)) return result;
    for (const row of data.records) {
      if (!row || row.status !== 'accepted' || !Number.isSafeInteger(row.pr) || row.pr < 1 ||
          !(row.evidence_commit === 'merged-pr' || /^[a-f0-9]{40}$/.test(row.evidence_commit)) || !/^[a-f0-9]{40}$/.test(row.measured_source) ||
          !(/^(site\/evidence|docs\/evidence)\/[a-zA-Z0-9_/-]+\.json$/.test(row.evidence_path) && !row.evidence_path.includes('..'))) continue;
      const a = row.accuracy, categories = ['passed','unsupported','wrong','error','not-run','excluded'];
      if (!a || a.suite !== 'SQLLogicTest' || !Number.isSafeInteger(a.applicable) || a.applicable <= 0 ||
          categories.some(key => !Number.isSafeInteger(a[key]) || a[key] < 0) ||
          categories.filter(key => key !== 'excluded').reduce((n, key) => n + a[key], 0) !== a.applicable) continue;
      const record = {accuracy: a, measuredSource: row.measured_source,
        evidencePath: row.evidence_path, source: row.evidence_commit === 'merged-pr' ? null : repoURL + '/blob/' + row.evidence_commit + '/' + row.evidence_path};
      const speed = row.speed;
      // Invalid or incomplete speed evidence never hides valid accuracy or becomes a zero.
      const sha = value => typeof value === 'string' && /^[a-f0-9]{40}$/.test(value);
      const positive = value => Number.isFinite(value) && value > 0;
      const text = value => typeof value === 'string' && value.trim().length > 0;
      if (data.schema === 'assay-contribution-display/v2' && speed?.status === 'accepted' &&
          speed.baseline_kind === 'tested-public-main' && sha(speed.baseline_source) &&
          sha(speed.candidate_source) && speed.candidate_source === row.measured_source &&
          speed.baseline_source !== speed.candidate_source && sha(speed.final_engine_equivalent_head) &&
          speed.suite === 'SF10' && speed.queries === 22 && speed.workers === 10 &&
          speed.rounds === 3 && speed.warmups === 1 && speed.timed_repetitions === 5 &&
          speed.aggregation === 'sum-query-round-medians' &&
          ['host', 'configuration', 'limits'].every(key => text(speed[key])) &&
          [speed.ratio, speed.candidate_seconds, speed.baseline_seconds].every(positive) &&
          Math.abs(speed.ratio - speed.candidate_seconds / speed.baseline_seconds) < 1e-12 &&
          Array.isArray(speed.confidence_95) && speed.confidence_95.length === 2 &&
          speed.confidence_95.every(positive) && speed.confidence_95[0] <= speed.ratio &&
          speed.ratio <= speed.confidence_95[1]) record.speed = speed;
      result.set(row.pr, record);
    }
    return result;
  }
  function render(doc, result) {
    const list = doc.getElementById('contribution-list'); list.replaceChildren();
    const add = (parent, tag, text) => {
      const node = doc.createElement(tag); if (text !== undefined) node.textContent = text;
      parent.append(node); return node;
    };
    function userLink(parent, user) {
      if (user.kind === 'bot') { add(parent, 'span', '@' + user.login + ' (bot)'); return; }
      const link = add(parent, 'a', '@' + user.login);
      link.href = 'https://github.com/' + encodeURIComponent(user.login);
      if (user.kind !== 'user') add(parent, 'span', ' (account type unknown)');
    }
    for (const row of result.rows) {
      const item = add(list, 'tr'); item.className = 'contribution-row';
      const author = add(item, 'td');
      if (row.author) userLink(author, row.author); else add(author, 'span', 'GitHub account unavailable');
      const heading = add(item, 'th'); heading.scope = 'row';
      const link = add(heading, 'a', '#' + row.number + ' · ' + row.title); link.href = repoURL + '/pull/' + row.number;
      const date = add(add(item, 'td'), 'time', row.mergedAt.slice(0, 10)); date.dateTime = row.mergedAt;
      const accuracy = add(item, 'td'), speed = add(item, 'td');
      accuracy.className = speed.className = 'contribution-value';
      const toggle = add(add(item, 'td'), 'button', 'Details');
      toggle.type = 'button'; toggle.className = 'contribution-toggle';
      toggle.setAttribute('aria-label', 'Details for PR #' + row.number);
      toggle.setAttribute('aria-expanded', 'false');
      const detailRow = add(list, 'tr'); detailRow.hidden = true; detailRow.className = 'contribution-detail-row';
      const details = add(detailRow, 'td'); details.colSpan = 6; details.id = 'contribution-details-' + row.number;
      toggle.setAttribute('aria-controls', details.id);
      toggle.addEventListener('click', () => {
        detailRow.hidden = !detailRow.hidden;
        toggle.setAttribute('aria-expanded', String(!detailRow.hidden));
      });
      const stored = result.metrics?.get(row.number);
      const evidence = stored && (stored.source || row.mergeCommit) ? {...stored, source: stored.source || repoURL + '/blob/' + row.mergeCommit + '/' + stored.evidencePath} : null;
      if (evidence) {
        const a = evidence.accuracy, fmt = n => n.toLocaleString('en-US');
        add(accuracy, 'span', (100 * a.passed / a.applicable).toFixed(4) + '%');
        add(details, 'h3', 'SQLLogicTest scope and failure breakdown');
        add(details, 'p', fmt(a.passed) + ' / ' + fmt(a.applicable) + ' applicable SQL tests passed.');
        add(details, 'p', fmt(a.wrong) + ' wrong; ' + fmt(a.unsupported) + ' unsupported; ' + fmt(a.error) + ' errors; ' + fmt(a['not-run']) + ' not run. ' + fmt(a.excluded) + ' excluded control records are outside the applicable denominator.');
        add(details, 'p', 'Measured source ' + evidence.measuredSource.slice(0, 8) + '. Retained reviewed evidence, not a fresh run of the merged PR head.');
        const link = add(details, 'a', 'Reviewed evidence and limits'); link.href = evidence.source;
      } else {
        accuracy.textContent = 'Not measured';
        add(details, 'p', 'Accuracy: no reviewed applicable-suite result.');
      }
      if (evidence?.speed) {
        const s = evidence.speed, change = 100 * (s.ratio - 1);
        add(speed, 'span', Math.abs(change).toFixed(2) + '% ' + (change < 0 ? 'less' : change > 0 ? 'more' : 'change in') +
          ' elapsed time');
        add(details, 'h3', 'Speed sources, method and limits');
        add(details, 'p', 'SF10, 22 queries, 10 workers · versus tested main ' + s.baseline_source.slice(0, 8));
        add(details, 'p', 'Candidate/main total ratio ' + s.ratio.toFixed(6) +
          '; 95% descriptive interval ' + s.confidence_95.map(n => n.toFixed(6)).join('–') +
          '. Total sums query-round medians (' + s.candidate_seconds + ' / ' + s.baseline_seconds +
          ' seconds); it is not a geometric mean or full harness runtime.');
        add(details, 'p', s.host + '; same pinned inputs and configuration for both roles: ' + s.configuration +
          '. Three rotated orders; one warmup and five timed repetitions per query/engine/round.');
        for (const [label, sha] of [['Tested candidate', s.candidate_source], ['Tested main baseline', s.baseline_source],
          ['Final engine-equivalent head (not a new run)', s.final_engine_equivalent_head]]) {
          const line = add(details, 'p', label + ': '); const link = add(line, 'a', sha.slice(0, 8));
          link.href = repoURL + '/commit/' + sha;
        }
        add(details, 'p', s.limits);
        const link = add(details, 'a', 'Reviewed evidence and qualification'); link.href = evidence.source;
        if (evidence.evidencePath === 'docs/evidence/engine-snapshot-checks.json') {
          const qualification = add(details, 'a', ' · Qualification report');
          qualification.href = evidence.source.replace('docs/evidence/engine-snapshot-checks.json', 'docs/ENGINE-SNAPSHOT-QUALIFICATION.md');
        }
      } else {
        speed.textContent = 'Not measured';
        add(details, 'p', 'Speed: not measured against the same main baseline and host/configuration.');
      }
      for (const [kind, label] of [['user', 'Other commit authors'], ['bot', 'Bot commit authors'], ['unknown', 'Other accounts with commits']]) {
        const accounts = row.authors.filter(a => a.kind === kind);
        if (!accounts.length) continue;
        const line = add(details, 'p', label + ': ');
        accounts.forEach((user, i) => {
          if (i) add(line, 'span', ' · ');
          userLink(line, user);
          const evidence = add(line, 'a', ' (commit)'); evidence.href = repoURL + '/commit/' + user.sha;
        });
      }
      if (row.unlinkedAuthors) add(details, 'p', 'Some commits have no linked GitHub author account.').className = 'note';
      if (!row.authorsComplete) add(details, 'p', 'The commit-author list is incomplete or unavailable.').className = 'note';
    }
    const status = doc.getElementById('contribution-status');
    status.textContent = result.partial ? (result.rows.length ?
      'Showing verified contributions from a limited or incomplete scan. More may be available on GitHub.' :
      'We could not complete the GitHub check. No contribution count is available; please check GitHub or try again later.') :
      (result.rows.length ? 'Merged engine contributions found in the public repository.' :
        'No merged engine contributions yet. The imported Alpha releases and website setup are not community contribution records.');
  }
  async function enhance(doc, fetcher) {
    const host = doc.getElementById('engine-contributions'); if (!host) return;
    const retry = doc.getElementById('contribution-retry'); retry.hidden = false;
    let metrics = new Map();
    try {
      const response = await fetcher('evidence/contribution-metrics.json', {credentials: 'omit', referrerPolicy: 'no-referrer'});
      if (response.ok) metrics = displayMetrics(await response.json());
    } catch (_) { /* Missing display evidence remains explicitly not measured. */ }
    async function refresh() {
      retry.disabled = true; host.setAttribute('aria-busy', 'true');
      doc.getElementById('contribution-status').textContent = 'Checking public merged engine contributions on GitHub…';
      try { render(doc, {...await load(fetcher), metrics}); }
      catch (_) { render(doc, {rows: [], partial: true}); }
      finally { retry.disabled = false; host.setAttribute('aria-busy', 'false'); }
    }
    retry.addEventListener('click', refresh); await refresh();
  }
  const api = {enginePath, account, contribution, commitAuthors, load, displayMetrics, render, enhance};
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.AssayContributors = api; enhance(root.document, root.fetch.bind(root)); }
})(typeof window === 'object' ? window : globalThis);
