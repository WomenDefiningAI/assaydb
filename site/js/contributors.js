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
      mergedAt: pr.merged_at, author: account(pr.user), authors: [], authorsComplete: true, unlinkedAuthors: false};
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
      const item = add(list, 'li'), heading = add(item, 'h3');
      const link = add(heading, 'a', '#' + row.number + ' · ' + row.title); link.href = repoURL + '/pull/' + row.number;
      const meta = add(item, 'p', 'Merged '), date = add(meta, 'time', row.mergedAt.slice(0, 10) + ' (UTC)'); date.dateTime = row.mergedAt;
      const author = add(item, 'p', 'PR author: ');
      if (row.author) userLink(author, row.author); else add(author, 'span', 'GitHub account unavailable');
      for (const [kind, label] of [['user', 'Other commit authors'], ['bot', 'Bot commit authors'], ['unknown', 'Other accounts with commits']]) {
        const accounts = row.authors.filter(a => a.kind === kind);
        if (!accounts.length) continue;
        const line = add(item, 'p', label + ': ');
        accounts.forEach((user, i) => {
          if (i) add(line, 'span', ' · ');
          userLink(line, user);
          const evidence = add(line, 'a', ' (commit)'); evidence.href = repoURL + '/commit/' + user.sha;
        });
      }
      if (row.unlinkedAuthors) add(item, 'p', 'Some commits have no linked GitHub author account.').className = 'note';
      if (!row.authorsComplete) add(item, 'p', 'The commit-author list is incomplete or unavailable.').className = 'note';
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
    async function refresh() {
      retry.disabled = true; host.setAttribute('aria-busy', 'true');
      doc.getElementById('contribution-status').textContent = 'Checking public merged engine contributions on GitHub…';
      try { render(doc, await load(fetcher)); }
      catch (_) { render(doc, {rows: [], partial: true}); }
      finally { retry.disabled = false; host.setAttribute('aria-busy', 'false'); }
    }
    retry.addEventListener('click', refresh); await refresh();
  }
  const api = {enginePath, account, contribution, commitAuthors, load, render, enhance};
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.AssayContributors = api; enhance(root.document, root.fetch.bind(root)); }
})(typeof window === 'object' ? window : globalThis);
