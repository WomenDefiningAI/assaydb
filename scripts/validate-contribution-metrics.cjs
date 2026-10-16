/* Run from the trusted base checkout. Candidate Git blobs are data, never code. */
'use strict';
const {execFileSync} = require('node:child_process');
const {createHash} = require('node:crypto');
const {enginePath, displayMetrics} = require('../site/js/contributors.js');
const METRICS = 'site/evidence/contribution-metrics.json';
function validate({base, head, pr, cwd = process.cwd()}) {
  const need = (ok, message) => { if (!ok) throw Error(message); };
  const sha = x => typeof x === 'string' && /^[0-9a-f]{40}$/.test(x);
  need(sha(base) && sha(head) && Number.isSafeInteger(pr) && pr > 0, 'Invalid invocation');
  const git = (...args) => execFileSync('git', args, {cwd, encoding: 'utf8'});
  const read = (rev, path) => git('show', `${rev}:${path}`);
  const changed = (a,b) => git('diff','--name-only','--no-renames','-z',a,b).split('\0').filter(Boolean);
  const qualifies = path => enginePath(path) || /^(Cargo\.(toml|lock)|build\.rs|rust-toolchain(\.toml)?|\.cargo\/|vendor\/)/.test(path);
  if (!changed(base,head).some(qualifies)) return 'No qualifying engine/testing/build changes; full maintainer merge policy still applies.';
  const data = JSON.parse(read(head,METRICS));
  need(data.records.filter(r => r?.pr === pr).length === 1, 'Include exactly one reviewed metrics record for this PR before merge');
  const row = data.records.find(r => r?.pr === pr), review = row.review;
  need(displayMetrics(data).get(pr)?.accuracy, 'Reviewed valid accuracy is required');
  need(displayMetrics(data).get(pr)?.speed, 'Reviewed paired speed is required; agree method support before merge');
  need(row.evidence_commit === 'merged-pr', 'Use merged-pr evidence binding, not a future commit SHA');
  need(review && /^[a-zA-Z0-9-]{1,39}$/.test(review.reviewer) && review.decision === 'accepted', 'Maintainer review record required');
  need(sha(review.audited_head) && sha(row.measured_source), 'Missing audited/tested source');
  need(row.speed.baseline_source === base, 'Tested baseline must match current PR base; recollect/review after base changes');
  need(row.speed.final_engine_equivalent_head === review.audited_head, 'Final equivalent identity must be the audited pre-record head');
  git('merge-base','--is-ancestor',row.measured_source,review.audited_head);
  git('merge-base','--is-ancestor',review.audited_head,head);
  // Only this PR's metrics record can be appended after review. No circular self hash.
  need(changed(review.audited_head,head).every(p => p === METRICS), 'Head changed after audit outside metrics record; repeat review');
  const before = JSON.parse(read(review.audited_head,METRICS));
  const without = d => ({...d, records: d.records.filter(r => r?.pr !== pr)});
  need(JSON.stringify(without(before)) === JSON.stringify(without(data)), 'Do not change other records or schema after audit');
  const delta = changed(row.measured_source,review.audited_head);
  const inert = p => !qualifies(p) && (/^docs\/.+\.(md|json)$/.test(p) ||
    /^site\/.+\.(html|css|js|json|svg|png)$/.test(p) || p === 'CONTRIBUTING.md');
  need(delta.every(inert), 'Execution-affecting changes since tested source require a new run');
  need(Array.isArray(review.audited_delta) && review.audited_delta.length === delta.length, 'Record every audited nonengine change');
  const inventory = delta.map(path => ({path, entry: git('ls-tree',review.audited_head,'--',path).trim()}));
  need(JSON.stringify(review.audited_delta) === JSON.stringify(inventory), 'Audited paths/blob IDs/modes do not match');
  need(read(head,row.evidence_path) === read(review.audited_head,row.evidence_path), 'Evidence changed after audit');
  const evidence = read(head,row.evidence_path);
  need(createHash('sha256').update(evidence).digest('hex') === review.evidence_sha256, 'Reviewed evidence digest mismatch');
  need(!row.evidence_path.includes('..'), 'Invalid evidence path');
  // The source JSON must carry explicit identities as well as the display record.
  const source = JSON.parse(evidence);
  need(source.tested_source === row.measured_source && source.baseline_source === base, 'Evidence source binding mismatch');
  const measuredSpeed = {...row.speed}; delete measuredSpeed.final_engine_equivalent_head;
  need(JSON.stringify(source.accuracy) === JSON.stringify(row.accuracy) &&
    JSON.stringify(source.speed) === JSON.stringify(measuredSpeed), 'Display values must match reviewed source evidence');
  return 'Reviewed metrics and audited source binding are present. Human acceptance/authenticity and full independent qualification remain required.';
}
module.exports = {validate};
if (require.main === module) {
  try { console.log(validate({base:process.argv[2],head:process.argv[3],pr:Number(process.argv[4])})); }
  catch (error) { console.error('Contribution metrics: ' + error.message); process.exitCode = 1; }
}
