'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const {execFileSync} = require('node:child_process'), {createHash} = require('node:crypto');
const {validate, executionPath} = require('../validate-contribution-metrics.cjs');
const sourceData = require('../../site/evidence/contribution-metrics.json');
function fixture(t, changedExecution = false) {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(),'metrics-gate-'));
  t.after(()=>fs.rmSync(cwd,{recursive:true,force:true}));
  const git = (...a)=>execFileSync('git',a,{cwd,encoding:'utf8'}).trim();
  const write = (p,data)=>{fs.mkdirSync(path.dirname(path.join(cwd,p)),{recursive:true});fs.writeFileSync(path.join(cwd,p),data);};
  const commit=()=>{git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','fixture');return git('rev-parse','HEAD');};
  git('init','-q'); const initial={...structuredClone(sourceData),records:[]};
  write('site/evidence/contribution-metrics.json',JSON.stringify(initial));write('src/lib.rs','base');const base=commit();
  write('src/lib.rs','candidate');const candidate=commit();
  const row=structuredClone(sourceData.records[1]);row.pr=99;row.evidence_commit='merged-pr';row.evidence_path='docs/evidence/pr99.json';row.measured_source=candidate;
  row.speed.candidate_source=candidate;row.speed.baseline_source=base;
  const speed={...row.speed};delete speed.final_engine_equivalent_head;
  const evidence=JSON.stringify({tested_source:candidate,baseline_source:base,accuracy:row.accuracy,speed});
  write(row.evidence_path,evidence);if (changedExecution) write('src/lib.rs','untested');const audited=commit();row.speed.final_engine_equivalent_head=audited;
  row.review={reviewer:'fixture-maintainer',decision:'accepted',audited_head:audited,
    audited_delta:[{path:row.evidence_path,entry:git('ls-tree',audited,'--',row.evidence_path)}],
    evidence_sha256:createHash('sha256').update(evidence).digest('hex')};
  const save=()=>{write('site/evidence/contribution-metrics.json',JSON.stringify({...initial,records:[row]}));return commit();};
  return {cwd,base,candidate,row,write,commit,save};
}
test('qualifying PR requires reviewed metrics; complete noncircular audited record passes',t=>{
  const f=fixture(t);
  assert.throws(()=>validate({...f,head:f.candidate,pr:99}),/exactly one reviewed metrics/);
  assert.match(validate({...f,head:f.save(),pr:99}),/present/);
});
test('missing speed, mismatched evidence digest, stale base and audit cannot pass',t=>{
  for(const change of [r=>r.speed=null,r=>r.review.evidence_sha256='0'.repeat(64),r=>r.review.audited_delta=[],r=>r.speed.baseline_source='a'.repeat(40)]) {
    const f=fixture(t);change(f.row);assert.throws(()=>validate({...f,head:f.save(),pr:99}));
  }
});
test('execution change after audit requires a fresh review/run',t=>{
  const f=fixture(t);f.save();f.write('src/lib.rs','changed after audit');
  assert.throws(()=>validate({...f,head:f.commit(),pr:99}),/changed after audit/);
});
test('website-only PR does not invent engine metrics or waive full merge policy',t=>{
  const f=fixture(t);f.write('site/index.html','page');const head=f.commit();
  assert.match(validate({...f,base:f.candidate,head,pr:100}),/No qualifying/);
});

test('execution changes before audit cannot be allowlisted as presentation',t=>{
  const f=fixture(t,true);assert.throws(()=>validate({...f,head:f.save(),pr:99}),/Execution-affecting/);
});

test('execution classifier covers infrastructure and data without a filename allowlist', () => {
  for (const file of [
    'testing/runtime_launch.py', 'testing/runtime_launch.sh', 'testing/controller/new_runner.py',
    'testing/collectors/new_collector.py', 'testing/tests/test_runtime_launch.py',
    'testing/source-manifest.json', 'testing/slt/full-scope.json', 'testing/policies/new.json',
    'testing/analysis/sql-manifest.json', 'testing/fixtures/answers.txt',
    'scripts/with_engine_activity.py', 'scripts/with_release_paths.py',
    'scripts/window_ranking_reference.py', 'scripts/new_collector.sh',
    'tools/testing/collector.py', 'controller/run.py', 'harness/config.toml',
    'collectors/run.py', 'tests/fixtures/expected.csv', 'bindings/python/pyproject.toml',
    '.github/workflows/engine-tests.yml', '.python-version', 'requirements-test.txt',
    'Cargo.lock', 'docs/BENCHMARK-INPUTS.md'
  ]) assert.equal(executionPath(file), true, file);
  for (const file of [
    'site/js/contributors.js', 'site/evidence/contribution-metrics.json',
    'scripts/website-tests/contributors.browser.py', 'scripts/website-tests/metrics-premerge.test.cjs',
    'scripts/render_speed_journey.py', 'scripts/validate-contribution-metrics.cjs',
    'testing/site_evidence.py', 'testing/tests/test_site_evidence.py',
    '.github/workflows/contribution-metrics.yml', 'testing/README.md',
    'docs/CONTRIBUTION-WORKFLOW.md', 'docs/evidence/accepted.json', 'CONTRIBUTING.md'
  ]) assert.equal(executionPath(file), false, file);
});
test('infrastructure-only PRs fail missing metrics rather than returning No qualifying', t => {
  for (const file of [
    'testing/runtime_launch.py', 'testing/runtime_launch.sh', 'testing/controller/run.py',
    'testing/collectors/new.py', 'testing/policy.json', 'testing/tests/test_new_collector.py',
    'scripts/with_engine_activity.py', 'tools/testing/harness.py'
  ]) {
    const f = fixture(t); f.write(file, 'changed execution input');
    assert.throws(() => validate({...f, base:f.candidate, head:f.commit(), pr:100}),
      /exactly one reviewed metrics/, file);
  }
});
test('pure website, docs and display tooling remain exempt from engine metric records', t => {
  const f = fixture(t);
  for (const file of ['site/index.html', 'testing/site_evidence.py',
    'testing/tests/test_site_evidence.py', 'scripts/render_speed_journey.py',
    'scripts/website-tests/new.test.cjs', 'docs/guide.md', 'testing/README.md']) f.write(file, 'presentation only');
  assert.match(validate({...f, base:f.candidate, head:f.commit(), pr:100}), /No qualifying/);
});
test('removing a harness also requires metrics even when no engine source changes', t => {
  const f = fixture(t); const file = 'testing/collectors/old.py';
  f.write(file, 'old collector'); const base = f.commit();
  fs.unlinkSync(path.join(f.cwd,file));
  assert.throws(() => validate({...f, base, head:f.commit(), pr:100}), /exactly one reviewed metrics/);
});
