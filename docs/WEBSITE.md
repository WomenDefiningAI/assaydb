# Public website

The buildless site has four pages: `index.html`, `verification.html`,
`reference.html`, and `contribute.html`. It needs no package installation,
backend, credentials, or build step. Serve `site/` as the web root so charts can
fetch their local JSON:

```sh
python3 -m http.server 8080 --bind 127.0.0.1 --directory site
```

Open http://localhost:8080/. Opening the HTML directly from disk will prevent
the interactive charts from fetching their data.

## Source and preservation

The approved website snapshot is `ce55d8c4a704ebc988685356c258aba11bea7d9b`.
Its handoff archive, `assaydb-approved-website-ce55d8c.zip`, has SHA-256
`be08b8d8224f4679d927c4909b1e4000398b93d0d186df31b2537ac59a88aa4b`.
All 49 runtime files were imported. At import, CSS, fonts, images, measurement exports, and evidence summaries preserved the
approved bytes. The contribution activity section described below adds a small CSS extension. Chart code is unchanged except for the coverage summary link.
Public-facing copy and links were updated, and `progress.json` retains only the chart inputs,
without the unused activity log and planning board.

Retain the font licenses in `site/fonts/*-OFL.txt` and the repository's existing
licenses and notices. The self-hosted fonts are Outfit, DM Sans, and JetBrains
Mono. Preserve the typography, coral accents, bordered sections, charts, and
interactive MAX teaching example when editing.

## Measurement boundaries

The evidence JSON contains compact historical summaries, not raw logs or
benchmark datasets. The six early correctness checkpoints, Alpha 2 speed run
on Apple M2 Pro, earlier Apple M4 speed results, and separate reference coverage
have distinct scopes. Their original source identifiers are provenance, not
commit links into this public export. Do not remap them to public commits or
pull request numbers.

No fresh TestMiniM4 Alpha 1 or Alpha 2 baseline has been run. The 69.66% figure
is coverage of the evaluator's test corpus, not the proportion of engine code
proved. Those runs disabled fast-engine cross-checking. The reference evaluator
and its proof workspace are not publicly released. The public packages contain
the experimental fast engine only.

Historical timing methods are retained in the collapsed section on the engine
page. Timing and reference-coverage links resolve to bundled JSON measurement
summaries. These summaries are not raw logs. The site does not depend on the
former assaydb.org performance, component-results, or receipt-viewer pages.

## Validation when editing

- Run `node --check` on every file in `site/js/` and parse each JSON file.
- Check local asset paths, page anchors, external links, and publication privacy.
- Serve the site over HTTP and review all four pages at desktop and mobile widths.
- Switch both performance tabs, select build and coverage points with arrow keys,
  and toggle positive and negative MAX inputs. Verify the no-JavaScript fallback.
- Keep all historical values, denominators, losses, caveats, and license notices.

## Static hosting

The Vercel project uses Root Directory `site` and production branch `main`.
Keep those project settings in place; they are not set by `vercel.json`.
The configuration lives at `site/vercel.json`, in the project's root directory:

- `framework: null` selects Other.
- Empty `installCommand` and `buildCommand` values skip installation and building.
- `outputDirectory: "."` serves the static files directly from `site/`.

No routing or header overrides are added. Existing HTML paths, assets and JSON
remain unchanged. See Vercel's
[configuration reference](https://vercel.com/docs/project-configuration/vercel-json)
and [static build and Root Directory guidance](https://vercel.com/docs/builds/configure-a-build).

The former repository-root configuration, including `git.deploymentEnabled: false`,
has been removed to eliminate contradictory configuration. It has not been
established whether that flag suppressed deployments with Root Directory `site`;
this cleanup is not a proven fix for the stale production deployment.

GitHub deployment records show a successful Vercel production deployment for
public main `e4c616fd1168115308e021615c915fe2bb26a43d`. The Git integration can
deploy reviewed website content from main without a separate manual site update.
For each promotion, verify the deployment commit and live content after merge;
a successful historical deployment does not prove a later deployment succeeded.
These repository changes do not alter Vercel dashboard or GitHub settings.

## Public contribution activity

The contribution page reads the unauthenticated public GitHub REST API for
`WomenDefiningAI/assaydb` (repository ID `1418037309`) when opened or when a reader
chooses **Check again**. It has no token, backend, build-time snapshot, persistent
cache, scheduled job, or automatic polling. It works when `site/` is the hosting
root. This feature does not change the hosting configuration.

Only closed PRs with a merge timestamp into this repository's `main` can appear.
Changed files must match at least one of these explicit paths:

- `src/**/*.rs`, `tests/**/*.rs`, or `tests/fixtures/**/*.{sql,json,tsv}`;
- `bindings/python/src/**/*.rs`, `bindings/python/python/**/*.{py,pyi}`, or
  `bindings/python/tests/**/*.py`;
- `benches/**/*.{rs,py,sql}`, `benchmarks/**/*.{rs,py,sql}`, or the existing
  benchmark-method document `docs/BENCHMARK-INPUTS.md`;
- `proofs/**/*.{rs,lean,v,smt2}` or `verification/**/*.{rs,lean,v,smt2}`.

Here `**/*` also includes files directly inside that directory. The latter
benchmark/proof directories reserve explicit locations for future public work;
they do not assert that the separate reference evaluator has been released.
Renamed files qualify by their old or new path. These are transparent path-based
criteria, not an automated judgment of a change's scientific value. A mixed PR
with a qualifying file can appear. Titles do not determine eligibility. Routine
website, styling, deployment, release setup, manifests alone, build scripts and
unmerged PRs do not qualify. The imported Alpha commits have no public PR records
and are never seeded into the list. New relevant paths need an explicit update to
this allowlist and its tests.

Each entry shows its exact public PR, title, UTC merge date, and GitHub PR author.
Additional authors appear only from GitHub-linked accounts on non-merge commits
in that PR, with a commit link as evidence. Accounts are deduplicated without
regard to letter case. Bots are labeled and grouped separately from human commit
authors. Bot names are plain text, with commit evidence links where available;
we do not guess human profile URLs for app accounts. Reviews, comments, committers, raw author names/emails and coauthor
trailers are not used to infer collaboration. Deleted/unlinked accounts and
incomplete commit-author scans are labeled explicitly. This is attribution for a
PR, not a claim that each author wrote every eligible file. Merged-PR credit does
not certify correctness or accept benchmark results. Future chart updates must
use reviewed, accepted measurements under the
[planned result contract](CONTRIBUTION-RESULTS.md); existing historical JSON is unchanged.

The scan requests up to three 100-item pages of closed PRs ordered by most recent
update, then sorts qualifying results by merge date. It displays up to 12 entries
and stops after 30 requests or 25 seconds (8 seconds per request). It follows file
and commit pagination using only fixed GitHub API URLs, respecting the API's
3,000-file and 250-commit caps. Limits and failures produce an incomplete/error
message, not a false zero; verified entries remain visible. A complete scan with
no qualifying PRs shows an honest empty state. The fallback GitHub link includes
all merged PRs, including website work. It is available without JavaScript.

Unauthenticated GitHub access is limited to 60 requests per hour per originating
IP address, shared with other anonymous clients on that IP, and depends on GitHub availability.
A page load normally needs one PR-list request, one file request per merged PR,
and commit requests for qualifying PRs. Retry is manual. Live results can change
between paginated requests; this is recent activity, not an exhaustive ledger.
See the official [pull request API](https://docs.github.com/en/rest/pulls/pulls)
and [rate limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).

All titles are rendered with DOM `textContent`, never HTML. API-provided URLs,
PR bodies, email/name metadata and scripts are not used. Email-like strings are
omitted from titles. Profile and evidence URLs use validated identifiers and
fixed public GitHub prefixes. Requests omit credentials and the page referrer.

Run dependency-free selection/pagination/error tests with:

```sh
node --test scripts/website-tests/contributors.test.cjs
```

With Python Playwright and Chromium already installed, serve `site/` as above,
then run browser checks (fixture responses only; they do not call GitHub):

```sh
python3 scripts/website-tests/contributors.browser.py
```

`CHROMIUM` overrides `/usr/bin/chromium`; `WEBSITE_TEST_URL` overrides the local
server URL. Optional `WEBSITE_SCREENSHOTS` writes desktop/mobile section captures.
Browser tests cover all four pages, profile links, responsive overflow, XSS,
loading, empty/error/retry states, unavailable accounts, bot attribution and the
no-JavaScript fallback. Synthetic contribution records exist only in tests, never
in the shipped site's data.

## Source snapshot evidence

The contribution page links to `site/evidence/source-snapshot-validation.json`.
This compact record is generated from scoped checks, reviewed and committed in
the source promotion PR. Vercel deploys it as static repository content. The
contributor list separately reads GitHub at page load; it does not generate test
results or automatically accept measurements. No chart values change in this
promotion. See [snapshot provenance](SNAPSHOT-PROMOTION.md).

## Accuracy and speed journeys

`js/journeys.js` progressively enhances the two journey panels and milestone
lessons. Arrow keys, Home/End, focus, hover and tap work; all content remains
visible without JavaScript. The accuracy receipt is unchanged: 99.9293% means
5,935,512 / 5,939,714 applicable records, or 79.9790% of 7,421,339 fixed records.
Its measured source is not the published Alpha 2 source; do not relabel it without
source-equivalence evidence.

Speed bars reuse the public historical M2 Pro receipt and reviewed PR5 receipt.
They are separate host/source/configuration groups, not a controlled continuous
trend. Both total time and geometric mean must reach the 0.95× Polars goal;
no Alpha 3 achievement is claimed. New draft PR6 contributes no measurements.
`evidence/speed-journey-notes.json` contains six dated numeric development-gate
milestones and a separate provisional M4 attempt. `scripts/render_speed_journey.py`
renders the static chart and lessons from that sanitized data. Recorded dates are
proposal timestamps, not measurement completion. Single-round retained-reference
diagnostics stay separate from rotated-protocol results and other hosts.

The contribution page links to plain GitHub issue creation (issues enabled,
discussions disabled) and the existing `wrong-answer.md` template. Its accuracy and speed
metrics are documented in `CONTRIBUTION-RESULTS.md`. PR5 shows reviewed applicable
SQLLogicTest accuracy and failures; speed remains Not measured because the retained
historical comparison is not a tested public-main baseline. The full-suite table remains generated by `testing/site_evidence.py`;
its horizontal scroll regions keep all values available on narrow screens.
