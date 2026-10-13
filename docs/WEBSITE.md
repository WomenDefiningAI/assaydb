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
All 49 runtime files were imported. CSS, fonts, images, measurement exports, and evidence summaries preserve the
approved bytes. Chart code is unchanged except for the coverage summary link.
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

## Later static hosting

The root `vercel.json` selects no framework, skips install/build commands, and
serves only `site/` as output. Its settings follow the
[Vercel configuration reference](https://vercel.com/docs/project-configuration/vercel-json).
It contains no project or account identity. Automatic Git deployments are
disabled with `git.deploymentEnabled: false`, following the
[Git configuration reference](https://vercel.com/docs/project-configuration/git-configuration).
Enabling them requires a later, separately approved change. Connecting a host
and deploying remain separate actions.
