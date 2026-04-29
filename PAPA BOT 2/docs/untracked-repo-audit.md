# Untracked Repo Audit

Date: `2026-04-29`

This audit records the large `git status --short` untracked set around `C:\PROJECT\GPT` so it is no longer an implicit problem.

## Safe generated or local-only noise

These are good candidates for ignore rules or local cleanup:

- `.claude/`
- `.vs/`
- `backups/`
- `dist/`
- `last_deploy*.txt`
- `logs_temp*.txt`
- `tmp-write-test*.txt`
- `autosetup_temp.txt`
- `*.bak`
- `bot-version-readable.tmp.json`

## Likely real project content

These should not be mass-deleted or blindly ignored because they look like source, docs, runtime code, or deployment assets:

- `src/`
- `tests/`
- `scripts/`
- `docs/`
- `callback-proxy/`
- `render-uploader/`
- `vk-token-extension/`
- `yandex-function/src/`
- `package.json`
- `package-lock.json`
- `Dockerfile`
- `DEPLOY.md`
- `PROJECT_DOCUMENTATION.md`
- `REBUILD_TO_1000_COMMUNITIES.md`

## Duplicate-looking content

There are also repeated paths both inside `PAPA BOT 2` and one level above it, for example:

- `adminPanelHTML.js`
- `attachments_backup_working.js`
- `attachments_old.js`
- `bot-version.json`
- `PROJECT_DOCUMENTATION.md`
- `docs/`
- `scripts/`
- `src/`
- `tests/`
- `yandex-function/`

This looks like parent-repo drift rather than simple temp noise. It needs a deliberate repo-structure decision:

1. keep only root copies
2. keep only `PAPA BOT 2` copies
3. split into separate tracked projects

## Recommended next cleanup order

1. Ignore or delete only confirmed generated/local noise.
2. Decide canonical location for duplicated source trees.
3. Only after that, stage or remove remaining real files intentionally.

## Not done automatically

I did not mass-delete or mass-ignore the remaining untracked set because it contains plausible source-of-truth files and duplicate trees, not just temp output.
