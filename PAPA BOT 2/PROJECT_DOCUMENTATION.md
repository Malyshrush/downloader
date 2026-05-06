# PAPA BOT - Project Documentation

## Overview

PAPA BOT is a VK automation system deployed on Yandex Cloud Functions. The project supports multiple VK communities, message and comment flows, delayed processing, profile-scoped configuration, and a web admin panel.

## Architecture

- Entry: `index.js`
- Backend router: `src/handler.js`
- Main modules live in `src/modules/`
- Admin UI is delivered from `adminPanelHTML.js`
- Persistent data is stored in Yandex Object Storage
- Large-file upload support is handled by the external `render-uploader` service

## Main Functional Areas

- messages and step flows
- comments and wall interactions
- users and groups
- variables and profile-shared variables
- mailings and delayed jobs
- structured and classic triggers
- admin profiles, promo codes, recovery, and sessions

## Admin Session Security

- admin login is stored in a server-side `admin_sessions.json` store
- browser access is bound through `adminSessionId` in an `HttpOnly` cookie
- the admin UI also sends a server-issued session token through `X-Admin-Session` as a transport fallback for direct Cloud Function requests
- suspicious IP or user-agent changes can switch the session into CAPTCHA-required mode
- login CAPTCHA and session CAPTCHA are generated server-side as SVG
- session CAPTCHA is rendered in a dedicated blocking overlay and its refresh flow is rate-limited without destroying the current visible CAPTCHA
- a successful session CAPTCHA pass resets the suspicious-session state so the next stable request does not immediately re-trigger CAPTCHA
- three failed CAPTCHA attempts terminate the active session and force CAPTCHA on the next login
- CAPTCHA flows are rate-limited independently from profile and promo limits

## Admin Profiles And Promo Codes

Admin profile fields:

- `name`
- `username`
- `password`
- `email`
- `durationMinutes`
- `requestsLimit`

Promo-code fields:

- `durationMinutes`
- `dailyRequestsLimit`

Behavior:

- only the main admin creates and edits profiles in `АДМИН`
- promo-code activation and reactivation can grant both profile lifetime and daily request limit
- the profile dashboard reads the actual profile limit instead of an independent dashboard-only value
- the `РџР РћР¤РР›Р¬` tab can activate a promo code for the current profile and allows only 3 promo-entry attempts per Moscow day
- connected communities in `РџР РћР¤РР›Р¬` are rendered as cards and the active community is highlighted

## Daily Request Limit Model

The profile dashboard exposes:

- `dailyLimit`
- `dailyUsed`
- `dailyRemaining`

Rules:

- `dailyLimit` comes from the admin profile
- once `dailyUsed >= dailyLimit`, all incoming events for communities owned by that profile stop being processed
- the daily counter resets at `00:00` Moscow time
- pending limit-increase requests can be deleted by the profile user from `ПРОФИЛЬ`

## Release And Versioning

Source of truth:

- `bot-version.json`

Current release target:

`version 002.0032.0004.0005.0004.0003.0002.0001.0001.0008.0009.0024`

Rules:

- bump the relevant block for every completed working code change
- keep `capabilities` aligned with real behavior
- keep `README.md`, `PROJECT_DOCUMENTATION.md`, and `GPT.md` aligned with the release
- completed working changes must end with deploy and post-deploy verification unless the user explicitly forbids rollout

## Deploy

Primary command:

```bash
node scripts/deploy.js
```

The deploy pipeline:

1. syncs `displayVersion`
2. creates a mandatory full project snapshot in `backup_papa_bot/<timestamp>/PAPA BOT 2`
3. prepares `dist/`
4. installs production dependencies
5. trims the deployment bundle
6. deploys a new Yandex Cloud Function version

Backup rule:

- every deploy or prepare flow must create a local project backup first
- `--skip-backup` is a legacy compatibility flag and must not bypass the mandatory project backup
- backup snapshots are stored in `backup_papa_bot/` and are ignored by git
- recursive/build-heavy folders are excluded from the snapshot: `.git`, `node_modules`, `dist`, `backups`, `backup_papa_bot`, `.claude`, `.vs`

## Required Post-Deploy Verification

After every deploy:

1. wait 15 seconds
2. fetch recent logs from Yandex Cloud
3. confirm there are no `ERROR`, `502`, `Module not found`, or `SyntaxError`
4. verify:
   - `https://functions.yandexcloud.net/d4eg37ikm3vl5tm1mjld?health`
   - `https://functions.yandexcloud.net/d4eg37ikm3vl5tm1mjld?sheet=СООБЩЕНИЯ`

Note: `?sheet=...` and `?getBotVersion` are now admin-protected. For smoke checks, either use an authenticated admin session through cookie or `X-Admin-Session`, or confirm that the unauthenticated response returns `sessionInvalid`.

## Supporting Subprojects

- `render-uploader/` - external upload service for large files
- `callback-proxy/` - helper service for callback and token-dependent operations
- `vk-token-extension/` - browser extension for obtaining VK user tokens
- `yandex-function/` - deployment-specific entrypoint and package manifest
