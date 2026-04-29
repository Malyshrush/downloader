# Admin Session CAPTCHA And IP Risk Design

Date: 2026-04-21
Project: PAPA BOT admin panel
Status: Draft for review

## Goal

Add server-enforced admin session protection for the whole admin panel with:

- `HttpOnly` cookie-based admin sessions
- session timeout after inactivity
- risk-based IP and device change detection
- local server-generated SVG CAPTCHA
- forced CAPTCHA when a session becomes suspicious
- forced logout after 3 failed CAPTCHA attempts
- CAPTCHA on the login screen after a security incident
- security event logging
- rate limiting for login and CAPTCHA flows

The design must preserve the current multi-profile admin model and must not rely on frontend-only checks.

## Why The Current Model Is Not Enough

Current admin access is effectively identified by `principalProfileId` sent from the frontend. Backend validates the profile state and role, but there is no strong server session binding. Because of that:

- IP-bound protection implemented only in the frontend would be bypassable
- there is no durable session state for tracking suspicious environment changes
- logout and security escalation cannot be enforced reliably on the server

This feature must therefore start with a real server session model.

## Recommended Approach

Use a risk-scored session model.

The backend creates and owns an admin session. Every admin request is evaluated against the session state. If the environment change looks suspicious, the backend blocks privileged actions and requires CAPTCHA. Only after successful CAPTCHA does the backend accept the new IP and continue the same session. If CAPTCHA fails 3 times, the backend kills the session and forces a new login, with CAPTCHA also required there.

This is preferred over strict IP binding because many real users sit behind dynamic IPs, mobile networks, or VPNs. The chosen design reduces false positives while still raising friction during suspicious session drift.

## Scope

In scope:

- admin login session creation
- `HttpOnly` cookie storage for `adminSessionId`
- per-request admin session validation
- suspicious IP and user-agent change detection
- SVG CAPTCHA generation and verification
- blur-lock screen in the admin panel
- CAPTCHA-protected re-entry after suspicious session change
- logout after repeated CAPTCHA failure
- login CAPTCHA after security incident
- session timeout after inactivity
- rate limits for login and CAPTCHA
- security event logging

Out of scope:

- third-party CAPTCHA providers
- MFA by email or SMS
- device fingerprinting beyond IP and user-agent
- persistent trusted devices across sessions

## Security Model

### Session Identity

After successful username/password login, the backend generates a random `adminSessionId` and stores it in a server-side session record. The response sets:

- cookie name: `adminSessionId`
- `HttpOnly`
- `SameSite=Lax`
- `Path=/`
- `Secure` enabled if runtime access is confirmed to be HTTPS-only end-to-end

Frontend JavaScript must not read the session identifier. All admin fetches must use `credentials: 'include'`.

### Session Timeout

Session lifetime is inactivity-based:

- timeout: 12 hours of inactivity
- `lastSeenAt` is updated on valid non-blocked admin requests
- once expired, the session is invalidated server-side and the cookie is cleared

### Risk Scoring

For every admin request, backend calculates a risk score:

- new IP compared with `lastVerifiedIp`: `+2`
- new `User-Agent` compared with `lastUserAgent`: `+2`
- IP changed less than 15 minutes after last verified activity: `+2`
- prior suspicious environment changes in the same session: `+1` each, capped at `+3`

Thresholds:

- score `0-2`: request allowed
- score `3+`: request blocked pending CAPTCHA

This gives low sensitivity to harmless drift and high sensitivity to sudden multi-signal changes.

### CAPTCHA Rules

Two related states exist:

- `session CAPTCHA`: user is already logged in, but request is blocked until CAPTCHA is solved
- `login CAPTCHA`: login form requires CAPTCHA before allowing authentication

CAPTCHA failure policy:

- one challenge allows up to 3 answer attempts
- after 3 failed attempts in session CAPTCHA:
  - current session is killed
  - cookie is cleared
  - frontend returns to login screen
  - next login requires CAPTCHA

### Rate Limits

Rate limiting is separate from current profile/promo limits.

Login rate limits:

- by `username + IP`
- preserve the existing failed login lock behavior
- extend it so repeated abuse from one IP cannot freely rotate usernames

CAPTCHA rate limits:

- answer submit cooldown: 1 request every 2 seconds
- challenge refresh cooldown: 1 request every 2 seconds
- max 10 CAPTCHA operations in 10 minutes per `sessionId` or fallback `IP`

If rate limit is exceeded, backend returns a temporary block response instead of issuing unlimited new challenges.

## Data Model

## File: `admin_sessions.json`

Store sessions separately from `admin_security.json`.

Proposed shape:

```json
{
  "sessions": {
    "sess_xxx": {
      "sessionId": "sess_xxx",
      "profileId": "2",
      "createdAt": "2026-04-21T10:00:00.000Z",
      "lastSeenAt": "2026-04-21T10:05:00.000Z",
      "lastVerifiedIp": "203.0.113.10",
      "lastUserAgent": "Mozilla/5.0 ...",
      "captchaRequired": false,
      "captchaFailCount": 0,
      "suspiciousChangeCount": 0,
      "captchaChallenge": {
        "hash": "",
        "expiresAt": "",
        "attempts": 0,
        "mode": "session"
      },
      "loginCaptchaRequired": false,
      "loginCaptchaFailCount": 0,
      "terminatedAt": null,
      "terminateReason": ""
    }
  }
}
```

Notes:

- session record remains authoritative even if cookie exists
- expired or terminated sessions are rejected even if cookie is presented
- cleanup can prune old sessions periodically or on write

## File: `admin_security.json`

Keep current fields and extend with:

```json
{
  "securityEvents": [],
  "loginRateLimits": {},
  "captchaRateLimits": {}
}
```

Security events must be append-only with retention limit similar to current login log retention.

## Request Context Extraction

The backend must normalize client context from the request:

- IP from `x-forwarded-for` first item, fallback to other headers when needed
- user agent from `user-agent`

Normalization rules:

- trim whitespace
- lowercase only where appropriate
- do not store full forwarded chain; keep the resolved client IP only

## Backend Components

### 1. New module: `src/modules/admin-sessions.js`

Responsibilities:

- load/save `admin_sessions.json`
- create session
- read session by cookie
- update last activity
- compute risk score
- create and store CAPTCHA challenge
- verify CAPTCHA challenge
- terminate session
- clear expired sessions

Key functions:

- `createAdminSession({ profileId, ip, userAgent })`
- `getAdminSession(sessionId)`
- `validateAdminSessionRequest(event)`
- `requireCaptchaForSession(session, reason)`
- `generateSessionCaptcha(sessionId, mode)`
- `verifySessionCaptcha({ sessionId, answer, ip, userAgent })`
- `killAdminSession(sessionId, reason)`
- `touchAdminSession(sessionId, ip, userAgent)`
- `shouldRequireLoginCaptcha({ username, ip })`

### 2. Existing module: `src/modules/admin-security.js`

Responsibilities extended:

- security event log append helpers
- rate limit helpers for login and CAPTCHA
- optional shared helpers for Moscow-day locking if reused

New helper examples:

- `appendSecurityEvent({ type, profileId, sessionId, ip, userAgent, reason, meta })`
- `checkLoginRateLimit({ username, ip })`
- `registerLoginRateLimitHit({ username, ip, success })`
- `checkCaptchaRateLimit({ sessionId, ip, action })`
- `registerCaptchaRateLimitHit({ sessionId, ip, action })`

### 3. Existing module: `src/handler.js`

Responsibilities changed:

- login now creates cookie-backed session
- all admin routes validate cookie session
- blocked routes return CAPTCHA-required response when appropriate
- logout route clears current session
- add CAPTCHA endpoints

## HTTP/API Design

## Login

### `POST ?loginAdmin`

Input:

```json
{
  "username": "admin",
  "password": "secret",
  "captchaAnswer": "AB12C"
}
```

Behavior:

- check login rate limit
- if login CAPTCHA required, validate CAPTCHA first
- verify credentials
- create server session
- set `Set-Cookie: adminSessionId=...`
- return profile metadata, not token text

Success response:

```json
{
  "success": true,
  "profileId": "2",
  "principalProfileId": "2",
  "profileName": "Profile Name",
  "role": "admin",
  "isMainAdmin": false
}
```

No fake `token: authenticated_*` should remain after migration.

## Logout

### `POST ?logoutAdmin`

Behavior:

- invalidate current session
- clear cookie
- return success

## Session validation

### `GET ?validateSession`

Behavior:

- read `adminSessionId` from cookie
- if missing/invalid/expired: `sessionInvalid`
- if suspicious and CAPTCHA pending: `captchaRequired`
- if valid: return current profile/role

Example blocked response:

```json
{
  "success": false,
  "sessionInvalid": false,
  "captchaRequired": true,
  "reason": "ip_changed_suspicious"
}
```

## CAPTCHA fetch

### `GET ?getCaptcha`

Modes:

- `mode=login`
- `mode=session`

Behavior:

- create new challenge if rate limits allow
- return SVG markup or image payload plus expiry metadata

Example:

```json
{
  "success": true,
  "mode": "session",
  "captchaSvg": "<svg ...>...</svg>",
  "expiresAt": "2026-04-21T10:10:00.000Z",
  "remainingAttempts": 3
}
```

## CAPTCHA verify

### `POST ?verifyCaptcha`

Input:

```json
{
  "mode": "session",
  "answer": "AB12C",
  "username": "admin"
}
```

Behavior for `session` mode:

- validate current cookie session
- check challenge expiry
- compare answer hash
- on success:
  - set `lastVerifiedIp`
  - set `lastUserAgent`
  - clear session CAPTCHA flags
- on 3 failures:
  - kill session
  - clear cookie

Behavior for `login` mode:

- validate login challenge bound to username/IP or anonymous login context
- on success allow login flow to continue

## Existing admin routes

Routes such as:

- `?getSettings`
- `?sheet=...`
- `?getBotSettings`
- `?getBotVersion`
- `?getAdminProfiles`
- `?getAdminDashboard`
- `?getProfileDashboard`
- `?saveAdminProfile`
- `?deleteAdminProfile`
- `?savePromoCode`
- `?resolveRecovery`
- and other admin POST routes

must stop trusting `principalProfileId` alone.

New rule:

- cookie session establishes identity
- `principalProfileId` may remain only as an explicit target/context hint for current UI code
- authorization decisions come from the session-bound profile

## Frontend Behavior

## Login screen

Current login flow in `adminPanelHTML.js` must be updated:

- all fetch requests use `credentials: 'include'`
- no auth token is stored in `localStorage`
- profile metadata may still be stored client-side for UI convenience, but not as proof of auth

When backend returns `loginCaptchaRequired`:

- show SVG CAPTCHA in the login form
- require answer before allowing login submission

## Admin panel lock overlay

When any admin request returns `captchaRequired`:

- blur the whole admin panel
- show modal/overlay above all content
- block interaction until CAPTCHA is passed
- allow challenge refresh if rate limit permits

After success:

- overlay disappears
- the blocked action can be retried or the page state can be revalidated

After 3 failures:

- clear client-side profile state
- show login form
- login form displays CAPTCHA requirement

## UI states

Recommended flags:

- `auth-required`
- `captcha-lock`
- `login-captcha-required`

## Error Handling

The system must respond explicitly for:

- missing cookie session
- expired session
- terminated session
- rate-limited login
- rate-limited CAPTCHA
- CAPTCHA challenge expired
- CAPTCHA required because of suspicious session change

Responses must be structured and machine-readable so the frontend does not rely on string matching.

Suggested response flags:

- `success`
- `sessionInvalid`
- `sessionExpired`
- `captchaRequired`
- `loginCaptchaRequired`
- `rateLimited`
- `lockUntil`
- `errorCode`
- `error`

## Logging

Security events to log:

- `session_created`
- `ip_changed`
- `captcha_required`
- `captcha_passed`
- `captcha_failed`
- `session_killed`
- `session_expired`
- `login_captcha_required`
- `login_rate_limited`

Every security event should capture:

- timestamp
- profileId when known
- sessionId when known
- resolved IP
- normalized user agent
- reason
- small metadata object

## Testing Strategy

### Unit tests

Add tests for:

- risk score calculation
- session timeout
- CAPTCHA generation and expiry
- CAPTCHA 3-fail kill flow
- login CAPTCHA requirement flow
- rate-limit counters

### Integration tests

Add tests for:

- login creates session and cookie
- normal admin request passes
- suspicious request triggers CAPTCHA
- successful CAPTCHA updates verified IP
- failed CAPTCHA 3 times kills session
- login after session kill requires CAPTCHA

### Regression checks

Verify that existing flows still work:

- main admin dashboard loading
- profile creation/edit/delete
- promo code operations
- profile dashboard
- app logs
- version panel

## Migration Notes

Existing frontend code currently sends `principalProfileId` in many places. During migration:

- keep sending it temporarily if it simplifies UI context
- backend should ignore it for authentication
- backend should derive actor identity from session

This allows incremental frontend migration without immediately rewriting every UI assumption.

## Files Expected To Change

- `C:\PROJECT\GPT\src\handler.js`
- `C:\PROJECT\GPT\src\modules\admin-security.js`
- `C:\PROJECT\GPT\src\modules\admin-profiles.js`
- `C:\PROJECT\GPT\src\modules\admin-sessions.js` (new)
- `C:\PROJECT\GPT\adminPanelHTML.js`
- `C:\PROJECT\GPT\bot-version.json`
- relevant project documentation files

## Rollout Sequence

1. Add server session module and cookie support.
2. Migrate login and validateSession to real server sessions.
3. Add CAPTCHA generation/verification endpoints.
4. Enforce session validation on admin routes.
5. Add risk-scored IP and user-agent checks.
6. Add frontend blur-lock overlay and login CAPTCHA UI.
7. Add logs, rate limits, tests, version/docs updates.
8. Deploy and verify with live admin flows.

## Open Checks Before Implementation

- Verify whether `Secure` cookie can be enabled safely in current Yandex Cloud access path.
- Confirm exact header source for reliable client IP extraction in production.
- Confirm whether existing frontend stores auth state in localStorage fields that need cleanup during migration.

## Spec Self-Review

Completed checks:

- no placeholders such as TODO/TBD remain
- session authority is consistently server-side throughout the document
- CAPTCHA behavior is consistent between session and login flows
- risk-score thresholds and failure policy are defined
- rollout order matches the architecture

Known implementation-sensitive points:

- cookie handling with current deployment path
- accurate client IP extraction from forwarded headers

These are explicitly called out as pre-implementation checks, not missing design details.
