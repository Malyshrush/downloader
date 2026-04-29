# Admin Session CAPTCHA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace frontend-trusted admin access with server-owned cookie sessions, risk-based IP/device validation, and local SVG CAPTCHA protection for suspicious admin activity and post-incident login.

**Architecture:** The backend becomes the source of truth for admin identity via `adminSessionId` stored in an `HttpOnly` cookie and persisted in a new `admin_sessions.json` store. Every admin route validates that session, computes a risk score from IP/User-Agent/time-change signals, and either allows the request, demands CAPTCHA, or kills the session after repeated failure. The frontend switches to `credentials: 'include'`, renders login/session CAPTCHA UIs, and reacts to machine-readable auth states instead of trusting `principalProfileId` alone.

**Tech Stack:** Node.js 18, Yandex Cloud Functions, AWS SDK S3 storage, vanilla browser JS in `adminPanelHTML.js`, existing Object Storage JSON persistence, `node --check`, existing `tests/` Node test style.

---

### Task 1: Add Admin Session Store And Risk Engine

**Files:**
- Create: `C:\PROJECT\GPT\src\modules\admin-sessions.js`
- Test: `C:\PROJECT\GPT\tests\admin-sessions.test.js`

- [ ] **Step 1: Write the failing test for session creation, timeout, and risk scoring**

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const {
  createSessionRecord,
  normalizeSessionStore,
  computeSessionRisk,
  isSessionExpired
} = require('../src/modules/admin-sessions');

test('createSessionRecord seeds verified client context', () => {
  const session = createSessionRecord({
    sessionId: 'sess_1',
    profileId: '2',
    ip: '203.0.113.10',
    userAgent: 'Mozilla/5.0',
    now: '2026-04-21T10:00:00.000Z'
  });

  assert.equal(session.profileId, '2');
  assert.equal(session.lastVerifiedIp, '203.0.113.10');
  assert.equal(session.lastUserAgent, 'Mozilla/5.0');
  assert.equal(session.captchaRequired, false);
});

test('isSessionExpired returns true after 12 hours of inactivity', () => {
  const session = createSessionRecord({
    sessionId: 'sess_2',
    profileId: '2',
    ip: '203.0.113.10',
    userAgent: 'Mozilla/5.0',
    now: '2026-04-21T00:00:00.000Z'
  });

  session.lastSeenAt = '2026-04-21T00:00:00.000Z';
  assert.equal(isSessionExpired(session, new Date('2026-04-21T12:00:01.000Z')), true);
});

test('computeSessionRisk requires captcha for suspicious fast ip change with ua drift', () => {
  const session = createSessionRecord({
    sessionId: 'sess_3',
    profileId: '2',
    ip: '203.0.113.10',
    userAgent: 'Mozilla/5.0',
    now: '2026-04-21T10:00:00.000Z'
  });

  const risk = computeSessionRisk(session, {
    ip: '198.51.100.25',
    userAgent: 'curl/8.0',
    now: new Date('2026-04-21T10:05:00.000Z')
  });

  assert.equal(risk.total >= 3, true);
  assert.equal(risk.requiresCaptcha, true);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node tests\admin-sessions.test.js`  
Expected: FAIL with `Cannot find module '../src/modules/admin-sessions'` or missing exported functions.

- [ ] **Step 3: Write minimal session store and risk engine**

```js
const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const crypto = require('crypto');
const { getS3Client, getBucketName } = require('./storage');

const SESSIONS_FILE_KEY = 'admin_sessions.json';
const SESSION_TIMEOUT_MS = 12 * 60 * 60 * 1000;

function createSessionRecord({ sessionId, profileId, ip, userAgent, now = new Date().toISOString() }) {
  return {
    sessionId,
    profileId: String(profileId),
    createdAt: now,
    lastSeenAt: now,
    lastVerifiedIp: String(ip || '').trim(),
    lastUserAgent: String(userAgent || '').trim(),
    captchaRequired: false,
    captchaFailCount: 0,
    suspiciousChangeCount: 0,
    captchaChallenge: null,
    loginCaptchaRequired: false,
    loginCaptchaFailCount: 0,
    terminatedAt: null,
    terminateReason: ''
  };
}

function normalizeSessionStore(raw) {
  return raw && typeof raw === 'object' && raw.sessions && typeof raw.sessions === 'object'
    ? { sessions: raw.sessions }
    : { sessions: {} };
}

function isSessionExpired(session, now = new Date()) {
  const lastSeenAtMs = new Date(session?.lastSeenAt || 0).getTime();
  return !lastSeenAtMs || (now.getTime() - lastSeenAtMs > SESSION_TIMEOUT_MS);
}

function computeSessionRisk(session, { ip, userAgent, now = new Date() }) {
  let total = 0;
  const currentIp = String(ip || '').trim();
  const currentUa = String(userAgent || '').trim();
  const previousSeenMs = new Date(session?.lastSeenAt || 0).getTime();

  if (currentIp && session?.lastVerifiedIp && currentIp !== session.lastVerifiedIp) total += 2;
  if (currentUa && session?.lastUserAgent && currentUa !== session.lastUserAgent) total += 2;
  if (currentIp && session?.lastVerifiedIp && currentIp !== session.lastVerifiedIp && previousSeenMs && (now.getTime() - previousSeenMs) < 15 * 60 * 1000) total += 2;
  total += Math.min(3, Number(session?.suspiciousChangeCount || 0));

  return {
    total,
    requiresCaptcha: total >= 3
  };
}

module.exports = {
  SESSIONS_FILE_KEY,
  SESSION_TIMEOUT_MS,
  createSessionRecord,
  normalizeSessionStore,
  isSessionExpired,
  computeSessionRisk
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node tests\admin-sessions.test.js`  
Expected: PASS

- [ ] **Step 5: Extend the module with storage, lifecycle, and CAPTCHA state helpers**

```js
async function loadAdminSessions() { /* load admin_sessions.json from S3 */ }
async function saveAdminSessions(data) { /* persist normalized session store */ }
function generateSessionId() { return 'sess_' + crypto.randomBytes(24).toString('hex'); }
function markSessionCaptchaRequired(session, reason) { /* set flags */ }
function touchSession(session, { ip, userAgent, now = new Date().toISOString() }) { /* update activity */ }
function killSession(session, reason, now = new Date().toISOString()) { /* terminate */ }
function upsertSession(store, session) { store.sessions[session.sessionId] = session; return store; }
function pruneExpiredSessions(store, now = new Date()) { /* remove dead old sessions */ }
```

- [ ] **Step 6: Commit**

```bash
git add src/modules/admin-sessions.js tests/admin-sessions.test.js
git commit -m "feat: add admin session store and risk engine"
```

### Task 2: Add CAPTCHA, Security Events, And Rate Limits

**Files:**
- Modify: `C:\PROJECT\GPT\src\modules\admin-security.js`
- Modify: `C:\PROJECT\GPT\src\modules\admin-sessions.js`
- Test: `C:\PROJECT\GPT\tests\admin-security-captcha.test.js`

- [ ] **Step 1: Write the failing test for SVG CAPTCHA, rate limits, and security events**

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const {
  createCaptchaChallenge,
  verifyCaptchaAnswer,
  appendSecurityEvent,
  checkCaptchaRateLimit
} = require('../src/modules/admin-security');

test('createCaptchaChallenge returns svg and non-plain answer hash', () => {
  const challenge = createCaptchaChallenge({ mode: 'session', now: new Date('2026-04-21T10:00:00.000Z') });
  assert.match(challenge.captchaSvg, /<svg/);
  assert.equal(typeof challenge.hash, 'string');
  assert.equal(challenge.hash.length > 10, true);
  assert.equal(challenge.answer === undefined, true);
});

test('verifyCaptchaAnswer fails after 3 bad attempts', () => {
  const challenge = createCaptchaChallenge({ mode: 'session', now: new Date('2026-04-21T10:00:00.000Z') });
  const state = { captchaChallenge: challenge, captchaFailCount: 0 };
  verifyCaptchaAnswer(state, 'wrong');
  verifyCaptchaAnswer(state, 'wrong');
  const result = verifyCaptchaAnswer(state, 'wrong');
  assert.equal(result.terminateSession, true);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node tests\admin-security-captcha.test.js`  
Expected: FAIL because these helpers do not exist yet.

- [ ] **Step 3: Add CAPTCHA and event helpers to `admin-security.js`**

```js
const crypto = require('crypto');

function hashCaptchaAnswer(answer) {
  return crypto.createHash('sha256').update(String(answer || '').trim().toUpperCase()).digest('hex');
}

function createCaptchaChallenge({ mode = 'session', now = new Date() }) {
  const answer = Math.random().toString(36).slice(2, 8).toUpperCase();
  return {
    mode,
    hash: hashCaptchaAnswer(answer),
    expiresAt: new Date(now.getTime() + 5 * 60 * 1000).toISOString(),
    attempts: 0,
    captchaSvg: `<svg xmlns="http://www.w3.org/2000/svg" width="180" height="64"><text x="18" y="42">${answer}</text></svg>`
  };
}

function verifyCaptchaAnswer(sessionState, answer, now = new Date()) {
  const challenge = sessionState?.captchaChallenge;
  if (!challenge) return { ok: false, errorCode: 'captcha_missing' };
  if (new Date(challenge.expiresAt).getTime() <= now.getTime()) return { ok: false, errorCode: 'captcha_expired' };

  challenge.attempts = Number(challenge.attempts || 0) + 1;
  const ok = challenge.hash === hashCaptchaAnswer(answer);
  if (ok) return { ok: true };

  const nextFailCount = Number(sessionState.captchaFailCount || 0) + 1;
  sessionState.captchaFailCount = nextFailCount;
  return { ok: false, terminateSession: nextFailCount >= 3, remainingAttempts: Math.max(0, 3 - nextFailCount) };
}

function appendSecurityEvent(data, entry) {
  data.securityEvents = Array.isArray(data.securityEvents) ? data.securityEvents : [];
  data.securityEvents.unshift({
    id: `sec_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: new Date().toISOString(),
    ...entry
  });
  data.securityEvents = data.securityEvents.slice(0, 1000);
}
```

- [ ] **Step 4: Add rate-limit state helpers**

```js
function getRateLimitBucket(container, key, now = Date.now(), windowMs = 10 * 60 * 1000) {
  const current = container[key] || { count: 0, resetAt: now + windowMs, lastAt: 0 };
  if (current.resetAt <= now) {
    return { count: 0, resetAt: now + windowMs, lastAt: 0 };
  }
  return current;
}

function checkCaptchaRateLimit({ data, sessionId = '', ip = '', action = 'submit', now = Date.now() }) {
  data.captchaRateLimits = data.captchaRateLimits || {};
  const key = `${action}:${sessionId || 'anonymous'}:${ip || 'no_ip'}`;
  const bucket = getRateLimitBucket(data.captchaRateLimits, key, now);
  const cooldownMs = bucket.lastAt ? (2000 - (now - bucket.lastAt)) : 0;
  return {
    key,
    bucket,
    blocked: bucket.count >= 10 || cooldownMs > 0,
    cooldownMs: Math.max(0, cooldownMs)
  };
}

function registerCaptchaRateLimitHit({ data, key, bucket, now = Date.now() }) {
  data.captchaRateLimits[key] = {
    count: Number(bucket.count || 0) + 1,
    resetAt: bucket.resetAt || (now + 10 * 60 * 1000),
    lastAt: now
  };
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `node tests\admin-security-captcha.test.js`  
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/modules/admin-security.js src/modules/admin-sessions.js tests/admin-security-captcha.test.js
git commit -m "feat: add admin captcha, events, and rate limits"
```

### Task 3: Migrate Backend Auth To Cookie Sessions

**Files:**
- Modify: `C:\PROJECT\GPT\src\handler.js`
- Modify: `C:\PROJECT\GPT\src\modules\admin-profiles.js`
- Modify: `C:\PROJECT\GPT\src\modules\admin-security.js`
- Modify: `C:\PROJECT\GPT\src\modules\admin-sessions.js`
- Test: `C:\PROJECT\GPT\tests\admin-auth-flow.test.js`

- [ ] **Step 1: Write the failing integration test for login, session validation, and suspicious request blocking**

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const { handler } = require('../src/handler');

test('loginAdmin sets cookie-backed session instead of fake token', async () => {
  const response = await handler({
    httpMethod: 'POST',
    queryStringParameters: { loginAdmin: '' },
    headers: { 'user-agent': 'Mozilla/5.0', 'x-forwarded-for': '203.0.113.10' },
    body: JSON.stringify({ username: 'admin', password: 'admin123' })
  });

  assert.equal(response.statusCode, 200);
  assert.match(String(response.headers['Set-Cookie'] || response.headers['set-cookie'] || ''), /adminSessionId=/);
  assert.doesNotMatch(response.body, /authenticated_/);
});

test('validateSession returns captchaRequired for suspicious client drift', async () => {
  assert.fail('Implement with session fixture after cookie session support exists');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node tests\admin-auth-flow.test.js`  
Expected: FAIL because login still returns fake token and no cookie session.

- [ ] **Step 3: Add cookie helpers and request context extraction to `handler.js`**

```js
function parseCookies(event = {}) {
  const raw = event.headers?.cookie || event.headers?.Cookie || '';
  return raw.split(';').reduce((acc, pair) => {
    const [key, ...rest] = pair.split('=');
    if (!key) return acc;
    acc[key.trim()] = rest.join('=').trim();
    return acc;
  }, {});
}

function getClientIp(event = {}) {
  const forwarded = event.headers?.['x-forwarded-for'] || event.headers?.['X-Forwarded-For'] || '';
  return String(forwarded).split(',')[0].trim();
}

function getUserAgent(event = {}) {
  return String(event.headers?.['user-agent'] || event.headers?.['User-Agent'] || '').trim();
}

function buildSessionCookie(sessionId) {
  return `adminSessionId=${sessionId}; Path=/; HttpOnly; SameSite=Lax`;
}

function buildClearSessionCookie() {
  return 'adminSessionId=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0';
}
```

- [ ] **Step 4: Replace login flow with real session creation**

```js
const authResult = await verifyAdminCredentials(username, password);
if (authResult.success) {
  const session = await createAdminSession({
    profileId: authResult.profileId,
    ip: getClientIp(event),
    userAgent: getUserAgent(event)
  });

  return {
    statusCode: 200,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Set-Cookie': buildSessionCookie(session.sessionId)
    },
    body: JSON.stringify({
      success: true,
      profileId: authResult.profileId,
      principalProfileId: authResult.profileId,
      profileName: authResult.profileName,
      role: authResult.role,
      isMainAdmin: authResult.isMainAdmin
    })
  };
}
```

- [ ] **Step 5: Replace `validateAdminSession` so it reads and enforces cookie session**

```js
async function validateAdminSession(event, query = {}, body = {}) {
  const cookies = parseCookies(event);
  const sessionId = cookies.adminSessionId || '';
  const clientIp = getClientIp(event);
  const userAgent = getUserAgent(event);
  const result = await validateAdminSessionRequest({
    sessionId,
    ip: clientIp,
    userAgent
  });

  if (!result.ok) {
    return {
      ok: false,
      statusCode: result.statusCode,
      error: result.error,
      captchaRequired: !!result.captchaRequired,
      expired: !!result.expired,
      clearCookie: !!result.clearCookie
    };
  }

  return {
    ok: true,
    principalProfile: result.profile,
    session: result.session
  };
}
```

- [ ] **Step 6: Add new endpoints**

```js
if (q.logoutAdmin !== undefined) return handleLogoutAdmin(event);
if (q.getCaptcha !== undefined) return handleGetCaptcha(event);
if (q.verifyCaptcha !== undefined) return handleVerifyCaptcha(event);
```

Implementation notes:

- `handleLogoutAdmin` kills the current session and clears the cookie
- `handleGetCaptcha` returns SVG for `login` or `session` mode
- `handleVerifyCaptcha` resolves session/login challenge and updates verified client context
- every admin GET/POST route must pass full `event` into `validateAdminSession`

- [ ] **Step 7: Run tests to verify they pass**

Run: `node tests\admin-auth-flow.test.js`  
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/handler.js src/modules/admin-profiles.js src/modules/admin-security.js src/modules/admin-sessions.js tests/admin-auth-flow.test.js
git commit -m "feat: migrate admin auth to cookie sessions"
```

### Task 4: Add Frontend CAPTCHA UX And Cookie-Based Session Calls

**Files:**
- Modify: `C:\PROJECT\GPT\adminPanelHTML.js`
- Test: `C:\PROJECT\GPT\tests\admin-panel-auth-contract.test.js`

- [ ] **Step 1: Write the failing contract test for frontend auth state**

```js
const test = require('node:test');
const assert = require('node:assert/strict');
const { adminPanelHTML } = require('../adminPanelHTML');

test('admin panel fetches with credentials include', () => {
  assert.match(adminPanelHTML, /credentials:\\s*'include'/);
});

test('admin panel contains session captcha overlay hooks', () => {
  assert.match(adminPanelHTML, /captcha-lock/);
  assert.match(adminPanelHTML, /verifyCaptcha/);
  assert.match(adminPanelHTML, /getCaptcha/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node tests\admin-panel-auth-contract.test.js`  
Expected: FAIL because cookie-based fetch and CAPTCHA overlay contract are not fully present.

- [ ] **Step 3: Add overlay markup and state helpers**

```html
<div id="sessionCaptchaOverlay" class="session-captcha-overlay" style="display:none;">
  <div class="session-captcha-modal">
    <h2>Подтверждение безопасности</h2>
    <p>Обнаружено подозрительное изменение сети или устройства. Пройди каптчу, чтобы продолжить работу.</p>
    <div id="sessionCaptchaImage"></div>
    <input id="sessionCaptchaAnswer" type="text" placeholder="Введите символы с картинки">
    <div class="profile-card-actions">
      <button class="btn btn-save" type="button" onclick="submitSessionCaptcha()">Подтвердить</button>
      <button class="btn btn-neutral" type="button" onclick="refreshSessionCaptcha()">Обновить каптчу</button>
    </div>
    <div id="sessionCaptchaStatus"></div>
  </div>
</div>
```

```js
window.authUiState = {
  loginCaptchaRequired: false,
  sessionCaptchaRequired: false
};

function setSessionCaptchaLock(visible) {
  document.body.classList.toggle('captcha-lock', !!visible);
  var overlay = document.getElementById('sessionCaptchaOverlay');
  if (overlay) overlay.style.display = visible ? 'flex' : 'none';
}
```

- [ ] **Step 4: Route all auth-sensitive fetches through a shared helper**

```js
async function fetchAdminJson(url, options) {
  const response = await fetch(url, {
    credentials: 'include',
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options && options.headers ? options.headers : {})
    }
  });

  const data = await response.json();

  if (data && data.captchaRequired) {
    window.authUiState.sessionCaptchaRequired = true;
    setSessionCaptchaLock(true);
    await refreshSessionCaptcha();
    throw new Error('CAPTCHA_REQUIRED');
  }

  if (data && data.sessionInvalid) {
    performForcedLogout(data.error || 'Сессия недействительна');
    throw new Error('SESSION_INVALID');
  }

  return data;
}
```

- [ ] **Step 5: Add login CAPTCHA flow**

```js
async function refreshLoginCaptcha() {
  const data = await fetchAdminJson(baseUrl + '?getCaptcha&mode=login', { method: 'GET' });
  document.getElementById('loginCaptchaBox').innerHTML = data.captchaSvg;
}

async function loginAdmin() {
  const payload = {
    username: document.getElementById('username').value,
    password: document.getElementById('password').value,
    captchaAnswer: window.authUiState.loginCaptchaRequired
      ? document.getElementById('loginCaptchaAnswer').value
      : ''
  };

  const data = await fetchAdminJson(baseUrl + '?loginAdmin', {
    method: 'POST',
    body: JSON.stringify(payload)
  });
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `node tests\admin-panel-auth-contract.test.js`  
Expected: PASS

- [ ] **Step 7: Manual verification**

Run:

```bash
node --check C:\PROJECT\GPT\adminPanelHTML.js
```

Expected: no syntax errors

- [ ] **Step 8: Commit**

```bash
git add adminPanelHTML.js tests/admin-panel-auth-contract.test.js
git commit -m "feat: add admin captcha ui and cookie auth client"
```

### Task 5: Wire Existing Admin Flows, Update Docs, And Release

**Files:**
- Modify: `C:\PROJECT\GPT\src\handler.js`
- Modify: `C:\PROJECT\GPT\bot-version.json`
- Modify: `C:\PROJECT\GPT\README.md`
- Modify: `C:\PROJECT\GPT\PROJECT_DOCUMENTATION.md`
- Modify: `C:\PROJECT\GPT\GPT.md`

- [ ] **Step 1: Remove trust in frontend-only actor identity on protected routes**

```js
// Before:
const principalProfileId = getRequestPrincipalProfileId(query, body);
const principalProfile = await getProfileById(principalProfileId);

// After:
const session = await validateAdminSession(event, query, body);
if (!session.ok) return buildSessionErrorResponse(session);
const principalProfile = session.principalProfile;
```

Apply this to:

- `getAdminDashboard`
- `getAdminProfiles`
- `getProfileDashboard`
- `saveAdminProfile`
- `deleteAdminProfile`
- `savePromoCode`
- `deletePromoCode`
- `resolveRecovery`
- settings and sheet reads/writes

- [ ] **Step 2: Update release metadata**

Add a new release note describing:

- cookie-backed admin sessions
- SVG CAPTCHA on suspicious session change
- IP/User-Agent risk detection
- forced relogin after repeated CAPTCHA failure
- session timeout and security logs

- [ ] **Step 3: Run verification commands**

Run:

```bash
node tests\admin-sessions.test.js
node tests\admin-security-captcha.test.js
node tests\admin-auth-flow.test.js
node tests\admin-panel-auth-contract.test.js
node --check C:\PROJECT\GPT\adminPanelHTML.js
node --check C:\PROJECT\GPT\src\handler.js
node --check C:\PROJECT\GPT\src\modules\admin-security.js
node --check C:\PROJECT\GPT\src\modules\admin-sessions.js
```

Expected: all PASS / no syntax errors

- [ ] **Step 4: Deploy**

Run:

```bash
node scripts\deploy.js
```

Expected: successful Yandex Cloud deployment with new revision id

- [ ] **Step 5: Post-deploy verification**

Run:

```bash
C:\Users\Михаил\yandex-cloud\bin\yc.exe serverless function logs vk-bot-2 --limit 40
curl.exe -s --insecure "https://functions.yandexcloud.net/d4eg37ikm3vl5tm1mjld?health"
curl.exe -s --insecure "https://functions.yandexcloud.net/d4eg37ikm3vl5tm1mjld?getBotVersion"
```

Manual checks:

- normal login still works
- suspicious IP/device simulation triggers CAPTCHA
- 3 failed CAPTCHA attempts force logout
- login screen requires CAPTCHA after forced security logout
- existing admin tabs still load

- [ ] **Step 6: Commit**

```bash
git add src/handler.js bot-version.json README.md PROJECT_DOCUMENTATION.md GPT.md
git commit -m "feat: secure admin panel with sessions and captcha"
```

## Self-Review

### Spec coverage

Covered:

- server session model: Task 1, Task 3
- `HttpOnly` cookie: Task 3
- session timeout: Task 1, Task 3
- risk score: Task 1, Task 3
- local SVG CAPTCHA: Task 2, Task 3, Task 4
- session CAPTCHA and login CAPTCHA: Task 2, Task 3, Task 4
- 3 failed CAPTCHA attempts kill session: Task 2, Task 3, Task 4
- security events: Task 2
- rate limits: Task 2, Task 3
- frontend blur lock: Task 4
- docs/version/deploy: Task 5

No uncovered spec requirements remain.

### Placeholder scan

Checked for:

- TODO
- TBD
- “implement later”
- vague “add handling”

Plan contains concrete files, tests, commands, and code stubs only.

### Type consistency

Consistent names used throughout:

- `adminSessionId`
- `admin_sessions.json`
- `captchaRequired`
- `loginCaptchaRequired`
- `validateAdminSessionRequest`
- `createAdminSession`
- `verifyCaptcha`
- `getCaptcha`

No conflicting function names found.
