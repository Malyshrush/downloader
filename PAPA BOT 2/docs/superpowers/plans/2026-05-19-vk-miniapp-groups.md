# VK Mini App Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the MVP VK Mini App groups experience: a separate Vite/React frontend, PAPA BOT public Mini App API, admin-configured group visuals, and subscribe/unsubscribe against existing user group data.

**Architecture:** Keep PAPA BOT as the source of truth and add focused Mini App modules for group normalization, signed VK launch auth, and public image assets. Keep the frontend in a separate `miniapp/` project that calls PAPA BOT API and uses VK Bridge only for Mini App lifecycle and the message permission gate.

**Tech Stack:** Node.js CommonJS backend, `node:test`, existing PAPA BOT storage APIs, Vite, React, VK Bridge.

---

## Scope Check

This plan implements one MVP with three connected parts:

- PAPA BOT backend/API and storage helpers;
- PAPA BOT admin panel group settings;
- separate `miniapp/` frontend.

Do not split these into unrelated branches because each part depends on the same group field contract. Execute in small commits so the backend can be reviewed before the frontend is wired.

## File Structure

- Create `src/modules/miniapp-groups.js`: normalize group rows, validate slugs, resolve visible/detail group DTOs, and avoid UI-specific logic in `handler.js`.
- Create `src/modules/miniapp-auth.js`: parse and verify VK Mini App launch params.
- Create `src/modules/miniapp-assets.js`: store uploaded icon/banner image bytes in object storage and serve them through signed/public asset URLs controlled by PAPA BOT.
- Modify `src/modules/users.js`: expose an idempotent helper that creates a missing Mini App user without requiring a VK API name lookup.
- Modify `src/handler.js`: route `GET` and `POST` Mini App endpoints before admin-session-protected routes.
- Modify `adminPanelHTML.js`: add Mini App fields to the `ГРУППЫ` manager and upload/link controls for icon and banner.
- Create `miniapp/`: Vite + React frontend project.
- Update `FUNCTIONALITY.md`: document the new Mini App groups functionality.
- Add tests:
  - `tests/miniapp-groups.test.js`
  - `tests/miniapp-auth.test.js`
  - `tests/miniapp-assets.test.js`
  - `tests/miniapp-api.test.js`
  - extend `tests/admin-panel-groups-ui.test.js`

Use `node --test tests/<file>.test.js` because the current root `package.json` has no test script.

---

### Task 1: Mini App Group Normalization

**Files:**
- Create: `src/modules/miniapp-groups.js`
- Create: `tests/miniapp-groups.test.js`

- [ ] **Step 1: Write failing tests for group normalization**

Create `tests/miniapp-groups.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const {
  normalizeMiniAppSlug,
  normalizeMiniAppGroupRows,
  listVisibleMiniAppGroups,
  findMiniAppGroupBySlug
} = require('../src/modules/miniapp-groups');

test('normalizeMiniAppSlug keeps stable latin slugs', () => {
  assert.equal(normalizeMiniAppSlug(' VIP_offer-2026 '), 'vip_offer-2026');
});

test('normalizeMiniAppSlug generates latin fallback when input is empty', () => {
  assert.match(normalizeMiniAppSlug('', 'VIP Club'), /^vip-club$/);
});

test('visible group list excludes hidden and disabled groups', () => {
  const rows = normalizeMiniAppGroupRows([
    {
      'Группа': 'vip',
      'MiniApp включен': 'да',
      'MiniApp скрыть из списка': '',
      'MiniApp slug': 'vip',
      'MiniApp заголовок': 'VIP',
      'MiniApp описание': 'Visible',
      'MiniApp иконка URL': 'https://cdn.example/icon.png',
      'MiniApp иконка файл': 'https://files.example/icon-upload.png'
    },
    {
      'Группа': 'secret',
      'MiniApp включен': 'да',
      'MiniApp скрыть из списка': 'да',
      'MiniApp slug': 'secret',
      'MiniApp заголовок': 'Secret'
    },
    {
      'Группа': 'off',
      'MiniApp включен': '',
      'MiniApp slug': 'off',
      'MiniApp заголовок': 'Off'
    }
  ]);

  const visible = listVisibleMiniAppGroups(rows);
  assert.deepEqual(visible.map(item => item.slug), ['vip']);
  assert.equal(visible[0].iconUrl, 'https://cdn.example/icon.png');
});

test('direct lookup returns hidden enabled group by slug', () => {
  const rows = normalizeMiniAppGroupRows([
    {
      'Группа': 'secret',
      'MiniApp включен': 'да',
      'MiniApp скрыть из списка': 'да',
      'MiniApp slug': 'secret',
      'MiniApp заголовок': 'Secret',
      'MiniApp текст подписки': 'Беру',
      'MiniApp текст отписки': 'Убрать'
    }
  ]);

  const group = findMiniAppGroupBySlug(rows, 'secret');
  assert.equal(group.groupName, 'secret');
  assert.equal(group.subscribeText, 'Беру');
  assert.equal(group.unsubscribeText, 'Убрать');
});

test('duplicate enabled slugs are rejected', () => {
  assert.throws(() => normalizeMiniAppGroupRows([
    { 'Группа': 'a', 'MiniApp включен': 'да', 'MiniApp slug': 'dup' },
    { 'Группа': 'b', 'MiniApp включен': 'да', 'MiniApp slug': 'dup' }
  ]), /Duplicate MiniApp slug: dup/);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
node --test tests/miniapp-groups.test.js
```

Expected: FAIL with `Cannot find module '../src/modules/miniapp-groups'`.

- [ ] **Step 3: Implement group normalization module**

Create `src/modules/miniapp-groups.js`:

```js
const TRUE_VALUES = new Set(['1', 'true', 'yes', 'y', 'да', 'дa', 'вкл', 'включен', 'включено']);

function clean(value) {
    return String(value || '').trim();
}

function isTruthy(value) {
    return TRUE_VALUES.has(clean(value).toLowerCase());
}

function transliterateBasic(value) {
    const map = {
        а: 'a', б: 'b', в: 'v', г: 'g', д: 'd', е: 'e', ё: 'e', ж: 'zh', з: 'z', и: 'i', й: 'y',
        к: 'k', л: 'l', м: 'm', н: 'n', о: 'o', п: 'p', р: 'r', с: 's', т: 't', у: 'u',
        ф: 'f', х: 'h', ц: 'c', ч: 'ch', ш: 'sh', щ: 'sch', ы: 'y', э: 'e', ю: 'yu', я: 'ya',
        ъ: '', ь: ''
    };
    return String(value || '').toLowerCase().split('').map(ch => map[ch] !== undefined ? map[ch] : ch).join('');
}

function normalizeMiniAppSlug(value, fallback = '') {
    const source = clean(value) || clean(fallback);
    const slug = transliterateBasic(source)
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^[-_]+|[-_]+$/g, '');
    return slug || 'group';
}

function resolveImageUrl(manualUrl, uploadedUrl) {
    return clean(manualUrl) || clean(uploadedUrl);
}

function normalizeMiniAppGroupRows(rows) {
    const seen = new Set();
    return (Array.isArray(rows) ? rows : []).map((row, index) => {
        const groupName = clean(row['Группа']);
        const enabled = isTruthy(row['MiniApp включен']);
        const slug = normalizeMiniAppSlug(row['MiniApp slug'], groupName || `group-${index + 1}`);
        if (enabled) {
            if (seen.has(slug)) throw new Error(`Duplicate MiniApp slug: ${slug}`);
            seen.add(slug);
        }
        return {
            rowIndex: index,
            groupName,
            adminDescription: clean(row['Описание']),
            enabled,
            hidden: isTruthy(row['MiniApp скрыть из списка']),
            slug,
            title: clean(row['MiniApp заголовок']) || groupName,
            description: clean(row['MiniApp описание']) || clean(row['Описание']),
            iconUrl: resolveImageUrl(row['MiniApp иконка URL'], row['MiniApp иконка файл']),
            bannerUrl: resolveImageUrl(row['MiniApp баннер URL'], row['MiniApp баннер файл']),
            subscribeText: clean(row['MiniApp текст подписки']) || 'Подписаться',
            unsubscribeText: clean(row['MiniApp текст отписки']) || 'Отписаться'
        };
    });
}

function toListDto(group) {
    return {
        slug: group.slug,
        title: group.title,
        description: group.description,
        iconUrl: group.iconUrl,
        subscribed: Boolean(group.subscribed)
    };
}

function toDetailDto(group, subscribed = false) {
    return {
        slug: group.slug,
        title: group.title,
        description: group.description,
        iconUrl: group.iconUrl,
        bannerUrl: group.bannerUrl,
        subscribeText: group.subscribeText,
        unsubscribeText: group.unsubscribeText,
        subscribed
    };
}

function listVisibleMiniAppGroups(groups, subscribedNames = []) {
    const subscribed = new Set((Array.isArray(subscribedNames) ? subscribedNames : []).map(item => String(item || '').trim().toLowerCase()));
    return groups
        .filter(group => group.enabled && !group.hidden)
        .map(group => toListDto({ ...group, subscribed: subscribed.has(group.groupName.toLowerCase()) }));
}

function findMiniAppGroupBySlug(groups, slug) {
    const normalized = normalizeMiniAppSlug(slug);
    return groups.find(group => group.enabled && group.slug === normalized) || null;
}

module.exports = {
    normalizeMiniAppSlug,
    normalizeMiniAppGroupRows,
    listVisibleMiniAppGroups,
    findMiniAppGroupBySlug,
    toDetailDto
};
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
node --test tests/miniapp-groups.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/modules/miniapp-groups.js tests/miniapp-groups.test.js
git commit -m "Add Mini App group normalization"
```

---

### Task 2: VK Mini App Launch Signature Verification

**Files:**
- Create: `src/modules/miniapp-auth.js`
- Create: `tests/miniapp-auth.test.js`
- Modify: `.env-template`

- [ ] **Step 1: Write failing auth tests**

Create `tests/miniapp-auth.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const {
  buildVkLaunchSignPayload,
  signVkLaunchParams,
  verifyVkLaunchParams
} = require('../src/modules/miniapp-auth');

test('buildVkLaunchSignPayload sorts vk params and excludes sign', () => {
  const payload = buildVkLaunchSignPayload({
    sign: 'ignored',
    vk_user_id: '123',
    vk_app_id: '999',
    c: '229445618'
  });
  assert.equal(payload, 'vk_app_id=999&vk_user_id=123');
});

test('verifyVkLaunchParams accepts valid signature', () => {
  const secret = 'miniapp-secret';
  const params = {
    vk_app_id: '999',
    vk_user_id: '123',
    vk_group_id: '229445618'
  };
  const sign = signVkLaunchParams(params, secret);
  const result = verifyVkLaunchParams({ ...params, sign }, { secret });
  assert.equal(result.ok, true);
  assert.equal(result.userId, '123');
  assert.equal(result.groupId, '229445618');
});

test('verifyVkLaunchParams rejects invalid signature', () => {
  const result = verifyVkLaunchParams({
    vk_app_id: '999',
    vk_user_id: '123',
    sign: 'bad'
  }, { secret: 'miniapp-secret' });
  assert.equal(result.ok, false);
  assert.equal(result.error, 'invalid_vk_sign');
});

test('verifyVkLaunchParams rejects missing user id', () => {
  const secret = 'miniapp-secret';
  const params = { vk_app_id: '999' };
  const sign = crypto.createHmac('sha256', secret).update('vk_app_id=999').digest('base64url');
  const result = verifyVkLaunchParams({ ...params, sign }, { secret });
  assert.equal(result.ok, false);
  assert.equal(result.error, 'missing_vk_user_id');
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
node --test tests/miniapp-auth.test.js
```

Expected: FAIL with `Cannot find module '../src/modules/miniapp-auth'`.

- [ ] **Step 3: Implement signed launch auth**

Create `src/modules/miniapp-auth.js`:

```js
const crypto = require('node:crypto');

function clean(value) {
    return String(value || '').trim();
}

function buildVkLaunchSignPayload(params) {
    return Object.keys(params || {})
        .filter(key => key.startsWith('vk_'))
        .filter(key => key !== 'sign')
        .sort()
        .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(clean(params[key]))}`)
        .join('&');
}

function signVkLaunchParams(params, secret) {
    return crypto
        .createHmac('sha256', clean(secret))
        .update(buildVkLaunchSignPayload(params))
        .digest('base64url');
}

function timingSafeEqualString(a, b) {
    const left = Buffer.from(clean(a));
    const right = Buffer.from(clean(b));
    if (left.length !== right.length) return false;
    return crypto.timingSafeEqual(left, right);
}

function verifyVkLaunchParams(params, options = {}) {
    const secret = clean(options.secret || process.env.VK_MINIAPP_SECRET);
    if (!secret) return { ok: false, error: 'missing_miniapp_secret' };
    const expected = signVkLaunchParams(params, secret);
    if (!timingSafeEqualString(params && params.sign, expected)) {
        return { ok: false, error: 'invalid_vk_sign' };
    }
    const userId = clean(params.vk_user_id);
    if (!userId) return { ok: false, error: 'missing_vk_user_id' };
    return {
        ok: true,
        userId,
        appId: clean(params.vk_app_id),
        groupId: clean(params.vk_group_id),
        params: { ...params }
    };
}

module.exports = {
    buildVkLaunchSignPayload,
    signVkLaunchParams,
    verifyVkLaunchParams
};
```

Modify `.env-template` by adding:

```text
VK_MINIAPP_SECRET=
VK_MINIAPP_APP_URL=https://vk.com/appXXXX
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
node --test tests/miniapp-auth.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/modules/miniapp-auth.js tests/miniapp-auth.test.js .env-template
git commit -m "Add VK Mini App launch auth"
```

---

### Task 3: Public Mini App Asset Storage

**Files:**
- Create: `src/modules/miniapp-assets.js`
- Create: `tests/miniapp-assets.test.js`
- Modify: `src/handler.js`

- [ ] **Step 1: Write failing asset tests**

Create `tests/miniapp-assets.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const {
  validateMiniAppImageUpload,
  buildMiniAppAssetKey,
  createMiniAppAssetUrl
} = require('../src/modules/miniapp-assets');

test('validateMiniAppImageUpload accepts png and jpeg under size limit', () => {
  assert.doesNotThrow(() => validateMiniAppImageUpload({
    contentType: 'image/png',
    buffer: Buffer.from('png')
  }));
  assert.doesNotThrow(() => validateMiniAppImageUpload({
    contentType: 'image/jpeg',
    buffer: Buffer.from('jpg')
  }));
});

test('validateMiniAppImageUpload rejects non-image content', () => {
  assert.throws(() => validateMiniAppImageUpload({
    contentType: 'application/pdf',
    buffer: Buffer.from('pdf')
  }), /Unsupported Mini App image type/);
});

test('buildMiniAppAssetKey scopes assets by profile and community', () => {
  const key = buildMiniAppAssetKey({
    profileId: '1',
    communityId: '229445618',
    assetId: 'asset_abc',
    extension: 'png'
  });
  assert.equal(key, 'miniapp-assets/profile_1/community_229445618/asset_abc.png');
});

test('createMiniAppAssetUrl points to PAPA BOT public asset endpoint', () => {
  const url = createMiniAppAssetUrl({
    baseUrl: 'https://bot.example/handler',
    assetId: 'asset_abc'
  });
  assert.equal(url, 'https://bot.example/handler?miniappAsset=asset_abc');
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
node --test tests/miniapp-assets.test.js
```

Expected: FAIL with `Cannot find module '../src/modules/miniapp-assets'`.

- [ ] **Step 3: Implement asset helper**

Create `src/modules/miniapp-assets.js`:

```js
const crypto = require('node:crypto');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const BUCKET_NAME = process.env.BUCKET_NAME || 'bot-data-storage';
const MAX_IMAGE_BYTES = 2 * 1024 * 1024;
const MIME_TO_EXTENSION = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/webp': 'webp'
};

const s3Client = new S3Client({
    region: 'ru-central1',
    endpoint: 'https://storage.yandexcloud.net',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
});

function clean(value) {
    return String(value || '').trim();
}

function validateMiniAppImageUpload({ contentType, buffer }) {
    const type = clean(contentType).toLowerCase();
    if (!MIME_TO_EXTENSION[type]) throw new Error('Unsupported Mini App image type');
    if (!Buffer.isBuffer(buffer) || buffer.length === 0) throw new Error('Mini App image is empty');
    if (buffer.length > MAX_IMAGE_BYTES) throw new Error('Mini App image is too large');
    return { contentType: type, extension: MIME_TO_EXTENSION[type] };
}

function createMiniAppAssetId() {
    return 'asset_' + crypto.randomBytes(12).toString('hex');
}

function buildMiniAppAssetKey({ profileId, communityId, assetId, extension }) {
    return `miniapp-assets/profile_${clean(profileId) || '1'}/community_${clean(communityId)}/${clean(assetId)}.${clean(extension)}`;
}

function createMiniAppAssetUrl({ baseUrl, assetId }) {
    const url = new URL(clean(baseUrl));
    url.search = '';
    url.searchParams.set('miniappAsset', clean(assetId));
    return url.toString();
}

async function saveMiniAppAssetWithDependencies(payload, overrides = {}) {
    const validation = validateMiniAppImageUpload(payload);
    const assetId = clean(payload.assetId) || createMiniAppAssetId();
    const key = buildMiniAppAssetKey({
        profileId: payload.profileId,
        communityId: payload.communityId,
        assetId,
        extension: validation.extension
    });
    const client = overrides.s3Client || s3Client;
    await client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        Body: payload.buffer,
        ContentType: validation.contentType
    }));
    return {
        assetId,
        key,
        contentType: validation.contentType,
        url: createMiniAppAssetUrl({ baseUrl: payload.baseUrl, assetId })
    };
}

async function readMiniAppAssetWithDependencies(assetId, overrides = {}) {
    const client = overrides.s3Client || s3Client;
    const lookup = overrides.lookupAsset || null;
    const record = lookup ? await lookup(assetId) : null;
    const key = record && record.key ? record.key : clean(assetId);
    const result = await client.send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
    const chunks = [];
    for await (const chunk of result.Body) chunks.push(Buffer.from(chunk));
    return {
        buffer: Buffer.concat(chunks),
        contentType: result.ContentType || record?.contentType || 'application/octet-stream'
    };
}

module.exports = {
    validateMiniAppImageUpload,
    createMiniAppAssetId,
    buildMiniAppAssetKey,
    createMiniAppAssetUrl,
    saveMiniAppAssetWithDependencies,
    readMiniAppAssetWithDependencies
};
```

- [ ] **Step 4: Add handler route for serving assets**

In `src/handler.js`, import:

```js
const { readMiniAppAssetWithDependencies } = require('./modules/miniapp-assets');
```

Add a route before admin session checks:

```js
if (q.miniappAsset !== undefined) {
    const asset = await readMiniAppAssetWithDependencies(q.miniappAsset);
    return {
        statusCode: 200,
        headers: {
            'Content-Type': asset.contentType,
            'Cache-Control': 'public, max-age=31536000, immutable',
            'Access-Control-Allow-Origin': '*'
        },
        body: asset.buffer.toString('base64'),
        isBase64Encoded: true
    };
}
```

Also export a test helper:

```js
handleMiniAppAssetRequestWithDependencies
```

If direct object key lookup is used first, keep asset ids unguessable and generated by `createMiniAppAssetId()`.

- [ ] **Step 5: Run tests**

Run:

```powershell
node --test tests/miniapp-assets.test.js
```

Expected: PASS.

- [ ] **Step 6: Commit**

```powershell
git add src/modules/miniapp-assets.js src/handler.js tests/miniapp-assets.test.js
git commit -m "Add Mini App asset handling"
```

---

### Task 4: Mini App Read API

**Files:**
- Modify: `src/handler.js`
- Create: `tests/miniapp-api.test.js`

- [ ] **Step 1: Write failing read API tests**

Create `tests/miniapp-api.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const { __testOnly } = require('../src/handler');

function parse(response) {
  return JSON.parse(response.body);
}

test('Mini App group list returns only visible enabled groups', async () => {
  const response = await __testOnly.handleMiniAppRequestWithDependencies({
    httpMethod: 'GET',
    queryStringParameters: { miniapp: 'groups', c: '229445618' }
  }, {
    getSheetData: async () => [
      { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' },
      { 'Группа': 'secret', 'MiniApp включен': 'да', 'MiniApp скрыть из списка': 'да', 'MiniApp slug': 'secret', 'MiniApp заголовок': 'Secret' }
    ],
    resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
  });

  assert.equal(response.statusCode, 200);
  assert.deepEqual(parse(response).groups.map(item => item.slug), ['vip']);
});

test('Mini App detail returns hidden enabled group by direct slug', async () => {
  const response = await __testOnly.handleMiniAppRequestWithDependencies({
    httpMethod: 'GET',
    queryStringParameters: { miniapp: 'group', c: '229445618', g: 'secret' }
  }, {
    getSheetData: async () => [
      { 'Группа': 'secret', 'MiniApp включен': 'да', 'MiniApp скрыть из списка': 'да', 'MiniApp slug': 'secret', 'MiniApp заголовок': 'Secret' }
    ],
    resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
  });

  assert.equal(response.statusCode, 200);
  assert.equal(parse(response).group.slug, 'secret');
});

test('Mini App detail treats disabled group as not found', async () => {
  const response = await __testOnly.handleMiniAppRequestWithDependencies({
    httpMethod: 'GET',
    queryStringParameters: { miniapp: 'group', c: '229445618', g: 'off' }
  }, {
    getSheetData: async () => [
      { 'Группа': 'off', 'MiniApp включен': '', 'MiniApp slug': 'off', 'MiniApp заголовок': 'Off' }
    ],
    resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
  });

  assert.equal(response.statusCode, 404);
  assert.equal(parse(response).error, 'group_not_found');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
node --test tests/miniapp-api.test.js
```

Expected: FAIL because `handleMiniAppRequestWithDependencies` is not exported.

- [ ] **Step 3: Implement read route**

In `src/handler.js`, add imports:

```js
const {
    normalizeMiniAppGroupRows,
    listVisibleMiniAppGroups,
    findMiniAppGroupBySlug,
    toDetailDto
} = require('./modules/miniapp-groups');
```

Add helpers:

```js
function miniAppJson(statusCode, payload) {
    return {
        statusCode,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(payload)
    };
}

async function resolveMiniAppCommunity(c, profileId, overrides = {}) {
    const resolveCommunity = overrides.resolveCommunity;
    if (resolveCommunity) return resolveCommunity(c, profileId);
    await loadBotConfig(profileId);
    const config = getFullConfig(profileId);
    const communities = config.communities || {};
    const match = Object.entries(communities).find(([, community]) => {
        return String(community.vk_group_id || '').trim() === String(c || '').trim();
    });
    if (!match) return null;
    return { profileId, communityId: String(c), internalCommunityId: match[0] };
}

async function loadMiniAppGroupsForCommunity(resolved, overrides = {}) {
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const rows = await getSheetDataImpl('ГРУППЫ', resolved.communityId, resolved.profileId);
    return normalizeMiniAppGroupRows(rows);
}

async function handleMiniAppRequestWithDependencies(event, overrides = {}) {
    const q = event.queryStringParameters || event.query || {};
    const profileId = getRequestProfileId(q, {});
    const resolved = await resolveMiniAppCommunity(q.c, profileId, overrides);
    if (!resolved) return miniAppJson(404, { success: false, error: 'community_not_found', message: 'Сообщество не найдено' });

    const groups = await loadMiniAppGroupsForCommunity(resolved, overrides);
    if (event.httpMethod === 'GET' && q.miniapp === 'groups') {
        return miniAppJson(200, { success: true, communityId: resolved.communityId, groups: listVisibleMiniAppGroups(groups) });
    }
    if (event.httpMethod === 'GET' && q.miniapp === 'group') {
        const group = findMiniAppGroupBySlug(groups, q.g);
        if (!group) return miniAppJson(404, { success: false, error: 'group_not_found', message: 'Группа не найдена' });
        return miniAppJson(200, { success: true, communityId: resolved.communityId, group: toDetailDto(group) });
    }
    return miniAppJson(404, { success: false, error: 'miniapp_route_not_found', message: 'Mini App route not found' });
}
```

Route it in `handler(event)` and `handlePostRequest(event)`:

```js
if (q.miniapp !== undefined) {
    return handleMiniAppRequestWithDependencies(event);
}
```

Export it in `__testOnly`.

- [ ] **Step 4: Run read API tests**

Run:

```powershell
node --test tests/miniapp-api.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/handler.js tests/miniapp-api.test.js
git commit -m "Add Mini App group read API"
```

---

### Task 5: Mini App Subscribe And Unsubscribe API

**Files:**
- Modify: `src/modules/users.js`
- Modify: `src/handler.js`
- Modify: `tests/miniapp-api.test.js`

- [ ] **Step 1: Add failing mutation tests**

Append to `tests/miniapp-api.test.js`:

```js
const { signVkLaunchParams } = require('../src/modules/miniapp-auth');

test('Mini App subscribe verifies launch params and adds group', async () => {
  const secret = 'miniapp-secret';
  const launch = { vk_app_id: '999', vk_user_id: '123', vk_group_id: '229445618' };
  const calls = [];
  const response = await __testOnly.handleMiniAppRequestWithDependencies({
    httpMethod: 'POST',
    queryStringParameters: { miniapp: 'subscribe', c: '229445618', g: 'vip' },
    body: JSON.stringify({ launchParams: { ...launch, sign: signVkLaunchParams(launch, secret) } })
  }, {
    miniAppSecret: secret,
    getSheetData: async () => [
      { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' }
    ],
    resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' }),
    ensureMiniAppUser: async (userId, communityId, profileId) => calls.push(['ensure', userId, communityId, profileId]),
    updateUserGroups: async (userId, add, remove, communityId, profileId) => calls.push(['groups', userId, add, remove, communityId, profileId])
  });

  assert.equal(response.statusCode, 200);
  assert.equal(parse(response).subscribed, true);
  assert.deepEqual(calls, [
    ['ensure', '123', '229445618', '1'],
    ['groups', '123', 'vip', '', '229445618', '1']
  ]);
});

test('Mini App subscribe rejects invalid launch signature', async () => {
  const response = await __testOnly.handleMiniAppRequestWithDependencies({
    httpMethod: 'POST',
    queryStringParameters: { miniapp: 'subscribe', c: '229445618', g: 'vip' },
    body: JSON.stringify({ launchParams: { vk_user_id: '123', sign: 'bad' } })
  }, {
    miniAppSecret: 'miniapp-secret',
    getSheetData: async () => [
      { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' }
    ],
    resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
  });

  assert.equal(response.statusCode, 401);
  assert.equal(parse(response).error, 'invalid_vk_sign');
});

test('Mini App unsubscribe removes group idempotently', async () => {
  const secret = 'miniapp-secret';
  const launch = { vk_app_id: '999', vk_user_id: '123', vk_group_id: '229445618' };
  const calls = [];
  const response = await __testOnly.handleMiniAppRequestWithDependencies({
    httpMethod: 'POST',
    queryStringParameters: { miniapp: 'unsubscribe', c: '229445618', g: 'vip' },
    body: JSON.stringify({ launchParams: { ...launch, sign: signVkLaunchParams(launch, secret) } })
  }, {
    miniAppSecret: secret,
    getSheetData: async () => [
      { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' }
    ],
    resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' }),
    updateUserGroups: async (userId, add, remove, communityId, profileId) => calls.push(['groups', userId, add, remove, communityId, profileId])
  });

  assert.equal(response.statusCode, 200);
  assert.equal(parse(response).subscribed, false);
  assert.deepEqual(calls, [
    ['groups', '123', '', 'vip', '229445618', '1']
  ]);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
node --test tests/miniapp-api.test.js
```

Expected: FAIL because mutation routes are not implemented.

- [ ] **Step 3: Add Mini App user creation helper**

In `src/modules/users.js`, add:

```js
async function ensureMiniAppUserWithDependencies(userId, communityId = null, profileId = '1', overrides = {}) {
    const existing = await getUserRowWithDependencies(userId, communityId, profileId, overrides);
    if (existing) return existing;
    const row = {
        [COLUMN_ID]: normalizeUserId(userId),
        [COLUMN_NAME]: `VK ${normalizeUserId(userId)}`,
        [COLUMN_CONSENTS_SUMMARY]: '',
        [COLUMN_CONSENTS_JSON]: '',
        [COLUMN_GROUPS]: '',
        [COLUMN_USER_VARIABLE_NAMES]: '',
        [COLUMN_USER_VARIABLE_VALUES]: '',
        [COLUMN_SHARED_VARIABLE_NAMES]: '',
        [COLUMN_SHARED_VARIABLE_VALUES]: '',
        [COLUMN_CURRENT_BOT]: '',
        [COLUMN_CURRENT_STEP]: '',
        [COLUMN_SENT_STEPS]: '',
        [COLUMN_GROUP_HISTORY]: '{}'
    };
    return updateUserRowWithDependencies(userId, row, communityId, profileId, overrides);
}

async function ensureMiniAppUser(userId, communityId = null, profileId = '1') {
    return ensureMiniAppUserWithDependencies(userId, communityId, profileId);
}
```

Export both in `module.exports` and `__testOnly`.

- [ ] **Step 4: Implement mutation routes**

In `src/handler.js`, import:

```js
const { verifyVkLaunchParams } = require('./modules/miniapp-auth');
const { ensureMiniAppUser, updateUserGroups } = require('./modules/users');
```

Add helpers:

```js
function parseJsonBody(event) {
    try {
        return JSON.parse(event.body || '{}');
    } catch (error) {
        return {};
    }
}

function verifyMiniAppRequest(body, overrides = {}) {
    return verifyVkLaunchParams(body.launchParams || {}, {
        secret: overrides.miniAppSecret || process.env.VK_MINIAPP_SECRET
    });
}
```

Extend `handleMiniAppRequestWithDependencies`:

```js
if (event.httpMethod === 'POST' && (q.miniapp === 'subscribe' || q.miniapp === 'unsubscribe')) {
    const body = parseJsonBody(event);
    const auth = verifyMiniAppRequest(body, overrides);
    if (!auth.ok) return miniAppJson(401, { success: false, error: auth.error, message: 'Не удалось подтвердить пользователя VK' });
    if (auth.groupId && String(auth.groupId) !== String(q.c)) {
        return miniAppJson(403, { success: false, error: 'community_mismatch', message: 'Сообщество Mini App не совпадает со ссылкой' });
    }
    const group = findMiniAppGroupBySlug(groups, q.g);
    if (!group) return miniAppJson(404, { success: false, error: 'group_not_found', message: 'Группа не найдена' });
    const ensureUserImpl = overrides.ensureMiniAppUser || ensureMiniAppUser;
    const updateGroupsImpl = overrides.updateUserGroups || updateUserGroups;
    if (q.miniapp === 'subscribe') {
        await ensureUserImpl(auth.userId, resolved.communityId, resolved.profileId);
        await updateGroupsImpl(auth.userId, group.groupName, '', resolved.communityId, resolved.profileId);
        return miniAppJson(200, { success: true, subscribed: true, group: toDetailDto(group, true) });
    }
    await updateGroupsImpl(auth.userId, '', group.groupName, resolved.communityId, resolved.profileId);
    return miniAppJson(200, { success: true, subscribed: false, group: toDetailDto(group, false) });
}
```

- [ ] **Step 5: Run mutation tests**

Run:

```powershell
node --test tests/miniapp-api.test.js
```

Expected: PASS.

- [ ] **Step 6: Run related user tests**

Run:

```powershell
node --test tests/users-hot-state.test.js tests/user-state-store.test.js
```

Expected: PASS.

- [ ] **Step 7: Commit**

```powershell
git add src/modules/users.js src/handler.js tests/miniapp-api.test.js
git commit -m "Add Mini App subscription API"
```

---

### Task 6: Admin Panel Group Visual Settings

**Files:**
- Modify: `adminPanelHTML.js`
- Modify: `tests/admin-panel-groups-ui.test.js`

- [ ] **Step 1: Add failing admin panel tests**

Extend `tests/admin-panel-groups-ui.test.js`:

```js
test('groups tab exposes Mini App visual settings', () => {
  assert.match(adminPanelHTML, /MiniApp включен/);
  assert.match(adminPanelHTML, /MiniApp скрыть из списка/);
  assert.match(adminPanelHTML, /MiniApp slug/);
  assert.match(adminPanelHTML, /MiniApp заголовок/);
  assert.match(adminPanelHTML, /MiniApp описание/);
  assert.match(adminPanelHTML, /MiniApp иконка URL/);
  assert.match(adminPanelHTML, /MiniApp баннер URL/);
  assert.match(adminPanelHTML, /MiniApp текст подписки/);
  assert.match(adminPanelHTML, /MiniApp текст отписки/);
});

test('groups tab renders generated Mini App links', () => {
  assert.match(adminPanelHTML, /buildMiniAppGroupLink/);
  assert.match(adminPanelHTML, /VK_MINIAPP_APP_URL/);
  assert.match(adminPanelHTML, /#c=/);
  assert.match(adminPanelHTML, /&g=/);
});

test('groups tab includes image upload controls for Mini App assets', () => {
  assert.match(adminPanelHTML, /uploadMiniAppGroupImage/);
  assert.match(adminPanelHTML, /miniappUploadAsset/);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
node --test tests/admin-panel-groups-ui.test.js
```

Expected: FAIL because the new strings/functions are not present.

- [ ] **Step 3: Add group form fields**

In `adminPanelHTML.js`, update `renderGroupFormPanel()` so the form includes existing fields and a Mini App section:

```js
var miniAppEnabled = row['MiniApp включен'] || '';
var miniAppHidden = row['MiniApp скрыть из списка'] || '';
var miniAppSlug = row['MiniApp slug'] || '';
var miniAppTitle = row['MiniApp заголовок'] || '';
var miniAppDescription = row['MiniApp описание'] || '';
var miniAppIconUrl = row['MiniApp иконка URL'] || '';
var miniAppIconFile = row['MiniApp иконка файл'] || '';
var miniAppBannerUrl = row['MiniApp баннер URL'] || '';
var miniAppBannerFile = row['MiniApp баннер файл'] || '';
var miniAppSubscribeText = row['MiniApp текст подписки'] || 'Подписаться';
var miniAppUnsubscribeText = row['MiniApp текст отписки'] || 'Отписаться';
```

Add controls:

```js
'<details class="settings-surface" open style="margin-top:12px;">' +
  '<summary style="cursor:pointer;font-weight:700;">Визуал Mini App</summary>' +
  '<div class="profile-form-grid" style="margin-top:12px;">' +
    '<label><input id="groupFormMiniAppEnabled" type="checkbox" ' + (miniAppEnabled ? 'checked' : '') + '> MiniApp включен</label>' +
    '<label><input id="groupFormMiniAppHidden" type="checkbox" ' + (miniAppHidden ? 'checked' : '') + '> MiniApp скрыть из списка</label>' +
    '<div><label><strong>MiniApp slug</strong></label><input id="groupFormMiniAppSlug" type="text" value="' + escapeHtml(miniAppSlug) + '" placeholder="vip"></div>' +
    '<div><label><strong>MiniApp заголовок</strong></label><input id="groupFormMiniAppTitle" type="text" value="' + escapeHtml(miniAppTitle) + '" placeholder="Заголовок для пользователя"></div>' +
    '<div style="grid-column:1/-1;"><label><strong>MiniApp описание</strong></label><textarea id="groupFormMiniAppDescription" rows="3">' + escapeHtml(miniAppDescription) + '</textarea></div>' +
    '<div><label><strong>MiniApp иконка URL</strong></label><input id="groupFormMiniAppIconUrl" type="url" value="' + escapeHtml(miniAppIconUrl) + '"></div>' +
    '<div><label><strong>MiniApp баннер URL</strong></label><input id="groupFormMiniAppBannerUrl" type="url" value="' + escapeHtml(miniAppBannerUrl) + '"></div>' +
    '<div><label><strong>MiniApp иконка файл</strong></label><input id="groupFormMiniAppIconFile" type="text" readonly value="' + escapeHtml(miniAppIconFile) + '"><button class="btn btn-info group-action-btn" type="button" onclick="uploadMiniAppGroupImage(&quot;icon&quot;)">Загрузить</button></div>' +
    '<div><label><strong>MiniApp баннер файл</strong></label><input id="groupFormMiniAppBannerFile" type="text" readonly value="' + escapeHtml(miniAppBannerFile) + '"><button class="btn btn-info group-action-btn" type="button" onclick="uploadMiniAppGroupImage(&quot;banner&quot;)">Загрузить</button></div>' +
    '<div><label><strong>MiniApp текст подписки</strong></label><input id="groupFormMiniAppSubscribeText" type="text" value="' + escapeHtml(miniAppSubscribeText) + '"></div>' +
    '<div><label><strong>MiniApp текст отписки</strong></label><input id="groupFormMiniAppUnsubscribeText" type="text" value="' + escapeHtml(miniAppUnsubscribeText) + '"></div>' +
  '</div>' +
'</details>'
```

- [ ] **Step 4: Save new fields**

In `saveGroupForm()`, include:

```js
row['MiniApp включен'] = document.getElementById('groupFormMiniAppEnabled')?.checked ? 'да' : '';
row['MiniApp скрыть из списка'] = document.getElementById('groupFormMiniAppHidden')?.checked ? 'да' : '';
row['MiniApp slug'] = getValue('groupFormMiniAppSlug');
row['MiniApp заголовок'] = getValue('groupFormMiniAppTitle');
row['MiniApp описание'] = getValue('groupFormMiniAppDescription');
row['MiniApp иконка URL'] = getValue('groupFormMiniAppIconUrl');
row['MiniApp иконка файл'] = getValue('groupFormMiniAppIconFile');
row['MiniApp баннер URL'] = getValue('groupFormMiniAppBannerUrl');
row['MiniApp баннер файл'] = getValue('groupFormMiniAppBannerFile');
row['MiniApp текст подписки'] = getValue('groupFormMiniAppSubscribeText') || 'Подписаться';
row['MiniApp текст отписки'] = getValue('groupFormMiniAppUnsubscribeText') || 'Отписаться';
```

- [ ] **Step 5: Add generated link helper**

Add to `adminPanelHTML.js`:

```js
function getMiniAppBaseUrl() {
    return window.VK_MINIAPP_APP_URL || localStorage.getItem('VK_MINIAPP_APP_URL') || 'https://vk.com/appXXXX';
}

function buildMiniAppGroupLink(row) {
    var communityConfig = getCurrentCommunityConfig ? getCurrentCommunityConfig() : {};
    var vkGroupId = communityConfig.vk_group_id || window.currentCommunityId || '';
    var slug = String(row['MiniApp slug'] || row['Группа'] || '').trim();
    var base = getMiniAppBaseUrl();
    if (!vkGroupId) return '';
    return base + '#c=' + encodeURIComponent(vkGroupId) + (slug ? '&g=' + encodeURIComponent(slug) : '');
}
```

Render the link on group cards:

```js
var miniAppLink = buildMiniAppGroupLink(group);
var miniAppLinkHtml = miniAppLink
    ? '<div class="profile-card-row"><span class="profile-card-label">Mini App:</span> <input readonly value="' + escapeHtml(miniAppLink) + '" onclick="this.select()"></div>'
    : '';
```

- [ ] **Step 6: Add image upload frontend hook**

Add a small upload function that posts base64 image data:

```js
window.uploadMiniAppGroupImage = async function(kind) {
    var input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/png,image/jpeg,image/webp';
    input.onchange = async function() {
        var file = input.files && input.files[0];
        if (!file) return;
        var dataUrl = await new Promise(function(resolve, reject) {
            var reader = new FileReader();
            reader.onload = function() { resolve(String(reader.result || '')); };
            reader.onerror = reject;
            reader.readAsDataURL(file);
        });
        var baseUrl = window.location.href.split('?')[0];
        var response = await fetch(baseUrl + '?miniappUploadAsset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({
                fileName: file.name,
                contentType: file.type,
                dataUrl: dataUrl,
                communityId: window.currentCommunityId,
                profileId: getCurrentProfileId()
            })
        });
        var payload = await response.json();
        if (!payload.success) throw new Error(payload.error || 'Не удалось загрузить изображение');
        var targetId = kind === 'banner' ? 'groupFormMiniAppBannerFile' : 'groupFormMiniAppIconFile';
        var target = document.getElementById(targetId);
        if (target) target.value = payload.url;
    };
    input.click();
};
```

Add the `miniappUploadAsset` backend action to `needsAdminSession` because uploads are admin-only.

- [ ] **Step 7: Run admin tests**

Run:

```powershell
node --test tests/admin-panel-groups-ui.test.js
```

Expected: PASS.

- [ ] **Step 8: Commit**

```powershell
git add adminPanelHTML.js src/handler.js tests/admin-panel-groups-ui.test.js
git commit -m "Add Mini App group settings UI"
```

---

### Task 7: Mini App Frontend Scaffold

**Files:**
- Create: `miniapp/package.json`
- Create: `miniapp/index.html`
- Create: `miniapp/src/main.jsx`
- Create: `miniapp/src/App.jsx`
- Create: `miniapp/src/api.js`
- Create: `miniapp/src/vk.js`
- Create: `miniapp/src/styles.css`

- [ ] **Step 1: Create frontend package files**

Create `miniapp/package.json`:

```json
{
  "name": "papa-bot-vk-miniapp",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite --host 127.0.0.1",
    "build": "vite build",
    "preview": "vite preview --host 127.0.0.1"
  },
  "dependencies": {
    "@vitejs/plugin-react": "^5.0.0",
    "vite": "^7.0.0",
    "react": "^19.0.0",
    "react-dom": "^19.0.0",
    "@vkontakte/vk-bridge": "^2.15.0"
  },
  "devDependencies": {}
}
```

Create `miniapp/index.html`:

```html
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>PAPA BOT Mini App</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
```

- [ ] **Step 2: Create API client**

Create `miniapp/src/api.js`:

```js
const API_BASE = import.meta.env.VITE_PAPA_BOT_API_URL || '';

function buildUrl(path, params = {}) {
  const url = new URL(path, API_BASE || window.location.origin);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, value);
  });
  return url.toString();
}

async function readJson(response) {
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.success === false) {
    throw new Error(data.message || data.error || 'Ошибка Mini App');
  }
  return data;
}

export function loadGroups(communityId) {
  return fetch(buildUrl('/', { miniapp: 'groups', c: communityId })).then(readJson);
}

export function loadGroup(communityId, slug) {
  return fetch(buildUrl('/', { miniapp: 'group', c: communityId, g: slug })).then(readJson);
}

export function subscribeGroup(communityId, slug, launchParams) {
  return fetch(buildUrl('/', { miniapp: 'subscribe', c: communityId, g: slug }), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ launchParams })
  }).then(readJson);
}

export function unsubscribeGroup(communityId, slug, launchParams) {
  return fetch(buildUrl('/', { miniapp: 'unsubscribe', c: communityId, g: slug }), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ launchParams })
  }).then(readJson);
}
```

- [ ] **Step 3: Create VK Bridge helper**

Create `miniapp/src/vk.js`:

```js
import bridge from '@vkontakte/vk-bridge';

export function initVkBridge() {
  bridge.send('VKWebAppInit').catch(() => {});
}

export function getLaunchParams() {
  return Object.fromEntries(new URLSearchParams(window.location.search));
}

export function getRouteParams() {
  const hash = window.location.hash.replace(/^#/, '');
  return Object.fromEntries(new URLSearchParams(hash));
}

export async function allowMessagesFromGroup(groupId) {
  try {
    await bridge.send('VKWebAppAllowMessagesFromGroup', { group_id: Number(groupId) });
    return true;
  } catch (error) {
    return false;
  }
}
```

- [ ] **Step 4: Create React app**

Create `miniapp/src/main.jsx`:

```jsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App.jsx';
import './styles.css';

createRoot(document.getElementById('root')).render(<App />);
```

Create `miniapp/src/App.jsx`:

```jsx
import { useEffect, useMemo, useState } from 'react';
import { loadGroup, loadGroups, subscribeGroup, unsubscribeGroup } from './api.js';
import { allowMessagesFromGroup, getLaunchParams, getRouteParams, initVkBridge } from './vk.js';

export default function App() {
  const [state, setState] = useState({ loading: true, error: '', groups: [], group: null });
  const [route, setRoute] = useState(getRouteParams());
  const launchParams = useMemo(() => getLaunchParams(), []);
  const communityId = route.c || '';
  const slug = route.g || '';

  useEffect(() => {
    initVkBridge();
    const onHash = () => setRoute(getRouteParams());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(() => {
    if (!communityId) {
      setState({ loading: false, error: 'Откройте Mini App по ссылке сообщества', groups: [], group: null });
      return;
    }
    setState(prev => ({ ...prev, loading: true, error: '' }));
    const request = slug ? loadGroup(communityId, slug) : loadGroups(communityId);
    request.then(data => {
      setState({ loading: false, error: '', groups: data.groups || [], group: data.group || null });
    }).catch(error => {
      setState({ loading: false, error: error.message, groups: [], group: null });
    });
  }, [communityId, slug]);

  async function handleToggle() {
    if (!state.group) return;
    try {
      if (state.group.subscribed) {
        const data = await unsubscribeGroup(communityId, state.group.slug, launchParams);
        setState(prev => ({ ...prev, group: data.group }));
        return;
      }
      const allowed = await allowMessagesFromGroup(communityId);
      if (!allowed) {
        setState(prev => ({ ...prev, error: 'Для подписки разрешите сообщения от сообщества' }));
        return;
      }
      const data = await subscribeGroup(communityId, state.group.slug, launchParams);
      setState(prev => ({ ...prev, group: data.group }));
    } catch (error) {
      setState(prev => ({ ...prev, error: error.message }));
    }
  }

  if (state.loading) return <main className="app"><div className="notice">Загрузка</div></main>;
  if (state.group) {
    const buttonText = state.group.subscribed ? state.group.unsubscribeText : state.group.subscribeText;
    return (
      <main className="app">
        {state.group.bannerUrl ? <img className="banner" src={state.group.bannerUrl} alt="" /> : null}
        <section className="detail">
          <h1>{state.group.title}</h1>
          <p>{state.group.description}</p>
          {state.error ? <div className="error">{state.error}</div> : null}
          <button className="primary" type="button" onClick={handleToggle}>{buttonText}</button>
        </section>
      </main>
    );
  }
  return (
    <main className="app">
      <h1>Группы</h1>
      {state.error ? <div className="error">{state.error}</div> : null}
      <div className="grid">
        {state.groups.map(group => (
          <a className="card" key={group.slug} href={`#c=${encodeURIComponent(communityId)}&g=${encodeURIComponent(group.slug)}`}>
            {group.iconUrl ? <img src={group.iconUrl} alt="" /> : <div className="placeholder" />}
            <div>
              <h2>{group.title}</h2>
              <p>{group.description}</p>
            </div>
          </a>
        ))}
      </div>
    </main>
  );
}
```

- [ ] **Step 5: Add CSS**

Create `miniapp/src/styles.css`:

```css
:root {
  color: #18202a;
  background: #f4f6f8;
  font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
}

.app {
  width: min(720px, 100%);
  margin: 0 auto;
  padding: 16px;
}

.grid {
  display: grid;
  gap: 10px;
}

.card {
  display: grid;
  grid-template-columns: 64px 1fr;
  gap: 12px;
  align-items: center;
  min-height: 88px;
  padding: 12px;
  color: inherit;
  text-decoration: none;
  background: #fff;
  border: 1px solid #d7dde5;
  border-radius: 8px;
}

.card img,
.placeholder {
  width: 64px;
  height: 64px;
  border-radius: 8px;
  object-fit: cover;
  background: #dfe6ee;
}

.card h2 {
  margin: 0 0 4px;
  font-size: 17px;
}

.card p,
.detail p {
  margin: 0;
  color: #536273;
  line-height: 1.45;
}

.banner {
  width: 100%;
  aspect-ratio: 16 / 9;
  object-fit: cover;
  border-radius: 8px;
  background: #dfe6ee;
}

.detail {
  padding: 16px 0;
}

.detail h1 {
  margin: 0 0 8px;
  font-size: 28px;
}

.primary {
  width: 100%;
  min-height: 48px;
  margin-top: 18px;
  border: 0;
  border-radius: 8px;
  background: #2f80ed;
  color: #fff;
  font-size: 17px;
  font-weight: 700;
}

.error,
.notice {
  padding: 12px;
  margin: 12px 0;
  border-radius: 8px;
  background: #fff4d6;
  color: #5c4300;
}
```

- [ ] **Step 6: Install and build**

Run:

```powershell
Set-Location miniapp
npm install
npm run build
```

Expected: `dist` is created and Vite build succeeds.

- [ ] **Step 7: Commit**

```powershell
git add miniapp
git commit -m "Add VK Mini App frontend"
```

---

### Task 8: Functionality Documentation And Verification

**Files:**
- Modify: `FUNCTIONALITY.md`
- Optional modify: `README.md` if deployment steps for Mini App are added

- [ ] **Step 1: Update functionality registry**

Add a section or bullets to `FUNCTIONALITY.md` covering:

```markdown
## VK Mini App Groups

- Есть отдельный VK Mini App frontend на Vite + React + VK Bridge.
- Mini App открывает список групп сообщества по ссылке `vk.com/appXXXX#c=<vk_group_id>`.
- Mini App открывает конкретную группу по ссылке `vk.com/appXXXX#c=<vk_group_id>&g=<slug>`.
- Скрытые группы не отображаются в списке, но доступны по прямой ссылке.
- В админке группы настраиваются Mini App slug, публичный заголовок, описание, иконка, баннер и тексты кнопки подписки/отписки.
- Подписка требует разрешение сообщений от сообщества через VK Bridge.
- Подписка добавляет существующую группу PAPA BOT в данные пользователя.
- Отписка удаляет существующую группу PAPA BOT из данных пользователя.
- Backend Mini App API проверяет подпись VK launch params для пользовательских действий.
```

- [ ] **Step 2: Run backend tests**

Run:

```powershell
node --test tests/miniapp-groups.test.js tests/miniapp-auth.test.js tests/miniapp-assets.test.js tests/miniapp-api.test.js tests/admin-panel-groups-ui.test.js
```

Expected: PASS.

- [ ] **Step 3: Run frontend build**

Run:

```powershell
Set-Location miniapp
npm run build
```

Expected: PASS.

- [ ] **Step 4: Manual local check**

Start frontend dev server:

```powershell
Set-Location miniapp
npm run dev
```

Open:

```text
http://127.0.0.1:5173/#c=229445618
http://127.0.0.1:5173/#c=229445618&g=vip
```

Expected:

- missing backend data shows a readable error, not a blank screen;
- route parsing works;
- list/detail layouts fit mobile width.

- [ ] **Step 5: Commit**

```powershell
git add FUNCTIONALITY.md README.md
git commit -m "Document VK Mini App groups"
```

If `README.md` was not changed, omit it from `git add`.

---

## Final Verification

- [ ] Run all new backend tests:

```powershell
node --test tests/miniapp-groups.test.js tests/miniapp-auth.test.js tests/miniapp-assets.test.js tests/miniapp-api.test.js tests/admin-panel-groups-ui.test.js
```

- [ ] Build Mini App:

```powershell
Set-Location miniapp
npm run build
```

- [ ] Check git status:

```powershell
git status --short
```

Expected: only unrelated pre-existing dirty files remain, or working tree is clean if this branch has no unrelated local changes.

## Self-Review

Spec coverage:

- Shared single Mini App: Task 7.
- Separate Vite/React frontend: Task 7.
- Group list and direct hidden group links: Tasks 1, 4, 7.
- Subscribe/unsubscribe: Task 5 and Task 7.
- VK launch signature verification: Task 2 and Task 5.
- Message permission gate: Task 7.
- Admin visual settings: Task 6.
- Icon/banner URL and upload: Task 3 and Task 6.
- `FUNCTIONALITY.md`: Task 8.

Placeholder scan:

- No `TBD`, `TODO`, `implement later`, or unspecified test steps are present.

Type and naming consistency:

- Backend group fields use the same names across `miniapp-groups.js`, admin tests, and admin form save.
- Route names use `miniapp=groups`, `miniapp=group`, `miniapp=subscribe`, and `miniapp=unsubscribe` consistently.
- Frontend API client uses the same route names as backend tests.
