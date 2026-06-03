# Robokassa Payment Buttons Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production-only Robokassa support for PAPA BOT bot payment buttons with signed payment links, server-confirmed success routing, browser-return fail routing, and admin profile configuration.

**Architecture:** Extend the existing payment integration registry and payment-button creator with Robokassa credential normalization, hash helpers, numeric invoice IDs, and signed payment URLs. Add a focused `robokassa-payments.js` module for public ResultURL and browser-return processing, then wire those endpoints through `src/handler.js`. Keep Robokassa out of profile balance top-ups and keep the existing payment-button editor unchanged.

**Tech Stack:** Node.js CommonJS, built-in `crypto`, `node:test`, existing PAPA BOT profile dashboard storage, existing payment-button routing helpers, Robokassa standard payment interface.

---

## Scope Check

This is one connected feature:

- the profile integration UI stores credentials needed to sign Robokassa requests;
- payment buttons use those credentials to create links;
- public handlers use the same credentials and stored payment records to route users.

Do not split this into separate feature branches because the stored field names, signature rules, and endpoint markers must remain identical across the UI, payment creator, handler, and tests.

## File Structure

- Modify `src/modules/payment-integrations.js`: normalize and validate Robokassa settings, calculate signatures, generate numeric `InvId` values, build Robokassa URLs, create pending payment records, and make the TEST action meaningful without a network request.
- Create `src/modules/robokassa-payments.js`: parse Robokassa GET/POST parameters, verify ResultURL and return signatures, enforce amount/profile/integration checks, route success/fail steps, and preserve idempotency/state priority.
- Modify `src/handler.js`: expose public Robokassa ResultURL and browser-return routes before admin-session-protected handling.
- Modify `adminPanelHTML.js`: make Robokassa selectable, render the hash algorithm field and warnings, auto-generate the Result URL, and preserve the field in form read/edit/save flows.
- Modify `tests/payment-integrations.test.js`: cover integration normalization, validation, signatures, payment-link generation, pending storage, and TEST output.
- Create `tests/robokassa-payments.test.js`: cover ResultURL, return handling, rejection cases, idempotency, and success-over-fail priority.
- Modify `tests/admin-auth-flow.test.js`: cover public handler routing and HTTP response shapes.
- Modify `tests/admin-panel-consents-contract.test.js`: cover Robokassa profile UI contracts.
- Modify `FUNCTIONALITY.md`: document implemented Robokassa behavior.

Use `node --test tests/<file>.test.js` because the root `package.json` has no test script.

---

### Task 1: Robokassa Integration Fields And Hash Helpers

**Files:**
- Modify: `src/modules/payment-integrations.js`
- Modify: `tests/payment-integrations.test.js`

- [ ] **Step 1: Write failing tests for Robokassa normalization, validation, and signatures**

Add tests to `tests/payment-integrations.test.js`:

```js
test('savePaymentIntegrations normalizes Robokassa hash algorithm', async () => {
  const saved = [];
  const result = await payment.savePaymentIntegrations(
    '7',
    [{
      name: 'Robot',
      provider: 'robokassa',
      merchantLogin: ' merchant ',
      password1: ' pass-1 ',
      password2: ' pass-2 ',
      robokassaHashAlgorithm: ' SHA-256 '
    }],
    {
      loadDashboardData: async () => ({ profiles: { 7: { profileId: '7' } } }),
      saveDashboardData: async data => saved.push(data)
    }
  );

  assert.equal(result[0].robokassaHashAlgorithm, 'sha256');
  assert.equal(saved[0].profiles['7'].paymentIntegrations[0].robokassaHashAlgorithm, 'sha256');
});

test('savePaymentIntegrations rejects Robokassa without a supported hash algorithm', async () => {
  await assert.rejects(
    () => payment.savePaymentIntegrations(
      '7',
      [{
        name: 'Robot',
        provider: 'robokassa',
        merchantLogin: 'merchant',
        password1: 'pass-1',
        password2: 'pass-2',
        robokassaHashAlgorithm: 'sha1'
      }],
      {
        loadDashboardData: async () => ({ profiles: { 7: { profileId: '7' } } }),
        saveDashboardData: async () => {}
      }
    ),
    /Signature algorithm/
  );
});

test('createRobokassaSignature supports configured hash algorithms', () => {
  assert.equal(
    payment.createRobokassaSignature('merchant:10.00:123:pass-1', 'md5'),
    require('crypto').createHash('md5').update('merchant:10.00:123:pass-1').digest('hex')
  );
  assert.equal(
    payment.createRobokassaSignature('merchant:10.00:123:pass-1', 'sha256'),
    require('crypto').createHash('sha256').update('merchant:10.00:123:pass-1').digest('hex')
  );
  assert.equal(
    payment.createRobokassaSignature('merchant:10.00:123:pass-1', 'sha512'),
    require('crypto').createHash('sha512').update('merchant:10.00:123:pass-1').digest('hex')
  );
});
```

- [ ] **Step 2: Run the focused test file and verify it fails**

Run:

```powershell
node --test tests/payment-integrations.test.js
```

Expected: FAIL because `robokassaHashAlgorithm` is not normalized or required and `createRobokassaSignature` is not exported.

- [ ] **Step 3: Add Robokassa field metadata and normalization helpers**

In `src/modules/payment-integrations.js`, extend the Robokassa preset and add helpers:

```js
{
  id: 'robokassa',
  label: 'Robokassa',
  requiredFields: ['merchantLogin', 'password1', 'password2', 'robokassaHashAlgorithm'],
  fields: [
    { key: 'merchantLogin', label: 'Merchant login', required: true },
    { key: 'password1', label: 'Password 1', required: true },
    { key: 'password2', label: 'Password 2', required: true },
    { key: 'robokassaHashAlgorithm', label: 'Signature algorithm', required: true }
  ]
}
```

Add:

```js
const ROBOKASSA_HASH_ALGORITHMS = new Set(['md5', 'sha256', 'sha512']);

function normalizeRobokassaHashAlgorithm(value) {
  return normalizeText(value).toLowerCase().replace(/[^a-z0-9]/g, '');
}

function createRobokassaSignature(value, algorithm = '') {
  const normalized = normalizeRobokassaHashAlgorithm(algorithm);
  if (!ROBOKASSA_HASH_ALGORITHMS.has(normalized)) {
    throw new Error('Robokassa: unsupported Signature algorithm');
  }
  return crypto.createHash(normalized).update(String(value || ''), 'utf8').digest('hex');
}
```

Store the normalized field in `normalizePaymentIntegration`:

```js
robokassaHashAlgorithm: normalizeRobokassaHashAlgorithm(entry.robokassaHashAlgorithm),
```

Export:

```js
normalizeRobokassaHashAlgorithm,
createRobokassaSignature,
```

- [ ] **Step 4: Run the focused test file and verify it passes**

Run:

```powershell
node --test tests/payment-integrations.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit the integration contract**

```powershell
git add -- src/modules/payment-integrations.js tests/payment-integrations.test.js
git commit -m "Add Robokassa integration hash settings"
```

---

### Task 2: Robokassa Signed Payment Link Creation

**Files:**
- Modify: `src/modules/payment-integrations.js`
- Modify: `tests/payment-integrations.test.js`

- [ ] **Step 1: Write failing tests for numeric invoice IDs and signed links**

Add tests to `tests/payment-integrations.test.js`:

```js
test('makeRobokassaInvoiceId creates a positive numeric invoice id', () => {
  assert.equal(payment.makeRobokassaInvoiceId(() => 123456), '123456');
  assert.throws(() => payment.makeRobokassaInvoiceId(() => 'not-numeric'), /numeric InvId/);
});

test('createPaymentButtonPayment creates a signed Robokassa payment link', async () => {
  const saved = [];
  const result = await payment.createPaymentButtonPayment(
    '7',
    {
      paymentIntegrationId: 'robo_main',
      amountRub: 990,
      description: 'Order 42',
      successBot: 'Sales',
      successStep: 'Paid',
      failBot: 'Sales',
      failStep: 'Payment failed'
    },
    {
      communityId: '229445618',
      userId: '42',
      appUrl: 'https://functions.example/papa?admin=1'
    },
    {
      invoiceIdFactory: () => 123456,
      now: () => new Date('2026-06-03T10:00:00.000Z'),
      loadDashboardData: async () => ({
        profiles: {
          7: {
            profileId: '7',
            paymentIntegrations: [{
              id: 'robo_main',
              name: 'Robokassa Main',
              provider: 'robokassa',
              merchantLogin: 'merchant',
              password1: 'pass-1',
              password2: 'pass-2',
              robokassaHashAlgorithm: 'sha256'
            }]
          }
        }
      }),
      saveDashboardData: async data => saved.push(data)
    }
  );

  const url = new URL(result.confirmationUrl);
  assert.equal(result.provider, 'robokassa');
  assert.equal(result.paymentId, '123456');
  assert.equal(url.origin + url.pathname, 'https://auth.robokassa.ru/Merchant/Index.aspx');
  assert.equal(url.searchParams.get('MerchantLogin'), 'merchant');
  assert.equal(url.searchParams.get('OutSum'), '990.00');
  assert.equal(url.searchParams.get('InvId'), '123456');
  assert.equal(url.searchParams.get('Description'), 'Order 42');
  assert.equal(url.searchParams.get('Culture'), 'ru');
  assert.equal(url.searchParams.get('Shp_profileId'), '7');
  assert.equal(url.searchParams.get('Shp_integrationId'), 'robo_main');
  assert.equal(url.searchParams.get('Shp_paymentId'), '123456');
  assert.equal(url.searchParams.get('SuccessUrl2Method'), 'GET');
  assert.equal(url.searchParams.get('FailUrl2Method'), 'GET');
  assert.equal(url.searchParams.has('IsTest'), false);
  assert.equal(url.searchParams.has('Receipt'), false);
  assert.equal(saved[0].profiles['7'].paymentButtonPayments[0].status, 'pending');
  assert.equal(saved[0].profiles['7'].paymentButtonPayments[0].paymentId, '123456');
});
```

Also assert the exact signature base using exported helpers:

```js
const successUrl = url.searchParams.get('SuccessUrl2');
const failUrl = url.searchParams.get('FailUrl2');
const shp = [
  'Shp_integrationId=robo_main',
  'Shp_paymentId=123456',
  'Shp_profileId=7'
].join(':');
const signatureBase = [
  'merchant',
  '990.00',
  '123456',
  encodeURIComponent(successUrl),
  'GET',
  encodeURIComponent(failUrl),
  'GET',
  'pass-1',
  shp
].join(':');
assert.equal(
  url.searchParams.get('SignatureValue'),
  payment.createRobokassaSignature(signatureBase, 'sha256')
);
```

- [ ] **Step 2: Run the focused test file and verify it fails**

Run:

```powershell
node --test tests/payment-integrations.test.js
```

Expected: FAIL because Robokassa is rejected by `createPaymentButtonPayment` and invoice/link helpers do not exist.

- [ ] **Step 3: Implement Robokassa URL helpers and payment creation**

In `src/modules/payment-integrations.js`, add constants and helpers:

```js
const ROBOKASSA_PAYMENT_URL = 'https://auth.robokassa.ru/Merchant/Index.aspx';

function makeRobokassaInvoiceId(invoiceIdFactory = () => Date.now() * 1000 + Math.floor(Math.random() * 1000)) {
  const value = String(invoiceIdFactory()).trim();
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error('Robokassa: failed to generate numeric InvId');
  }
  return value;
}

function sortRobokassaShpParams(values = {}) {
  return Object.keys(values)
    .filter(key => key.startsWith('Shp_'))
    .sort()
    .map(key => `${key}=${String(values[key] || '')}`);
}

function buildRobokassaReturnUrl(profileId, integration, context, paymentId, outcome) {
  const base = normalizeText(context.appUrl || context.baseUrl || process.env.APP_URL || '');
  if (!base) return '';
  const url = new URL(base);
  url.search = '';
  url.hash = '';
  url.searchParams.set('robokassaReturn', outcome);
  url.searchParams.set('profileId', normalizeText(profileId || '1') || '1');
  url.searchParams.set('integrationId', normalizeText(integration.id));
  url.searchParams.set('paymentId', normalizeText(paymentId));
  return url.toString();
}
```

Add a `createRobokassaPaymentButtonPayment` function. Its signature base must follow Robokassa modifier order:

```js
async function createRobokassaPaymentButtonPayment(profileId, payload, context, integration, overrides = {}) {
  const amountRub = normalizePaymentButtonAmount(payload.amountRub || payload.amount || payload.value);
  const outSum = amountRub.toFixed(2);
  const description = normalizeText(payload.description) || `PAPA BOT payment ${outSum} RUB`;
  const paymentId = makeRobokassaInvoiceId(overrides.invoiceIdFactory);
  const successUrl = buildRobokassaReturnUrl(profileId, integration, context, paymentId, 'success');
  const failUrl = buildRobokassaReturnUrl(profileId, integration, context, paymentId, 'failure');
  if (!successUrl || !failUrl) throw new Error('Robokassa: APP_URL is required for payment returns');

  const shpParams = {
    Shp_profileId: normalizeText(profileId || '1') || '1',
    Shp_integrationId: normalizeText(integration.id),
    Shp_paymentId: paymentId
  };
  const signatureBase = [
    integration.merchantLogin,
    outSum,
    paymentId,
    encodeURIComponent(successUrl),
    'GET',
    encodeURIComponent(failUrl),
    'GET',
    integration.password1,
    ...sortRobokassaShpParams(shpParams)
  ].join(':');
  const signatureValue = createRobokassaSignature(signatureBase, integration.robokassaHashAlgorithm);

  const url = new URL(ROBOKASSA_PAYMENT_URL);
  Object.entries({
    MerchantLogin: integration.merchantLogin,
    OutSum: outSum,
    InvId: paymentId,
    Description: description.slice(0, 100),
    Culture: 'ru',
    SuccessUrl2: successUrl,
    SuccessUrl2Method: 'GET',
    FailUrl2: failUrl,
    FailUrl2Method: 'GET',
    SignatureValue: signatureValue,
    ...shpParams
  }).forEach(([key, value]) => url.searchParams.set(key, value));

  await savePaymentButtonPaymentRecord(profileId, {
    paymentId,
    provider: 'robokassa',
    providerLabel: integration.providerLabel,
    status: 'pending',
    integrationId: normalizeText(integration.id),
    integrationName: normalizeText(integration.name),
    communityId: normalizeText(context.communityId),
    userId: normalizeText(context.userId),
    amountRub: String(amountRub),
    description,
    confirmationUrl: url.toString(),
    successBot: normalizeText(payload.successBot),
    successStep: normalizeText(payload.successStep),
    failBot: normalizeText(payload.failBot),
    failStep: normalizeText(payload.failStep)
  }, overrides);

  return {
    success: true,
    provider: 'robokassa',
    providerLabel: integration.providerLabel,
    integrationId: integration.id,
    integrationName: integration.name,
    paymentId,
    confirmationUrl: url.toString()
  };
}
```

Dispatch Robokassa before the YooKassa-only rejection:

```js
if (integration.provider === 'robokassa') {
  return createRobokassaPaymentButtonPayment(profileId, payload, context, integration, overrides);
}
```

Export the new helpers used by tests and the handler module:

```js
makeRobokassaInvoiceId,
sortRobokassaShpParams,
buildRobokassaReturnUrl,
```

- [ ] **Step 4: Run the focused test file and verify it passes**

Run:

```powershell
node --test tests/payment-integrations.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit Robokassa payment-link creation**

```powershell
git add -- src/modules/payment-integrations.js tests/payment-integrations.test.js
git commit -m "Create Robokassa payment button links"
```

---

### Task 3: Robokassa ResultURL Success Processing

**Files:**
- Create: `src/modules/robokassa-payments.js`
- Create: `tests/robokassa-payments.test.js`

- [ ] **Step 1: Write failing tests for valid success, exact response data, and rejection**

Create `tests/robokassa-payments.test.js` with `node:test` and shared fixtures:

```js
const test = require('node:test');
const assert = require('node:assert/strict');

const robokassa = require('../src/modules/robokassa-payments');
const { createRobokassaSignature } = require('../src/modules/payment-integrations');

function buildData(status = 'pending') {
  return {
    profiles: {
      7: {
        profileId: '7',
        paymentIntegrations: [{
          id: 'robo_main',
          name: 'Robokassa Main',
          provider: 'robokassa',
          merchantLogin: 'merchant',
          password1: 'pass-1',
          password2: 'pass-2',
          robokassaHashAlgorithm: 'sha256'
        }],
        paymentButtonPayments: [{
          paymentId: '123456',
          provider: 'robokassa',
          status,
          integrationId: 'robo_main',
          communityId: '229445618',
          userId: '42',
          amountRub: '990',
          successBot: 'Sales',
          successStep: 'Paid',
          failBot: 'Sales',
          failStep: 'Payment failed'
        }]
      }
    }
  };
}

function buildResultParams(overrides = {}) {
  const params = {
    OutSum: '990.000000',
    InvId: '123456',
    Shp_integrationId: 'robo_main',
    Shp_paymentId: '123456',
    Shp_profileId: '7',
    ...overrides
  };
  const signatureBase = [
    params.OutSum,
    params.InvId,
    'pass-2',
    'Shp_integrationId=' + params.Shp_integrationId,
    'Shp_paymentId=' + params.Shp_paymentId,
    'Shp_profileId=' + params.Shp_profileId
  ].join(':');
  params.SignatureValue = createRobokassaSignature(signatureBase, 'sha256');
  return params;
}
```

Add:

```js
test('handleRobokassaResult verifies ResultURL and routes successful payment', async () => {
  const calls = [];
  const saved = [];
  const result = await robokassa.handleRobokassaResult(
    { queryStringParameters: buildResultParams() },
    {
      loadDashboardData: async () => buildData(),
      updateUserBotAndStep: async (...args) => calls.push(['route', ...args]),
      loadMessageRows: async () => ([{
        'Бот': 'Sales',
        'Шаг': 'Paid',
        'Ответ': 'Payment accepted'
      }]),
      sendMessageAndPerformActions: async () => {
        calls.push(['send']);
        return true;
      },
      saveDashboardData: async data => saved.push(data)
    }
  );

  assert.equal(result.success, true);
  assert.equal(result.responseText, 'OK123456');
  assert.deepEqual(calls[0], ['route', '42', 'Sales', 'Paid', '229445618', '7']);
  assert.deepEqual(calls[1], ['send']);
  assert.equal(saved[0].profiles['7'].paymentButtonPayments[0].status, 'succeeded');
});

test('handleRobokassaResult rejects invalid signature and amount mismatch', async () => {
  await assert.rejects(
    () => robokassa.handleRobokassaResult(
      { queryStringParameters: { ...buildResultParams(), SignatureValue: 'bad' } },
      { loadDashboardData: async () => buildData() }
    ),
    /signature/
  );

  await assert.rejects(
    () => robokassa.handleRobokassaResult(
      { queryStringParameters: buildResultParams({ OutSum: '991.000000' }) },
      { loadDashboardData: async () => buildData() }
    ),
    /amount/
  );
});
```

- [ ] **Step 2: Run the new test file and verify it fails**

Run:

```powershell
node --test tests/robokassa-payments.test.js
```

Expected: FAIL because `src/modules/robokassa-payments.js` does not exist.

- [ ] **Step 3: Implement parsing, lookup, verification, and success routing**

Create `src/modules/robokassa-payments.js`.

Import existing helpers:

```js
const crypto = require('crypto');
const {
  createRobokassaSignature,
  normalizePaymentIntegration,
  savePaymentButtonPaymentRecord,
  sendPaymentRouteStepAnswer,
  sortRobokassaShpParams,
  validatePaymentIntegration
} = require('./payment-integrations');
const { updateUserBotAndStep } = require('./users');
```

Implement parameter parsing for query strings and `application/x-www-form-urlencoded` POST bodies:

```js
function normalizeText(value) {
  return String(value === null || value === undefined ? '' : value).trim();
}

function parseRobokassaParams(event = {}) {
  const params = {};
  const query = event.queryStringParameters || event.query || event.params || {};
  Object.entries(query).forEach(([key, value]) => {
    params[key] = Array.isArray(value) ? normalizeText(value[0]) : normalizeText(value);
  });
  if (event.body) {
    const raw = event.isBase64Encoded
      ? Buffer.from(String(event.body), 'base64').toString('utf8')
      : String(event.body);
    new URLSearchParams(raw).forEach((value, key) => {
      params[key] = normalizeText(value);
    });
  }
  return params;
}
```

Implement dashboard loading and a strict record lookup:

```js
async function loadProfileDashboardData(overrides = {}) {
  if (typeof overrides.loadDashboardData === 'function') return overrides.loadDashboardData();
  const dashboard = require('./profile-dashboard');
  return dashboard.__testOnly.loadDashboardDataWithDependencies(overrides);
}

function findRobokassaContext(data = {}, params = {}) {
  const profileId = normalizeText(params.Shp_profileId);
  const integrationId = normalizeText(params.Shp_integrationId);
  const paymentId = normalizeText(params.Shp_paymentId || params.InvId);
  const container = data.profiles && data.profiles[profileId];
  if (!container) throw new Error('Robokassa profile not found');
  const record = (container.paymentButtonPayments || []).find(item =>
    normalizeText(item.provider) === 'robokassa' &&
    normalizeText(item.paymentId || item.id) === paymentId
  );
  if (!record) throw new Error('Robokassa payment record not found');
  if (normalizeText(record.integrationId) !== integrationId) {
    throw new Error('Robokassa integration mismatch');
  }
  const integration = (container.paymentIntegrations || [])
    .map((item, index) => normalizePaymentIntegration(item, { index }))
    .find(item => normalizeText(item.id) === integrationId);
  if (!integration || integration.provider !== 'robokassa') {
    throw new Error('Robokassa payment integration not found');
  }
  validatePaymentIntegration(integration);
  return { profileId, container, record, integration, paymentId };
}
```

Implement timing-safe signature comparison, decimal amount comparison, and ResultURL verification:

```js
function safeEqualHex(expected, received) {
  const left = Buffer.from(normalizeText(expected).toLowerCase());
  const right = Buffer.from(normalizeText(received).toLowerCase());
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function amountToCents(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return null;
  return Math.round(amount * 100);
}

function verifyRobokassaResult(params, context) {
  if (normalizeText(params.InvId) !== context.paymentId) {
    throw new Error('Robokassa InvId mismatch');
  }
  if (amountToCents(params.OutSum) !== amountToCents(context.record.amountRub)) {
    throw new Error('Robokassa amount mismatch');
  }
  const signatureBase = [
    normalizeText(params.OutSum),
    normalizeText(params.InvId),
    context.integration.password2,
    ...sortRobokassaShpParams(params)
  ].join(':');
  const expected = createRobokassaSignature(signatureBase, context.integration.robokassaHashAlgorithm);
  if (!safeEqualHex(expected, params.SignatureValue)) {
    throw new Error('Robokassa signature verification failed');
  }
}
```

Implement `handleRobokassaResult` so it routes only once, saves `succeeded`, and always returns `OK{InvId}` for repeated valid notifications:

```js
async function handleRobokassaResult(event = {}, overrides = {}) {
  const params = parseRobokassaParams(event);
  const data = await loadProfileDashboardData(overrides);
  const context = findRobokassaContext(data, params);
  verifyRobokassaResult(params, context);

  if (normalizeText(context.record.status) === 'succeeded' && context.record.routedAt) {
    return { success: true, duplicate: true, responseText: `OK${context.paymentId}` };
  }

  const updateUserBotAndStepImpl = overrides.updateUserBotAndStep || updateUserBotAndStep;
  let answerSent = false;
  if (context.record.successBot && context.record.successStep && context.record.userId && context.record.communityId) {
    await updateUserBotAndStepImpl(
      context.record.userId,
      context.record.successBot,
      context.record.successStep,
      context.record.communityId,
      context.profileId
    );
    answerSent = await sendPaymentRouteStepAnswer(
      context.record.userId,
      context.record.successBot,
      context.record.successStep,
      context.record.communityId,
      context.profileId,
      overrides
    );
  }

  await savePaymentButtonPaymentRecord(context.profileId, {
    ...context.record,
    paymentId: context.paymentId,
    provider: 'robokassa',
    status: 'succeeded',
    providerPaymentStatus: 'succeeded',
    routeOutcome: 'success',
    routeBot: normalizeText(context.record.successBot),
    routeStep: normalizeText(context.record.successStep),
    routeAnswerSent: answerSent,
    routedAt: new Date().toISOString(),
    resolvedAt: new Date().toISOString()
  }, overrides);

  return { success: true, responseText: `OK${context.paymentId}` };
}
```

Export parsing and verification helpers needed by later tests:

```js
module.exports = {
  handleRobokassaResult,
  parseRobokassaParams,
  verifyRobokassaResult
};
```

- [ ] **Step 4: Run the new test file and verify it passes**

Run:

```powershell
node --test tests/robokassa-payments.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit ResultURL handling**

```powershell
git add -- src/modules/robokassa-payments.js tests/robokassa-payments.test.js
git commit -m "Handle Robokassa payment results"
```

---

### Task 4: Robokassa Browser Returns And State Priority

**Files:**
- Modify: `src/modules/robokassa-payments.js`
- Modify: `tests/robokassa-payments.test.js`

- [ ] **Step 1: Write failing tests for success return, fail routing, idempotency, and late success**

Add to `tests/robokassa-payments.test.js`:

```js
function buildReturnParams(outcome, overrides = {}) {
  const params = {
    robokassaReturn: outcome,
    profileId: '7',
    integrationId: 'robo_main',
    paymentId: '123456',
    OutSum: '990.00',
    InvId: '123456',
    Shp_integrationId: 'robo_main',
    Shp_paymentId: '123456',
    Shp_profileId: '7',
    ...overrides
  };
  const signatureBase = [
    params.OutSum,
    params.InvId,
    'pass-1',
    'Shp_integrationId=' + params.Shp_integrationId,
    'Shp_paymentId=' + params.Shp_paymentId,
    'Shp_profileId=' + params.Shp_profileId
  ].join(':');
  params.SignatureValue = createRobokassaSignature(signatureBase, 'sha256');
  return params;
}

test('handleRobokassaReturn redirects success return without confirming payment', async () => {
  const saved = [];
  const result = await robokassa.handleRobokassaReturn(
    { queryStringParameters: buildReturnParams('success') },
    {
      loadDashboardData: async () => buildData(),
      saveDashboardData: async data => saved.push(data)
    }
  );

  assert.equal(result.redirectUrl, 'https://vk.com/im?sel=-229445618');
  assert.equal(result.confirmed, false);
  assert.equal(saved.length, 0);
});

test('handleRobokassaReturn routes valid fail return once', async () => {
  const calls = [];
  const saved = [];
  const result = await robokassa.handleRobokassaReturn(
    { queryStringParameters: buildReturnParams('failure') },
    {
      loadDashboardData: async () => buildData(),
      updateUserBotAndStep: async (...args) => calls.push(['route', ...args]),
      loadMessageRows: async () => ([{
        'Бот': 'Sales',
        'Шаг': 'Payment failed',
        'Ответ': 'Try again'
      }]),
      sendMessageAndPerformActions: async () => {
        calls.push(['send']);
        return true;
      },
      saveDashboardData: async data => saved.push(data)
    }
  );

  assert.equal(result.redirectUrl, 'https://vk.com/im?sel=-229445618');
  assert.deepEqual(calls[0], ['route', '42', 'Sales', 'Payment failed', '229445618', '7']);
  assert.equal(saved[0].profiles['7'].paymentButtonPayments[0].status, 'canceled');
});

test('successful ResultURL upgrades an earlier canceled payment', async () => {
  const calls = [];
  const saved = [];
  await robokassa.handleRobokassaResult(
    { queryStringParameters: buildResultParams() },
    {
      loadDashboardData: async () => buildData('canceled'),
      updateUserBotAndStep: async (...args) => calls.push(['route', ...args]),
      loadMessageRows: async () => ([{
        'Бот': 'Sales',
        'Шаг': 'Paid',
        'Ответ': 'Payment accepted'
      }]),
      sendMessageAndPerformActions: async () => true,
      saveDashboardData: async data => saved.push(data)
    }
  );

  assert.equal(saved[0].profiles['7'].paymentButtonPayments[0].status, 'succeeded');
  assert.deepEqual(calls[0], ['route', '42', 'Sales', 'Paid', '229445618', '7']);
});

test('handleRobokassaReturn does not reroute repeated failure or downgrade success', async () => {
  const calls = [];
  const canceled = buildData('canceled');
  canceled.profiles['7'].paymentButtonPayments[0].routedAt = '2026-06-03T10:00:00.000Z';
  await robokassa.handleRobokassaReturn(
    { queryStringParameters: buildReturnParams('failure') },
    {
      loadDashboardData: async () => canceled,
      updateUserBotAndStep: async (...args) => calls.push(args),
      saveDashboardData: async () => {}
    }
  );
  assert.equal(calls.length, 0);

  const succeeded = buildData('succeeded');
  succeeded.profiles['7'].paymentButtonPayments[0].routedAt = '2026-06-03T10:00:00.000Z';
  await robokassa.handleRobokassaReturn(
    { queryStringParameters: buildReturnParams('failure') },
    {
      loadDashboardData: async () => succeeded,
      updateUserBotAndStep: async (...args) => calls.push(args),
      saveDashboardData: async () => {}
    }
  );
  assert.equal(calls.length, 0);
});

test('handleRobokassaReturn rejects unsigned context mismatch', async () => {
  await assert.rejects(
    () => robokassa.handleRobokassaReturn(
      {
        queryStringParameters: {
          ...buildReturnParams('failure'),
          integrationId: 'other_integration'
        }
      },
      { loadDashboardData: async () => buildData() }
    ),
    /context mismatch/
  );
});
```

- [ ] **Step 2: Run the new test file and verify it fails**

Run:

```powershell
node --test tests/robokassa-payments.test.js
```

Expected: FAIL because `handleRobokassaReturn` does not exist.

- [ ] **Step 3: Implement return verification and routing**

In `src/modules/robokassa-payments.js`, add:

```js
function buildVkCommunityDialogUrl(communityId = '') {
  const raw = normalizeText(communityId);
  if (!/^-?\d+$/.test(raw)) return '';
  return `https://vk.com/im?sel=-${Math.abs(Number(raw))}`;
}

function verifyRobokassaReturn(params, context) {
  if (
    normalizeText(params.profileId) !== context.profileId ||
    normalizeText(params.integrationId) !== normalizeText(context.integration.id) ||
    normalizeText(params.paymentId) !== context.paymentId
  ) {
    throw new Error('Robokassa return context mismatch');
  }
  if (normalizeText(params.InvId) !== context.paymentId) {
    throw new Error('Robokassa InvId mismatch');
  }
  if (amountToCents(params.OutSum) !== amountToCents(context.record.amountRub)) {
    throw new Error('Robokassa amount mismatch');
  }
  const signatureBase = [
    normalizeText(params.OutSum),
    normalizeText(params.InvId),
    context.integration.password1,
    ...sortRobokassaShpParams(params)
  ].join(':');
  const expected = createRobokassaSignature(signatureBase, context.integration.robokassaHashAlgorithm);
  if (!safeEqualHex(expected, params.SignatureValue)) {
    throw new Error('Robokassa return signature verification failed');
  }
}
```

Implement `handleRobokassaReturn`:

```js
async function handleRobokassaReturn(event = {}, overrides = {}) {
  const params = parseRobokassaParams(event);
  const data = await loadProfileDashboardData(overrides);
  const context = findRobokassaContext(data, params);
  verifyRobokassaReturn(params, context);
  const redirectUrl = buildVkCommunityDialogUrl(context.record.communityId);
  const outcome = normalizeText(params.robokassaReturn).toLowerCase();

  if (outcome === 'success') {
    return { success: true, confirmed: false, redirectUrl };
  }
  if (outcome !== 'failure') {
    throw new Error('Robokassa return outcome is invalid');
  }
  if (normalizeText(context.record.status) === 'succeeded') {
    return { success: true, duplicate: true, redirectUrl };
  }
  if (normalizeText(context.record.status) === 'canceled' && context.record.routedAt) {
    return { success: true, duplicate: true, redirectUrl };
  }

  const updateUserBotAndStepImpl = overrides.updateUserBotAndStep || updateUserBotAndStep;
  let answerSent = false;
  if (context.record.failBot && context.record.failStep && context.record.userId && context.record.communityId) {
    await updateUserBotAndStepImpl(
      context.record.userId,
      context.record.failBot,
      context.record.failStep,
      context.record.communityId,
      context.profileId
    );
    answerSent = await sendPaymentRouteStepAnswer(
      context.record.userId,
      context.record.failBot,
      context.record.failStep,
      context.record.communityId,
      context.profileId,
      overrides
    );
  }

  await savePaymentButtonPaymentRecord(context.profileId, {
    ...context.record,
    paymentId: context.paymentId,
    provider: 'robokassa',
    status: 'canceled',
    providerPaymentStatus: 'canceled',
    routeOutcome: 'failure',
    routeBot: normalizeText(context.record.failBot),
    routeStep: normalizeText(context.record.failStep),
    routeAnswerSent: answerSent,
    routedAt: new Date().toISOString(),
    resolvedAt: new Date().toISOString()
  }, overrides);

  return { success: true, redirectUrl };
}
```

Export:

```js
handleRobokassaReturn,
verifyRobokassaReturn,
```

- [ ] **Step 4: Run the new test file and verify it passes**

Run:

```powershell
node --test tests/robokassa-payments.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit browser-return handling**

```powershell
git add -- src/modules/robokassa-payments.js tests/robokassa-payments.test.js
git commit -m "Handle Robokassa payment returns"
```

---

### Task 5: Public Handler Routes

**Files:**
- Modify: `src/handler.js`
- Modify: `tests/admin-auth-flow.test.js`

- [ ] **Step 1: Write failing handler tests for public ResultURL and return routes**

Extend the base mocks in `tests/admin-auth-flow.test.js`:

```js
'src/modules/robokassa-payments.js': {
  handleRobokassaResult: async () => ({ success: true, responseText: 'OK123456' }),
  handleRobokassaReturn: async () => ({ success: true, redirectUrl: 'https://vk.com/im?sel=-229445618' })
},
```

Add tests:

```js
test('robokassa ResultURL does not require admin session and returns plain OK response', async () => {
  const calls = [];
  const handler = loadHandlerWithMocks({
    'src/modules/robokassa-payments.js': {
      handleRobokassaResult: async event => {
        calls.push(event);
        return { success: true, responseText: 'OK123456' };
      },
      handleRobokassaReturn: async () => ({ success: true })
    }
  });

  const response = await handler({
    httpMethod: 'POST',
    queryStringParameters: { robokassaResult: '1' },
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: 'OutSum=990.000000&InvId=123456&SignatureValue=valid'
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.headers['Content-Type'], 'text/plain; charset=utf-8');
  assert.equal(response.body, 'OK123456');
  assert.equal(calls.length, 1);
});

test('robokassa browser return redirects user without admin session', async () => {
  const handler = loadHandlerWithMocks({
    'src/modules/robokassa-payments.js': {
      handleRobokassaResult: async () => ({ success: true, responseText: 'OK123456' }),
      handleRobokassaReturn: async () => ({
        success: true,
        redirectUrl: 'https://vk.com/im?sel=-229445618'
      })
    }
  });

  const response = await handler({
    httpMethod: 'GET',
    queryStringParameters: { robokassaReturn: 'failure', InvId: '123456' },
    headers: {}
  });

  assert.equal(response.statusCode, 302);
  assert.equal(response.headers.Location, 'https://vk.com/im?sel=-229445618');
});

test('robokassa GET ResultURL delegates verification and returns plain OK response', async () => {
  const calls = [];
  const handler = loadHandlerWithMocks({
    'src/modules/robokassa-payments.js': {
      handleRobokassaResult: async event => {
        calls.push(event.queryStringParameters);
        return { success: true, responseText: 'OK123456' };
      },
      handleRobokassaReturn: async () => ({ success: true })
    }
  });

  const response = await handler({
    httpMethod: 'GET',
    queryStringParameters: {
      robokassaResult: '1',
      OutSum: '990.000000',
      InvId: '123456',
      SignatureValue: 'valid'
    },
    headers: {}
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.body, 'OK123456');
  assert.equal(calls.length, 1);
});
```

- [ ] **Step 2: Run the handler test file and verify it fails**

Run:

```powershell
node --test tests/admin-auth-flow.test.js
```

Expected: FAIL because `src/handler.js` does not import or route Robokassa handlers.

- [ ] **Step 3: Wire Robokassa routes and response shapes**

In `src/handler.js`, import:

```js
const { handleRobokassaResult, handleRobokassaReturn } = require('./modules/robokassa-payments');
```

Route GET requests before admin routes:

```js
if (q.robokassaResult !== undefined) {
  return handleRobokassaResultRequest(event);
}

if (q.robokassaReturn !== undefined) {
  return handleRobokassaReturnRequest(event);
}
```

Route POST ResultURL requests before admin-session-protected actions:

```js
if (q.robokassaResult !== undefined) {
  return handleRobokassaResultRequest(event);
}
```

Add response helpers:

```js
async function handleRobokassaResultRequest(event) {
  try {
    const result = await handleRobokassaResult(event);
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Access-Control-Allow-Origin': '*'
      },
      body: String(result.responseText || '')
    };
  } catch (e) {
    log('error', 'Robokassa ResultURL handling failed:', e);
    return {
      statusCode: 400,
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Access-Control-Allow-Origin': '*'
      },
      body: 'invalid'
    };
  }
}

async function handleRobokassaReturnRequest(event) {
  try {
    const result = await handleRobokassaReturn(event);
    return {
      statusCode: 302,
      headers: {
        Location: String(result.redirectUrl || process.env.APP_URL || 'https://vk.com/im'),
        'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
        Pragma: 'no-cache',
        'Access-Control-Allow-Origin': '*'
      },
      body: ''
    };
  } catch (e) {
    log('error', 'Robokassa return handling failed:', e);
    return {
      statusCode: 302,
      headers: {
        Location: process.env.APP_URL || 'https://vk.com/im',
        'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
        Pragma: 'no-cache',
        'Access-Control-Allow-Origin': '*'
      },
      body: ''
    };
  }
}
```

- [ ] **Step 4: Run the handler test file and verify it passes**

Run:

```powershell
node --test tests/admin-auth-flow.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit public routes**

```powershell
git add -- src/handler.js tests/admin-auth-flow.test.js
git commit -m "Expose Robokassa payment callbacks"
```

---

### Task 6: Admin Profile Robokassa UI And TEST Action

**Files:**
- Modify: `adminPanelHTML.js`
- Modify: `src/modules/payment-integrations.js`
- Modify: `tests/admin-panel-consents-contract.test.js`
- Modify: `tests/payment-integrations.test.js`

- [ ] **Step 1: Write failing UI and TEST contract tests**

Add to `tests/admin-panel-consents-contract.test.js`:

```js
test('Profile payment integrations allow Robokassa with hash algorithm and Result URL', () => {
  assert.match(adminPanelHTML, /PROFILE_PAYMENT_PROVIDER_SELECT_IDS = \['yookassa', 'prodamus', 'robokassa'\]/);
  assert.match(adminPanelHTML, /robokassaHashAlgorithm/);
  assert.match(adminPanelHTML, /MD5/);
  assert.match(adminPanelHTML, /SHA-256/);
  assert.match(adminPanelHTML, /SHA-512/);
  assert.match(adminPanelHTML, /robokassaResult=1/);
  assert.match(adminPanelHTML, /только боевой режим/i);
  assert.match(adminPanelHTML, /Receipt/);
  assert.match(adminPanelHTML, /фискализац/i);
  assert.match(adminPanelHTML, /fail/i);
});
```

Add to `tests/payment-integrations.test.js`:

```js
test('testPaymentIntegration reports local Robokassa structural check without creating payment', async () => {
  const result = await payment.testPaymentIntegration({
    name: 'Robot',
    provider: 'robokassa',
    merchantLogin: 'merchant',
    password1: 'pass-1',
    password2: 'pass-2',
    robokassaHashAlgorithm: 'sha256'
  });

  assert.equal(result.success, true);
  assert.equal(result.provider, 'robokassa');
  assert.match(result.message, /локальн/i);
  assert.ok(result.details.some(line => /реальный платеж не создавался/i.test(line)));
  assert.ok(result.details.some(line => /SHA-256/i.test(line)));
});
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```powershell
node --test tests/admin-panel-consents-contract.test.js tests/payment-integrations.test.js
```

Expected: FAIL because Robokassa is hidden, the algorithm field is absent, and TEST uses the generic message.

- [ ] **Step 3: Render Robokassa fields, warnings, and auto-generated Result URL**

In `adminPanelHTML.js`:

1. Add an algorithm select field to the Robokassa preset:

```js
{ key: 'robokassaHashAlgorithm', label: 'Алгоритм подписи', required: true, type: 'select', options: [
  { value: 'md5', label: 'MD5' },
  { value: 'sha256', label: 'SHA-256' },
  { value: 'sha512', label: 'SHA-512' }
], help: 'Зачем: алгоритм должен точно совпадать с техническими настройками магазина Robokassa.' }
```

2. Make Robokassa selectable:

```js
const PROFILE_PAYMENT_PROVIDER_SELECT_IDS = ['yookassa', 'prodamus', 'robokassa'];
```

3. Update `renderPaymentIntegrationDynamicFields` to render `<select>` when `field.type === 'select'`:

```js
var controlHtml = field.type === 'select'
  ? '<select id="paymentIntegration' + escapeHtml(field.key.charAt(0).toUpperCase() + field.key.slice(1)) + '" data-payment-field="' + escapeHtml(field.key) + '" data-required="' + (field.required ? 'true' : 'false') + '">' +
      (field.options || []).map(function(option) {
        var selected = String(getPaymentIntegrationFieldValue(field.key, values, providerId)) === String(option.value) ? ' selected' : '';
        return '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
      }).join('') +
    '</select>'
  : '<input id="paymentIntegration' + escapeHtml(field.key.charAt(0).toUpperCase() + field.key.slice(1)) + '" type="' + escapeHtml(field.type || 'text') + '" placeholder="' + escapeHtml(field.placeholder || '') + '" value="' + escapeHtml(getPaymentIntegrationFieldValue(field.key, values, providerId)) + '" data-payment-field="' + escapeHtml(field.key) + '" data-required="' + (field.required ? 'true' : 'false') + '">';
```

4. Generalize `buildProfilePaymentIntegrationWebhookUrl`:

```js
if (!providerPreset || (providerPreset.id !== 'prodamus' && providerPreset.id !== 'robokassa')) return '';
var marker = providerPreset.id === 'robokassa' ? 'robokassaResult' : 'prodamusWebhook';
url.searchParams.set(marker, '1');
if (providerPreset.id === 'prodamus') {
  url.searchParams.set('profileId', getCurrentProfileId() || getPrincipalProfileId() || '1');
  url.searchParams.set('integrationId', integrationId);
}
```

5. Auto-fill and auto-refresh notification URLs for both providers:

```js
if (fieldKey === 'notificationUrl' && !current && (providerId === 'prodamus' || providerId === 'robokassa')) {
  return buildProfilePaymentIntegrationWebhookUrl(values);
}

if (!providerPreset || (providerPreset.id !== 'prodamus' && providerPreset.id !== 'robokassa') || !notificationEl) return;

var notificationAutoGenerated =
  (providerId === 'prodamus' || providerId === 'robokassa') &&
  !String(values.notificationUrl || '').trim();
```

For Robokassa, label the common notification field `Result URL`, make the auto-generated field read-only, and explain that this exact URL must be copied into Robokassa technical settings.

6. Add `renderPaymentIntegrationProviderNotice(providerId)` and include it below the dynamic fields. For Robokassa, render text that explicitly states:

```text
Robokassa: только боевой режим. PAPA BOT не передает Receipt; настройте фискализацию отдельно. Успех подтверждается Result URL, а fail-шаг зависит от возврата пользователя через браузер.
```

7. Add `robokassaHashAlgorithm` to:

- `readProfilePaymentIntegrationForm`
- `hasProfilePaymentIntegrationDraft`
- `setProfilePaymentIntegrationForm`
- saved-card credential summary

Use the UI field ID:

```js
paymentIntegrationRobokassaHashAlgorithm
```

- [ ] **Step 4: Make the backend TEST result provider-specific**

In `src/modules/payment-integrations.js`, after validation in `testPaymentIntegration`, special-case Robokassa:

```js
if (normalized.provider === 'robokassa') {
  const sampleBase = `${normalized.merchantLogin}:10.00:1:${normalized.password1}`;
  const sampleSignature = createRobokassaSignature(sampleBase, normalized.robokassaHashAlgorithm);
  return {
    success: true,
    provider: normalized.provider,
    providerLabel: normalized.providerLabel,
    message: 'Robokassa: локальная проверка реквизитов и подписи успешна.',
    details: [
      `Алгоритм подписи: ${normalized.robokassaHashAlgorithm === 'sha256' ? 'SHA-256' : normalized.robokassaHashAlgorithm === 'sha512' ? 'SHA-512' : 'MD5'}`,
      `Контрольная подпись сформирована: ${sampleSignature.slice(0, 12)}...`,
      'Сетевой запрос не выполнялся, реальный платеж не создавался.'
    ]
  };
}
```

- [ ] **Step 5: Run the focused tests and verify they pass**

Run:

```powershell
node --test tests/admin-panel-consents-contract.test.js tests/payment-integrations.test.js
```

Expected: PASS.

- [ ] **Step 6: Commit the admin UI**

```powershell
git add -- adminPanelHTML.js src/modules/payment-integrations.js tests/admin-panel-consents-contract.test.js tests/payment-integrations.test.js
git commit -m "Add Robokassa profile integration UI"
```

---

### Task 7: Documentation, Full Verification, And Deployment

**Files:**
- Modify: `FUNCTIONALITY.md`
- Verify: `src/modules/payment-integrations.js`
- Verify: `src/modules/robokassa-payments.js`
- Verify: `src/handler.js`
- Verify: `adminPanelHTML.js`
- Verify: relevant tests

- [ ] **Step 1: Update the functionality registry**

Update the payment integration section of `FUNCTIONALITY.md` so it no longer says profile selection offers only YooKassa and Prodamus.

Document:

```markdown
- Profile payment integration selection offers YooKassa, Prodamus, and Robokassa.
- Robokassa is supported for bot payment buttons only, not for PAPA BOT profile balance top-ups.
- Robokassa payment buttons create production signed payment links with a numeric `InvId`, configurable `MD5` / `SHA-256` / `SHA-512` signatures, and signed `Shp_*` routing parameters.
- Robokassa success is confirmed only by the public `robokassaResult` ResultURL handler using Password 2; repeated valid notifications return `OK{InvId}` without duplicate bot replies.
- Robokassa success/fail browser returns redirect users to the VK dialog; a valid fail return may run the fail route, while a later valid ResultURL success takes priority.
- The first Robokassa version does not use test mode, `Receipt`, profile balance top-ups, holding, saved cards, or refunds.
```

- [ ] **Step 2: Run focused Robokassa tests**

Run:

```powershell
node --test tests/payment-integrations.test.js tests/robokassa-payments.test.js tests/admin-auth-flow.test.js tests/admin-panel-consents-contract.test.js
```

Expected: PASS.

- [ ] **Step 3: Run adjacent payment-button regression tests**

Run:

```powershell
node --test tests/payment-keyboards.test.js tests/keyboard-payment.test.js tests/prodamus-payments.test.js tests/yookassa-balance.test.js
```

Expected: PASS.

- [ ] **Step 4: Check formatting and changed-file diff**

Run:

```powershell
git diff --check
git status --short
```

Expected: `git diff --check` produces no output. Review `git status --short` without reverting unrelated user changes.

- [ ] **Step 5: Commit documentation**

```powershell
git add -- FUNCTIONALITY.md
git commit -m "Document Robokassa payment buttons"
```

- [ ] **Step 6: Deploy the verified working version**

Run:

```powershell
node scripts/deploy.js
```

Expected: deployment completes successfully. The deploy script packages the root `adminPanelHTML.js`, `src`, and related runtime files for the Yandex Function deployment.

- [ ] **Step 7: Record deployment outcome**

If deployment succeeds, include it in the final response. If deployment is blocked by credentials, network access, or deployment tooling, state the exact failure and do not claim the feature is deployed.

---

## Final Verification

Before reporting completion, run:

```powershell
node --test tests/payment-integrations.test.js tests/robokassa-payments.test.js tests/admin-auth-flow.test.js tests/admin-panel-consents-contract.test.js tests/payment-keyboards.test.js tests/keyboard-payment.test.js tests/prodamus-payments.test.js tests/yookassa-balance.test.js
git diff --check
git status --short
```

Confirm:

- Robokassa is available only for payment buttons, not profile balance top-ups.
- Payment links omit `IsTest` and `Receipt`.
- Numeric `InvId`, `Shp_*`, `SuccessUrl2`, and `FailUrl2` are signed correctly.
- Only ResultURL confirms success and returns exact `OK{InvId}`.
- Fail returns route once, success cannot be downgraded, and late success overrides fail.
- Admin UI explains production-only mode, fiscalization responsibility, and fail-return limitations.
- `FUNCTIONALITY.md` matches the implementation.
- The verified version has been deployed or the exact deployment blocker is reported.
