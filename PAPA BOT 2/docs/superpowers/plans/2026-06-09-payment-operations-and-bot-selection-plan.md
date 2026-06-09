# Payment Operations and Bot Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the selected bot blue with a white circle marker and add a profile-wide, filterable payment operations journal enriched with bot, user, and community information.

**Architecture:** Keep `paymentButtonPayments` as the only payment journal. Pass `sourceBot` through existing payment keyboard contexts, persist it on provider-specific pending records, and have `profile-dashboard.js` sort and enrich stored operations from already loaded community users/config. Render and filter the resulting data entirely inside the existing embedded admin-panel script.

**Tech Stack:** Node.js CommonJS, `node:test`/`assert`, server-rendered HTML with embedded browser JavaScript, JSON profile dashboard storage.

---

## File Map

- Modify `src/modules/messages.js`: pass the current message row bot into payment keyboard resolution.
- Modify `src/modules/scheduler.js`: pass delayed-delivery and mailing source bots into payment keyboard resolution.
- Modify `src/modules/payment-integrations.js`: persist `sourceBot` for YooKassa, Prodamus, and Robokassa pending records.
- Modify `src/modules/profile-dashboard.js`: expose newest-first payment operations enriched with local user/community labels.
- Modify `adminPanelHTML.js`: update selected-bot appearance, add payment operation rendering/filtering, and add the community metric.
- Modify `tests/payment-integrations.test.js`: cover `sourceBot` persistence across providers.
- Modify `tests/messages-runtime.test.js`: cover source bot propagation for immediate message keyboards.
- Modify `tests/scheduler.test.js`: cover source bot propagation for delayed and mailing keyboards.
- Modify `tests/profile-dashboard.test.js`: cover sorting, enrichment, fallbacks, and the 500-record response limit.
- Modify `tests/admin-panel-bot-status-contract.test.js`: cover the blue active button and white circle marker.
- Modify `tests/admin-panel-profile-layout-contract.test.js`: cover block ordering, filters, table fields, and community metric placement/counting.
- Modify `FUNCTIONALITY.md`: document the selected-bot marker and profile payment operations journal.

### Task 1: Persist the source bot on new payment operations

**Files:**
- Modify: `tests/payment-integrations.test.js`
- Modify: `src/modules/payment-integrations.js`

- [ ] **Step 1: Add failing provider persistence assertions**

In the existing YooKassa, Prodamus, and Robokassa creation tests, add `sourceBot: 'Sales source'` to the payment context and assert:

```js
assert.equal(record.sourceBot, 'Sales source');
```

For the YooKassa test, bind the saved record first:

```js
const record = saved[0].profiles['7'].paymentButtonPayments[0];
assert.equal(record.sourceBot, 'Sales source');
```

- [ ] **Step 2: Run the focused test and verify failure**

Run:

```powershell
node --test tests/payment-integrations.test.js
```

Expected: FAIL because provider pending records do not yet contain `sourceBot`.

- [ ] **Step 3: Persist the normalized source bot in all provider records**

Add this field to each pending record passed to `savePaymentButtonPaymentRecord` in the YooKassa, Prodamus, and Robokassa creation paths:

```js
sourceBot: normalizeText(context.sourceBot),
```

Do not derive it from `successBot` or `failBot`.

- [ ] **Step 4: Run the focused test and verify success**

Run:

```powershell
node --test tests/payment-integrations.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit the persistence change**

```powershell
git add -- src/modules/payment-integrations.js tests/payment-integrations.test.js
git commit -m "Store source bot on payment operations"
```

### Task 2: Pass the source bot from every keyboard-producing runtime

**Files:**
- Modify: `tests/messages-runtime.test.js`
- Modify: `tests/scheduler.test.js`
- Modify: `src/modules/messages.js`
- Modify: `src/modules/scheduler.js`

- [ ] **Step 1: Add a failing message runtime assertion**

In the existing message test that stubs `resolvePaymentKeyboard`, capture its context and assert:

```js
assert.equal(paymentKeyboardContext.sourceBot, row['Бот']);
```

Use the actual test row bot value rather than a hard-coded unrelated bot.

- [ ] **Step 2: Add failing scheduler assertions**

In delayed-delivery and mailing tests that stub `resolvePaymentKeyboard`, capture each context and assert:

```js
assert.equal(delayedPaymentContext.sourceBot, delayedRow['Бот']);
assert.equal(mailingPaymentContext.sourceBot, mailingRow['Бот']);
```

- [ ] **Step 3: Run runtime tests and verify failure**

Run:

```powershell
node --test tests/messages-runtime.test.js tests/scheduler.test.js
```

Expected: FAIL because the three contexts omit `sourceBot`.

- [ ] **Step 4: Pass the row bot in each runtime**

Add to the payment keyboard context in `messages.js`:

```js
sourceBot: String(row['Бот'] || bot || '').trim(),
```

Add to delayed delivery in `scheduler.js`:

```js
sourceBot: String(row['Бот'] || '').trim(),
```

Add to mailing delivery using the row that produced the keyboard:

```js
sourceBot: String(effectiveRow['Бот'] || row['Бот'] || '').trim(),
```

- [ ] **Step 5: Run runtime tests and verify success**

Run:

```powershell
node --test tests/messages-runtime.test.js tests/scheduler.test.js
```

Expected: PASS.

- [ ] **Step 6: Commit runtime propagation**

```powershell
git add -- src/modules/messages.js src/modules/scheduler.js tests/messages-runtime.test.js tests/scheduler.test.js
git commit -m "Pass source bot to payment keyboards"
```

### Task 3: Return enriched payment operations from the profile dashboard

**Files:**
- Modify: `tests/profile-dashboard.test.js`
- Modify: `src/modules/profile-dashboard.js`

- [ ] **Step 1: Add a failing overview test**

Create a dashboard overview test with:

```js
paymentButtonPayments: [
  {
    paymentId: 'old',
    communityId: '100',
    userId: '42',
    sourceBot: '',
    createdAt: '2026-06-01T10:00:00.000Z'
  },
  {
    paymentId: 'new',
    communityId: '100',
    userId: '42',
    sourceBot: 'Sales',
    createdAt: '2026-06-02T10:00:00.000Z'
  },
  {
    paymentId: 'fallback',
    communityId: '999',
    userId: '77',
    createdAt: '2026-05-31T10:00:00.000Z'
  }
]
```

Stub configuration and users:

```js
getFullConfig: () => ({
  communities: {
    main: { vk_group_id: '100', group_name: 'Main Community' }
  }
}),
listUsers: async communityId => communityId === '100'
  ? [{ ID: '42', 'ИМЯ': 'Иван Иванов' }]
  : []
```

Assert:

```js
assert.deepEqual(result.paymentOperations.map(item => item.paymentId), ['new', 'old', 'fallback']);
assert.equal(result.paymentOperations[0].userName, 'Иван Иванов');
assert.equal(result.paymentOperations[0].communityName, 'Main Community');
assert.equal(result.paymentOperations[0].sourceBot, 'Sales');
assert.equal(result.paymentOperations[1].sourceBot, '');
assert.equal(result.paymentOperations[2].userName, '');
assert.equal(result.paymentOperations[2].communityName, '');
```

- [ ] **Step 2: Add a failing 500-record limit assertion**

Build 505 records with distinct descending timestamps and assert:

```js
assert.equal(result.paymentOperations.length, 500);
assert.equal(result.paymentOperations[0].paymentId, 'payment-504');
```

- [ ] **Step 3: Run the dashboard test and verify failure**

Run:

```powershell
node --test tests/profile-dashboard.test.js
```

Expected: FAIL because `paymentOperations` is not returned.

- [ ] **Step 4: Cache local user names while loading communities**

Before the community loop, create:

```js
const paymentUserNames = new Map();
const paymentCommunityNames = new Map();
```

When `listUsersImpl` succeeds, keep the returned rows and map names by both internal and VK community IDs:

```js
const userName = String(row['ИМЯ'] || row['Имя'] || row.name || '').trim();
const userId = String(row['ID'] || row.id || '').trim();
if (userId) {
  paymentUserNames.set(`${internalCommunityId}:${userId}`, userName);
  paymentUserNames.set(`${vkGroupId}:${userId}`, userName);
}
```

Map community labels similarly:

```js
const groupName = String(config?.group_name || '').trim();
paymentCommunityNames.set(String(internalCommunityId), groupName);
paymentCommunityNames.set(vkGroupId, groupName);
```

- [ ] **Step 5: Build newest-first enriched operations**

Before the return object, add:

```js
const paymentOperations = (Array.isArray(container.paymentButtonPayments)
  ? container.paymentButtonPayments
  : [])
  .slice()
  .sort((a, b) => String(b.createdAt || b.updatedAt || '').localeCompare(String(a.createdAt || a.updatedAt || '')))
  .slice(0, 500)
  .map(item => {
    const communityId = String(item.communityId || '').trim();
    const userId = String(item.userId || '').trim();
    return {
      ...item,
      sourceBot: String(item.sourceBot || '').trim(),
      userName: paymentUserNames.get(`${communityId}:${userId}`) || '',
      communityName: paymentCommunityNames.get(communityId) || ''
    };
  });
```

Return it as:

```js
paymentOperations,
```

- [ ] **Step 6: Run the dashboard test and verify success**

Run:

```powershell
node --test tests/profile-dashboard.test.js
```

Expected: PASS.

- [ ] **Step 7: Commit dashboard enrichment**

```powershell
git add -- src/modules/profile-dashboard.js tests/profile-dashboard.test.js
git commit -m "Expose profile payment operations"
```

### Task 4: Update the bot switcher and payment operations dashboard UI

**Files:**
- Modify: `tests/admin-panel-bot-status-contract.test.js`
- Modify: `tests/admin-panel-profile-layout-contract.test.js`
- Modify: `adminPanelHTML.js`

- [ ] **Step 1: Add failing selected-bot UI contracts**

Assert the active button uses a blue gradient and dedicated marker:

```js
assert.match(adminPanelHTML, /\.bot-switcher-chip--active \.bot-name-3d-btn[\s\S]*#3b82f6[\s\S]*#1d4ed8/);
assert.match(adminPanelHTML, /\.bot-active-marker/);
assert.match(adminPanelHTML, /marker\.className = 'bot-active-marker'/);
assert.doesNotMatch(adminPanelHTML, /botName \+ \(isActive \? ' ✓' : ''\)/);
```

Keep existing status-toggle check/cross assertions.

- [ ] **Step 2: Add failing profile layout contracts**

Assert all of the following:

```js
assert.match(adminPanelHTML, /var paymentOperations = Array\.isArray\(data\.paymentOperations\)/);
assert.match(adminPanelHTML, /id="profilePaymentOperations"/);
assert.match(adminPanelHTML, /filterProfilePaymentOperations/);
assert.match(adminPanelHTML, /paymentOperationsIntegrationFilter/);
assert.match(adminPanelHTML, /paymentOperationsStatusFilter/);
assert.match(adminPanelHTML, /paymentOperationsBotFilter/);
assert.match(adminPanelHTML, /paymentOperationsUserFilter/);
assert.match(adminPanelHTML, /Дата и время/);
assert.match(adminPanelHTML, /Пользователь/);
assert.match(adminPanelHTML, /Назначение/);
```

Extract the dashboard composition and assert:

```js
assert.ok(paymentOperationsIndex > paymentIntegrationsIndex);
assert.ok(documentsIndex > paymentOperationsIndex);
```

Assert community metric placement:

```js
assert.match(
  adminPanelHTML,
  /renderProfileCommunityMetricLink\('Платежные системы'[\s\S]{0,500}renderProfileCommunityMetricLink\('Платежные операции'/
);
```

- [ ] **Step 3: Run UI contract tests and verify failure**

Run:

```powershell
node --test tests/admin-panel-bot-status-contract.test.js tests/admin-panel-profile-layout-contract.test.js
```

Expected: FAIL because the new marker and journal UI do not exist.

- [ ] **Step 4: Change the active bot appearance**

Replace the active green rule with:

```css
.bot-switcher-chip--active .bot-name-3d-btn {
    background: linear-gradient(180deg, #3b82f6 0%, #1d4ed8 100%);
    color: #fff;
    text-shadow: 0 1px 1px rgba(0,0,0,0.28);
}
.bot-active-marker {
    width: 7px;
    height: 7px;
    flex: 0 0 7px;
    border-radius: 50%;
    background: #fff;
    box-shadow: 0 0 0 1px rgba(255,255,255,0.30);
}
```

Render the bot name and marker separately:

```js
nameBtn.textContent = botName;
if (isActive) {
    const marker = document.createElement('span');
    marker.className = 'bot-active-marker';
    marker.setAttribute('aria-hidden', 'true');
    nameBtn.appendChild(marker);
}
```

Ensure `.bot-name-3d-btn` uses inline-flex alignment so the marker sits beside the name:

```css
display: inline-flex;
align-items: center;
justify-content: center;
gap: 7px;
```

- [ ] **Step 5: Add payment operation helpers and filter state**

Read dashboard data:

```js
var paymentOperations = Array.isArray(data.paymentOperations) ? data.paymentOperations : [];
```

Add helpers for:

- canonical status key;
- Russian status label;
- badge class;
- unique integration and bot filter options;
- table row rendering;
- local filtering by integration, status, bot, and case-insensitive user name/ID.

Expose:

```js
window.filterProfilePaymentOperations = function() {
    // Read the four filter controls, toggle matching rows, and toggle the empty-state row.
};
```

Use escaped `data-*` values on rows so filtering does not parse visible text.

- [ ] **Step 6: Render the payment operations section**

Create `paymentOperationsSectionHtml` immediately after `paymentSectionHtml`. Include:

```html
<div id="profilePaymentOperations" class="settings-surface profile-manager">
```

Render four controls with IDs:

```text
paymentOperationsIntegrationFilter
paymentOperationsStatusFilter
paymentOperationsBotFilter
paymentOperationsUserFilter
```

Render columns in this order:

```text
Дата и время | Статус | Интеграция | Бот | Пользователь | Назначение | Сумма | Сообщество | ID платежа
```

Fallbacks:

```text
sourceBot -> Не указан
userName -> only VK ID
communityName -> communityId
description -> Без описания
```

Add `paymentOperationsSectionHtml` between `paymentSectionHtml` and `documentsSectionHtml` in `container.innerHTML`.

- [ ] **Step 7: Add the community payment metric**

For each card, count matching operations:

```js
var communityPaymentOperationsCount = paymentOperations.filter(function(operation) {
    var operationCommunityId = String(operation.communityId || '').trim();
    return operationCommunityId === communityKey || operationCommunityId === vkGroupId;
}).length;
```

Insert immediately after `Платежные системы`:

```js
renderProfileCommunityMetricLink(
  'Платежные операции',
  communityPaymentOperationsCount,
  'profilePaymentOperations',
  'Перейти к платежным операциям профиля'
)
```

- [ ] **Step 8: Run UI and embedded-script tests**

Run:

```powershell
node --test tests/admin-panel-bot-status-contract.test.js tests/admin-panel-profile-layout-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS.

- [ ] **Step 9: Commit the admin UI**

```powershell
git add -- adminPanelHTML.js tests/admin-panel-bot-status-contract.test.js tests/admin-panel-profile-layout-contract.test.js
git commit -m "Add payment operations dashboard"
```

### Task 5: Document, verify, review, and deploy

**Files:**
- Modify: `FUNCTIONALITY.md`

- [ ] **Step 1: Update functionality documentation**

In the profile dashboard section, document:

```text
- The active bot selector uses a blue name button with a small white circle marker; enabled/disabled state remains a separate green/red control.
- The profile dashboard shows up to 500 newest payment-button operations across all communities and bots, including pending, successful, canceled, failed, and unknown statuses.
- Payment operations can be filtered by integration, status, source bot, and user name/VK ID, and community cards link to the journal with a per-community count.
```

In the payment integration section, document that new records store the bot that generated the payment button and old records display a fallback without migration.

- [ ] **Step 2: Run focused regression tests**

Run:

```powershell
node --test tests/payment-integrations.test.js tests/messages-runtime.test.js tests/scheduler.test.js tests/profile-dashboard.test.js tests/admin-panel-bot-status-contract.test.js tests/admin-panel-profile-layout-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS.

- [ ] **Step 3: Run the full test suite**

Run:

```powershell
npm test
```

Expected: PASS. If unrelated pre-existing failures occur, record their exact names and verify every focused test still passes.

- [ ] **Step 4: Inspect the final diff**

Run:

```powershell
git diff --check
git status --short
git diff -- src/modules/messages.js src/modules/scheduler.js src/modules/payment-integrations.js src/modules/profile-dashboard.js adminPanelHTML.js tests/payment-integrations.test.js tests/messages-runtime.test.js tests/scheduler.test.js tests/profile-dashboard.test.js tests/admin-panel-bot-status-contract.test.js tests/admin-panel-profile-layout-contract.test.js FUNCTIONALITY.md
```

Expected: no whitespace errors; only intended task changes in the inspected diff.

- [ ] **Step 5: Request code review**

Use `superpowers:requesting-code-review` against the implementation diff. Address any confirmed issue and rerun the affected focused tests.

- [ ] **Step 6: Commit documentation and final fixes**

```powershell
git add -- FUNCTIONALITY.md
git commit -m "Document payment operations journal"
```

If review fixes changed implementation files, stage only those task files in the same final commit.

- [ ] **Step 7: Deploy the verified version**

Run:

```powershell
node scripts\deploy.js
```

Expected: deployment completes successfully. If credentials or infrastructure block deployment, preserve the verified local changes and report the exact blocking output.

- [ ] **Step 8: Verify post-deployment repository state**

Run:

```powershell
git status --short --branch
```

Expected: implementation files are committed; unrelated pre-existing worktree changes remain untouched.
