# Admin Financial Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Admin balance top-up cards with a read-only, filterable financial journal for all profiles and hide the Admin limit-increase block.

**Architecture:** Add a pure normalizer in `profile-dashboard.js` that merges existing top-up and per-profile balance-operation records into one newest-first list without adding storage or migrations. Expose that list through the existing main-admin dashboard endpoint, then render and filter it entirely from the returned normalized rows.

**Tech Stack:** Node.js CommonJS, Yandex Cloud Functions handler, embedded browser JavaScript and CSS in `adminPanelHTML.js`, Node test runner.

---

## File Structure

- Modify `src/modules/profile-dashboard.js`: normalize and merge all persisted financial records.
- Modify `src/handler.js`: request the normalized journal and return it from `getAdminDashboard`.
- Create `tests/admin-financial-operations.test.js`: unit coverage for normalization, deduplication, legacy records, and ordering.
- Modify `tests/admin-auth-flow.test.js`: verify main-admin API exposure.
- Create `tests/admin-panel-financial-operations-contract.test.js`: verify hidden limit block and the financial table/filter contract.
- Modify `adminPanelHTML.js`: replace Admin card history with the financial table and filters.
- Modify `FUNCTIONALITY.md`: document the new Admin journal and hidden limit block.

### Task 1: Normalize The Financial Journal

**Files:**
- Modify: `src/modules/profile-dashboard.js:1037-1076`
- Create: `tests/admin-financial-operations.test.js`

- [ ] **Step 1: Write failing tests for normalized operation types**

Create fixtures containing:

```js
{
  profiles: {
    '7': {
      profileId: '7',
      profileName: 'Profile 7',
      balanceOperations: [
        { id: 'daily-1', type: 'purchase_daily_limit', amount: -300, requests: 1650, communityId: '229', createdAt: '2026-06-09T12:00:00.000Z' },
        { id: 'extra-1', type: 'purchase_extra_limit', amount: -400, requests: 4600, createdAt: '2026-06-09T11:00:00.000Z' },
        { id: 'promo-1', type: 'promo_credit', amount: 250, promoCode: 'BONUS', extraRequestLimitAfter: 700, createdAt: '2026-06-09T10:00:00.000Z' },
        { id: 'admin-1', type: 'admin_balance_adjustment', amount: -50, extraRequestLimitBefore: 700, extraRequestLimitAfter: 500, createdAt: '2026-06-09T09:00:00.000Z' },
        { id: 'bug-1', type: 'bug_report_fixed_reward', amount: 0, bugReportId: 'report-1', createdAt: '2026-06-09T08:00:00.000Z' },
        { id: 'suggestion-1', type: 'suggestion_report_implemented_reward', amount: 0, suggestionReportId: 'idea-1', createdAt: '2026-06-09T07:00:00.000Z' },
        { id: 'legacy-1', type: 'legacy_unknown', amount: 12, createdAt: 'invalid-date' }
      ]
    }
  },
  balanceTopUps: []
}
```

Assert exact normalized type labels, status labels, `balanceDelta`, `limitDelta`,
community/source fields, profile identity, and that the invalid-date row sorts
last instead of being removed.

- [ ] **Step 2: Run the normalization test and verify RED**

Run:

```powershell
node --test tests/admin-financial-operations.test.js
```

Expected: FAIL because `getAdminFinancialOperationsWithDependencies` is not exported.

- [ ] **Step 3: Implement type and status normalization**

Add constants and pure helpers:

```js
const ADMIN_FINANCIAL_OPERATION_TYPES = {
    top_up: { label: 'Пополнение баланса', source: 'Платежная система' },
    purchase_daily_limit: { label: 'Покупка подписки', source: 'Баланс профиля' },
    purchase_extra_limit: { label: 'Покупка пакета', source: 'Баланс профиля' },
    promo_credit: { label: 'Промокод', source: 'Промокод' },
    bug_report_fixed_reward: { label: 'Награда за ошибку', source: 'Исправленная ошибка' },
    suggestion_report_implemented_reward: { label: 'Награда за предложение', source: 'Реализованное предложение' },
    admin_balance_adjustment: { label: 'Ручная корректировка', source: 'Главный администратор' }
};
```

Implement:

```js
function normalizeAdminFinancialOperation(profileId, container, operation)
function normalizeAdminBalanceTopUp(topUp, profiles)
function sortAdminFinancialOperations(rows)
async function getAdminFinancialOperations()
async function getAdminFinancialOperationsWithDependencies(overrides = {})
```

Rules:

- operation status defaults to `succeeded`;
- top-up status maps pending/succeeded/canceled/error to Russian labels;
- monetary top-up amount is `amountRub`, while `balanceDelta` is `credit` only
  after success;
- purchase balance delta is the negative `amount`;
- daily and extra purchase limit delta is `requests`;
- promo limit delta uses `extraRequestLimitCredit` when present, otherwise leave
  blank because `extraRequestLimitAfter` alone does not prove the delta;
- manual adjustment limit delta is `after - before`;
- fixed bug and suggestion rewards use their exported reward constants;
- unknown types become `other` / `Прочая операция` while retaining the original
  type in description/searchable source text.

- [ ] **Step 4: Preserve the explicit promo limit delta for new records**

In `grantProfilePromoCreditsWithDependencies`, add:

```js
extraRequestLimitCredit,
```

to the stored `promo_credit` balance operation. Extend
`tests/profile-balance.test.js` to assert the field equals the granted amount.
This improves future journal accuracy without rewriting historical records.

- [ ] **Step 5: Run the normalization test and verify GREEN**

Run:

```powershell
node --test tests/admin-financial-operations.test.js
```

Expected: PASS.

- [ ] **Step 6: Add a failing successful-top-up deduplication test**

Use a top-up:

```js
{ id: 'topup-1', profileId: '7', amountRub: 1000, credit: 1100, status: 'succeeded', providerPaymentId: 'pay-1', createdAt: '2026-06-09T10:00:00.000Z', creditedAt: '2026-06-09T10:01:00.000Z' }
```

and a matching balance operation:

```js
{ id: 'balance-1', type: 'top_up', amount: 1100, sourceId: 'topup-1', providerPaymentId: 'pay-1', createdAt: '2026-06-09T10:01:00.000Z' }
```

Assert one output row with top-up monetary amount, credited balance delta,
provider payment ID, and top-up ID.

- [ ] **Step 7: Run the deduplication test and verify RED**

Run:

```powershell
node --test tests/admin-financial-operations.test.js
```

Expected: FAIL with two rows for the same top-up.

- [ ] **Step 8: Implement conservative deduplication**

Build sets from normalized top-ups:

```js
const topUpIds = new Set(topUps.map(item => String(item.id || '')).filter(Boolean));
const providerPaymentIds = new Set(topUps.map(item => String(item.providerPaymentId || '')).filter(Boolean));
```

Skip a `top_up` balance operation when:

- `sourceId` matches a known top-up ID; or
- no `sourceId` exists and `providerPaymentId` matches a known top-up.

Do not merge by time and amount alone because those fields can collide.

- [ ] **Step 9: Run all financial normalizer tests**

Run:

```powershell
node --test tests/admin-financial-operations.test.js tests/profile-balance.test.js
```

Expected: PASS.

### Task 2: Expose The Journal Through The Main-Admin API

**Files:**
- Modify: `src/handler.js:136-160`
- Modify: `src/handler.js:826-859`
- Modify: `tests/admin-auth-flow.test.js:500-539`

- [ ] **Step 1: Extend the existing dashboard API test**

Mock:

```js
getAdminFinancialOperations: async () => ([
  { id: 'topup-1', profileId: '7', type: 'top_up', status: 'succeeded' }
])
```

Assert:

```js
assert.deepEqual(payload.financialOperations, [
  { id: 'topup-1', profileId: '7', type: 'top_up', status: 'succeeded' }
]);
```

- [ ] **Step 2: Run the API test and verify RED**

Run:

```powershell
node --test tests/admin-auth-flow.test.js
```

Expected: FAIL because `financialOperations` is absent.

- [ ] **Step 3: Add the handler dependency and response field**

Import `getAdminFinancialOperations`, request it in the existing post-reconcile
`Promise.all`, and add:

```js
financialOperations,
```

to the `getAdminDashboard` response.

Keep `limitRequests` and `balanceTopUps` in the response for compatibility;
only the Admin UI stops consuming them.

- [ ] **Step 4: Run API and balance tests**

Run:

```powershell
node --test tests/admin-auth-flow.test.js tests/admin-financial-operations.test.js tests/profile-balance.test.js
```

Expected: PASS.

### Task 3: Replace Admin Cards With The Filterable Table

**Files:**
- Modify: `adminPanelHTML.js:3653-3675`
- Modify: `adminPanelHTML.js:12491`
- Modify: `adminPanelHTML.js:13149-13200`
- Modify: `adminPanelHTML.js:16433-16442`
- Create: `tests/admin-panel-financial-operations-contract.test.js`

- [ ] **Step 1: Write the failing Admin UI contract test**

Assert:

```js
assert.doesNotMatch(adminPanelHTML, /id="adminLimitRequestsPanel"/);
assert.doesNotMatch(adminPanelHTML, /<h3 class="profile-manager-title">Увеличение лимитов<\/h3>/);
assert.match(adminPanelHTML, /id="adminFinancialOperationsPanel"/);
assert.match(adminPanelHTML, /Финансовые операции/);
assert.match(adminPanelHTML, /adminFinancialProfileFilter/);
assert.match(adminPanelHTML, /adminFinancialTypeFilter/);
assert.match(adminPanelHTML, /adminFinancialStatusFilter/);
assert.match(adminPanelHTML, /adminFinancialDateFrom/);
assert.match(adminPanelHTML, /adminFinancialDateTo/);
assert.match(adminPanelHTML, /adminFinancialSearch/);
assert.match(adminPanelHTML, /adminFinancialOperationsVisibleCount/);
assert.match(adminPanelHTML, /\.admin-financial-operations-table-wrap\s*\{[\s\S]*max-height:/);
assert.match(adminPanelHTML, /\.admin-financial-operations-table-wrap thead th\s*\{[\s\S]*position:\s*sticky/);
assert.doesNotMatch(adminPanelHTML, /deleteFilteredAdminFinancial/);
```

Also assert the ten approved column labels.

- [ ] **Step 2: Run the UI contract test and verify RED**

Run:

```powershell
node --test tests/admin-panel-financial-operations-contract.test.js
```

Expected: FAIL because the old limit and top-up card blocks still render.

- [ ] **Step 3: Replace the static Admin markup**

Remove the `Увеличение лимитов` surface from the Admin HTML only.

Replace `Пополнения балансов` with:

```html
<div class="settings-surface profile-manager">
  <div class="profile-manager-header">
    <div>
      <h3 class="profile-manager-title">Финансовые операции</h3>
      <div class="profile-manager-subtitle">Общий журнал денежных операций, покупок и начислений всех профилей.</div>
    </div>
  </div>
  <div id="adminFinancialOperationsPanel"></div>
</div>
```

- [ ] **Step 4: Add table CSS and render state**

Add `.admin-financial-operations-table-wrap` with:

```css
max-height: 430px;
overflow: auto;
border: 1px solid var(--table-border);
border-radius: 12px;
```

Make its `thead th` sticky at `top: 0` with a nontransparent table-header
background and higher `z-index`.

Initialize:

```js
window.adminDashboard = {
    promoCodes: [],
    recoveryRequests: [],
    loginLogs: [],
    limitRequests: [],
    balanceTopUps: [],
    financialOperations: [],
    errorReports: [],
    suggestionReports: []
};
```

- [ ] **Step 5: Implement rendering and combined filters**

Replace `renderAdminBalanceTopUps` with:

```js
window.renderAdminFinancialOperations = function() { ... };
window.filterAdminFinancialOperations = function() { ... };
```

Render profile/type/status options from unique normalized row values. Each row
must expose lowercase searchable data attributes for profile, type, status,
date, and combined search text.

Date filter rules:

```js
var fromTime = fromValue ? new Date(fromValue + 'T00:00:00').getTime() : null;
var toTime = toValue ? new Date(toValue + 'T23:59:59.999').getTime() : null;
```

Rows with invalid dates remain visible unless either date boundary is active.
Update `Отображается: N из M` after every filter change.

Use `—` for absent numeric values, prefix positive deltas with `+`, and preserve
negative signs.

- [ ] **Step 6: Load and render the API field**

In `loadAdminDashboard`, assign:

```js
financialOperations: Array.isArray(data.financialOperations) ? data.financialOperations : [],
```

In `renderAdminAuditPanel`, call `renderAdminFinancialOperations()` and remove
the limit-panel rendering call.

- [ ] **Step 7: Run UI and embedded-script tests**

Run:

```powershell
node --test tests/admin-panel-financial-operations-contract.test.js tests/admin-panel-inline-script-syntax.test.js
node --check adminPanelHTML.js
```

Expected: PASS and both syntax commands exit 0.

### Task 4: Documentation And Regression Verification

**Files:**
- Modify: `FUNCTIONALITY.md`
- Verify: all files changed in Tasks 1-3

- [ ] **Step 1: Update the functionality registry**

Document:

- `Увеличение лимитов` is hidden from Admin while its data/API remain;
- `Финансовые операции` replaces the old top-up card list;
- all profiles, statuses, purchases, promos, rewards, and manual corrections
  are included;
- filters, inclusive date period, search, count, sticky header, internal scroll,
  and read-only behavior;
- no migration or separate financial store is introduced.

- [ ] **Step 2: Run focused regression tests**

Run:

```powershell
node --test tests/admin-financial-operations.test.js tests/admin-auth-flow.test.js tests/profile-balance.test.js tests/admin-panel-financial-operations-contract.test.js tests/admin-panel-profile-layout-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS with zero failures.

- [ ] **Step 3: Run syntax and diff checks**

Run:

```powershell
node --check src/modules/profile-dashboard.js
node --check src/handler.js
node --check adminPanelHTML.js
git diff --check -- src/modules/profile-dashboard.js src/handler.js adminPanelHTML.js tests/admin-financial-operations.test.js tests/admin-auth-flow.test.js tests/admin-panel-financial-operations-contract.test.js FUNCTIONALITY.md
```

Expected: syntax checks exit 0. Report only pre-existing whitespace warnings
outside edited lines; fix any warning introduced by this change.

- [ ] **Step 4: Run the broader test suite**

Run every `tests/*.test.js` file sequentially, keeping the known
`miniapp-api.test.js` open-handle case separate so all assertions can be
reported accurately.

Expected: all ordinary test files pass; the separate Mini App file must show all
assertions passing even if its existing process handle requires timeout.

- [ ] **Step 5: Deploy**

Run:

```powershell
node scripts\deploy.js
```

Expected: main function, worker, sender, incoming trigger, and outbound trigger
all report `ACTIVE`.

- [ ] **Step 6: Report completion**

Summarize the hidden block, the new journal coverage and filters, focused/full
test evidence, any known unrelated open-handle issue, and deployment status.
