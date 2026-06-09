# Payment Operations Bulk Delete Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe permanent deletion of currently filtered payment operations, a filtered/total row counter, an internally scrolling sticky-header table, and a second bottom-right scroll-to-top button.

**Architecture:** Add one focused profile-dashboard mutation that removes requested `paymentId` values from one profile container and returns the removed count. Wire it through an authenticated handler endpoint, while the embedded admin script derives the IDs from visible table rows, confirms the destructive action, calls the endpoint, and reloads the profile dashboard.

**Tech Stack:** Node.js CommonJS, `node:test`/`assert`, server-rendered HTML with embedded browser JavaScript, profile dashboard JSON storage.

---

## File Map

- Modify `src/modules/profile-dashboard.js`: add and export the payment operation deletion mutation.
- Modify `src/handler.js`: authenticate and route the bulk deletion endpoint.
- Modify `adminPanelHTML.js`: row IDs, filtered count, delete button/action, scrollable table, sticky header, and second scroll-to-top button.
- Modify `tests/profile-dashboard.test.js`: mutation behavior and validation.
- Create `tests/profile-payment-operations-delete-api.test.js`: endpoint access and response contract.
- Modify `tests/admin-panel-profile-layout-contract.test.js`: UI deletion, row count, scrolling table, and second button contracts.
- Modify `FUNCTIONALITY.md`: document permanent filtered deletion, counts, table scrolling, and the additional button.

### Task 1: Add the profile payment operation deletion mutation

**Files:**
- Modify: `tests/profile-dashboard.test.js`
- Modify: `src/modules/profile-dashboard.js`

- [ ] **Step 1: Add a failing deletion test**

Append a test that starts with three operations and deletes two requested IDs:

```js
await run('deleteProfilePaymentOperations removes only requested payment ids', async () => {
  let state = {
    profiles: {
      '7': {
        profileId: '7',
        paymentButtonPayments: [
          { paymentId: 'pay-1' },
          { paymentId: 'pay-2' },
          { paymentId: 'pay-3' }
        ]
      }
    },
    limitRequests: []
  };
  const result = await profileDashboard.__testOnly.deleteProfilePaymentOperationsWithDependencies(
    '7',
    ['pay-1', 'pay-3', 'missing'],
    {
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async (_key, value) => {
          state = value;
        }
      }
    }
  );

  assert.equal(result.removedCount, 2);
  assert.deepEqual(
    state.profiles['7'].paymentButtonPayments.map(item => item.paymentId),
    ['pay-2']
  );
});
```

- [ ] **Step 2: Add failing validation assertions**

Add:

```js
await assert.rejects(
  () => profileDashboard.__testOnly.deleteProfilePaymentOperationsWithDependencies('7', [], {}),
  /paymentIds are required/
);
```

Also verify duplicate IDs are normalized and do not inflate `removedCount`.

- [ ] **Step 3: Run the module test and verify RED**

Run:

```powershell
node --test tests/profile-dashboard.test.js
```

Expected: FAIL because `deleteProfilePaymentOperationsWithDependencies` does not exist.

- [ ] **Step 4: Implement the mutation**

Add:

```js
async function deleteProfilePaymentOperationsWithDependencies(profileId, paymentIds, overrides = {}) {
    const normalizedProfileId = String(profileId || '').trim();
    const normalizedIds = Array.from(new Set(
        (Array.isArray(paymentIds) ? paymentIds : [])
            .map(value => String(value || '').trim())
            .filter(Boolean)
    ));
    if (!normalizedProfileId) throw new Error('profileId is required');
    if (!normalizedIds.length) throw new Error('paymentIds are required');

    const data = await loadDashboardDataWithDependencies(overrides);
    const container = data.profiles[normalizedProfileId];
    const payments = Array.isArray(container?.paymentButtonPayments)
        ? container.paymentButtonPayments
        : [];
    const ids = new Set(normalizedIds);
    const remaining = payments.filter(item => !ids.has(String(item?.paymentId || item?.id || '').trim()));
    const removedCount = payments.length - remaining.length;

    if (container && removedCount > 0) {
        container.paymentButtonPayments = remaining;
        await saveDashboardDataWithDependencies(data, overrides);
    }

    return { removedCount, requestedCount: normalizedIds.length };
}

async function deleteProfilePaymentOperations(profileId, paymentIds) {
    return deleteProfilePaymentOperationsWithDependencies(profileId, paymentIds);
}
```

Export the public function and expose the dependency-injected function in `__testOnly`.

- [ ] **Step 5: Run the module test and verify GREEN**

Run:

```powershell
node --test tests/profile-dashboard.test.js
```

Expected: PASS.

### Task 2: Add the authenticated deletion endpoint

**Files:**
- Create: `tests/profile-payment-operations-delete-api.test.js`
- Modify: `src/handler.js`

- [ ] **Step 1: Write a failing endpoint contract test**

Create a test using `__testOnly.handleDeleteProfilePaymentOperationsWithDependencies`:

```js
const response = await __testOnly.handleDeleteProfilePaymentOperationsWithDependencies({
  httpMethod: 'POST',
  __adminSession: {
    principalProfile: { id: '7', role: 'admin', name: 'Profile 7' }
  },
  body: JSON.stringify({
    profileId: '7',
    principalProfileId: '7',
    paymentIds: ['pay-1', 'pay-2']
  })
}, {
  deleteProfilePaymentOperations: async (profileId, paymentIds) => {
    assert.equal(profileId, '7');
    assert.deepEqual(paymentIds, ['pay-1', 'pay-2']);
    return { removedCount: 2, requestedCount: 2 };
  },
  getProfileDashboardOverview: async profileId => ({
    profileId,
    paymentOperations: []
  })
});

assert.equal(response.statusCode, 200);
assert.deepEqual(JSON.parse(response.body), {
  success: true,
  removedCount: 2,
  dashboard: { profileId: '7', paymentOperations: [] }
});
```

Add a second assertion that a non-main admin cannot target a different profile and receives `403`.

- [ ] **Step 2: Run the endpoint test and verify RED**

Run:

```powershell
node --test tests/profile-payment-operations-delete-api.test.js
```

Expected: FAIL because the handler helper does not exist.

- [ ] **Step 3: Import, authenticate, and route the endpoint**

Import `deleteProfilePaymentOperations` from `profile-dashboard`.

Add `q.deleteProfilePaymentOperations !== undefined` to `needsAdminSession`.

Route:

```js
if (q.deleteProfilePaymentOperations !== undefined) {
    return handleDeleteProfilePaymentOperations(event);
}
```

Implement a dependency-injected handler that:

1. Parses the body.
2. Reads the authenticated principal from `event.__adminSession`.
3. Allows the principal's own profile.
4. Allows another profile only when `isMainAdminProfile(principalProfile)` is true.
5. Calls the deletion mutation.
6. Returns a fresh dashboard and `removedCount`.
7. Returns `400` for validation errors and `403` for access errors.

Expose the helper in `__testOnly`.

- [ ] **Step 4: Run the endpoint test and verify GREEN**

Run:

```powershell
node --test tests/profile-payment-operations-delete-api.test.js
```

Expected: PASS.

### Task 3: Add filtered count and permanent filtered deletion to the UI

**Files:**
- Modify: `tests/admin-panel-profile-layout-contract.test.js`
- Modify: `adminPanelHTML.js`

- [ ] **Step 1: Add failing UI contracts**

Assert:

```js
assert.match(adminPanelHTML, /data-payment-id=/);
assert.match(adminPanelHTML, /id="paymentOperationsVisibleCount"/);
assert.match(adminPanelHTML, /id="paymentOperationsTotalCount"/);
assert.match(adminPanelHTML, /Удалить отфильтрованные/);
assert.match(adminPanelHTML, /window\.deleteFilteredProfilePaymentOperations/);
assert.match(adminPanelHTML, /\?deleteProfilePaymentOperations=1/);
assert.match(adminPanelHTML, /Удалить ' \+ paymentIds\.length \+ ' операций без возможности восстановления\?/);
```

Also assert the delete request includes:

```text
profileId
principalProfileId
paymentIds
```

- [ ] **Step 2: Run the UI contract test and verify RED**

Run:

```powershell
node --test tests/admin-panel-profile-layout-contract.test.js
```

Expected: FAIL because the count and deletion controls are absent.

- [ ] **Step 3: Render stable payment IDs on rows**

For each row, compute:

```js
var paymentId = String(item.paymentId || item.providerPaymentId || '').trim();
```

Add:

```html
data-payment-id="..."
```

Rows without a payment ID remain displayable but are not deletable through bulk deletion.

- [ ] **Step 4: Update filtering to maintain count and button state**

After applying filters:

```js
var visibleRows = rows.filter(function(row) { return row.style.display !== 'none'; });
var visiblePaymentIds = visibleRows
    .map(function(row) { return String(row.getAttribute('data-payment-id') || '').trim(); })
    .filter(Boolean);
var visibleCountEl = document.getElementById('paymentOperationsVisibleCount');
var deleteButton = document.getElementById('deleteFilteredPaymentOperationsButton');
if (visibleCountEl) visibleCountEl.textContent = String(visibleRows.length);
if (deleteButton) {
    deleteButton.disabled = visiblePaymentIds.length === 0;
    deleteButton.textContent = 'Удалить отфильтрованные (' + visiblePaymentIds.length + ')';
}
```

The total count remains `paymentOperations.length`.

- [ ] **Step 5: Add the deletion action**

Implement:

```js
window.deleteFilteredProfilePaymentOperations = async function() {
    var rows = Array.from(document.querySelectorAll('#profilePaymentOperationsTableBody .payment-operation-row'))
        .filter(function(row) { return row.style.display !== 'none'; });
    var paymentIds = Array.from(new Set(rows
        .map(function(row) { return String(row.getAttribute('data-payment-id') || '').trim(); })
        .filter(Boolean)));
    if (!paymentIds.length) return;
    if (!confirm('Удалить ' + paymentIds.length + ' операций без возможности восстановления?')) return;

    var statusEl = document.getElementById('profilePaymentOperationsStatus');
    try {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('warn', 'Удаляем платежные операции...');
        var response = await fetch(window.location.href.split('?')[0] + '?deleteProfilePaymentOperations=1', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                profileId: getCurrentProfileId(),
                principalProfileId: getPrincipalProfileId(),
                paymentIds: paymentIds
            })
        });
        var result = await response.json();
        if (!result.success) throw new Error(result.error || 'Не удалось удалить платежные операции');
        await loadProfileDashboard();
        var freshStatusEl = document.getElementById('profilePaymentOperationsStatus');
        if (freshStatusEl) {
            freshStatusEl.innerHTML = makeInlineNotice('success', 'Удалено операций: ' + String(result.removedCount || 0) + '.');
        }
    } catch (error) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Ошибка удаления: ' + error.message);
    }
};
```

- [ ] **Step 6: Render the count and delete control**

Above the table render:

```html
<div class="profile-payment-operations-toolbar">
  <div>Отображается: <strong id="paymentOperationsVisibleCount">N</strong> из <strong id="paymentOperationsTotalCount">M</strong></div>
  <button id="deleteFilteredPaymentOperationsButton" class="btn btn-delete" type="button" onclick="deleteFilteredProfilePaymentOperations()">Удалить отфильтрованные (N)</button>
</div>
<div id="profilePaymentOperationsStatus"></div>
```

Initialize the button as disabled when no deletable rows exist.

- [ ] **Step 7: Run UI and inline syntax tests**

Run:

```powershell
node --test tests/admin-panel-profile-layout-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS.

### Task 4: Add the internal table scroll and second scroll-to-top button

**Files:**
- Modify: `tests/admin-panel-profile-layout-contract.test.js`
- Modify: `adminPanelHTML.js`

- [ ] **Step 1: Add failing layout contracts**

Assert:

```js
assert.match(adminPanelHTML, /\.profile-payment-operations-table-wrap\s*\{[\s\S]*max-height:/);
assert.match(adminPanelHTML, /\.profile-payment-operations-table-wrap thead th\s*\{[\s\S]*position:\s*sticky/);
assert.match(adminPanelHTML, /id="adminBackToTopButtonRight"/);
assert.match(adminPanelHTML, /\^<br>\^<br>\^/);
```

Update the existing scroll-button contract to require both IDs in `updateAdminBackToTopButton` and `scrollToAdminTabTop`.

- [ ] **Step 2: Run the layout test and verify RED**

Run:

```powershell
node --test tests/admin-panel-profile-layout-contract.test.js
```

Expected: FAIL because the scroll wrapper and second button are absent.

- [ ] **Step 3: Add the table scroll styles**

Add:

```css
.profile-payment-operations-table-wrap {
    max-height: 460px;
    overflow: auto;
    border: 1px solid var(--table-border);
    border-radius: 12px;
}
.profile-payment-operations-table-wrap thead th {
    position: sticky;
    top: 0;
    z-index: 2;
}
.profile-payment-operations-toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    margin: 12px 0;
}
```

Apply `profile-payment-operations-table-wrap` to the existing table wrapper.

- [ ] **Step 4: Add the second button**

Next to the existing body-level button add:

```html
<button id="adminBackToTopButtonRight" class="admin-back-to-top-btn admin-back-to-top-btn--right" type="button" onclick="scrollToAdminTabTop()" aria-label="Вверх">^<br>^<br>^</button>
```

Position it at bottom-right without changing the existing button's position:

```css
.admin-back-to-top-btn--right {
    top: auto;
    left: auto;
    right: 18px;
    bottom: 18px;
    min-width: 48px;
    line-height: 0.72;
}
```

Update the visibility and scroll functions to add/remove `is-visible` on both buttons.

- [ ] **Step 5: Run layout and embedded-script tests**

Run:

```powershell
node --test tests/admin-panel-profile-layout-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS.

### Task 5: Document, verify, review, and deploy

**Files:**
- Modify: `FUNCTIONALITY.md`

- [ ] **Step 1: Update functionality documentation**

Document:

- `Отображается: N из M`.
- Permanent deletion of currently filtered operations with count confirmation.
- Approximately ten visible table rows with internal scrolling and sticky headers.
- The additional bottom-right `^ / ^ / ^` scroll-to-top button.

- [ ] **Step 2: Run focused regression tests**

Run:

```powershell
node --test tests/profile-dashboard.test.js tests/profile-payment-operations-delete-api.test.js tests/admin-panel-profile-layout-contract.test.js tests/admin-panel-inline-script-syntax.test.js tests/payment-integrations.test.js
```

Expected: PASS.

- [ ] **Step 3: Run JavaScript syntax checks**

Run:

```powershell
node --check src/modules/profile-dashboard.js
node --check src/handler.js
node --check adminPanelHTML.js
```

Expected: all commands exit `0`.

- [ ] **Step 4: Inspect the scoped diff**

Run:

```powershell
git diff --check -- FUNCTIONALITY.md adminPanelHTML.js src/handler.js src/modules/profile-dashboard.js tests/profile-dashboard.test.js tests/profile-payment-operations-delete-api.test.js tests/admin-panel-profile-layout-contract.test.js
git status --short
```

If `src/handler.js` still reports pre-existing whitespace outside the touched endpoint, record it without modifying unrelated code.

- [ ] **Step 5: Request code review**

Use `superpowers:requesting-code-review` against the implementation scope. Resolve all confirmed Critical and Important findings, then rerun affected tests.

- [ ] **Step 6: Deploy**

Run:

```powershell
node scripts\deploy.js
```

Expected: main, worker, and sender functions become `ACTIVE`, and queue triggers remain active.

- [ ] **Step 7: Verify repository state**

Run:

```powershell
git status --short --branch
```

Do not stage or revert unrelated existing changes.
