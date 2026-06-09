# Admin Financial Operations XLSX Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a true Excel export that downloads exactly the operations currently visible after filtering in the Admin `Финансовые операции` table.

**Architecture:** The browser sends the IDs of visible rows to a main-admin-only endpoint. The server reloads the authoritative financial journal, selects matching operations, builds an `.xlsx` workbook with ExcelJS, and returns it as a base64 API Gateway response. The browser converts the response to a Blob and starts a download with a local-time filename.

**Tech Stack:** Node.js, embedded browser JavaScript in `adminPanelHTML.js`, ExcelJS, `node:test`, Yandex Cloud Functions deployment.

---

### Task 1: Add and test the XLSX workbook builder

**Files:**
- Modify: `package.json`
- Modify: `package-lock.json`
- Create: `src/modules/admin-financial-export.js`
- Create: `tests/admin-financial-export.test.js`

**Step 1: Write the failing workbook tests**

Create tests covering:

```js
test('builds a financial operations workbook with the ten visible columns', async () => {
  const buffer = await buildAdminFinancialOperationsWorkbook(sampleOperations);
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);

  const sheet = workbook.getWorksheet('Финансовые операции');
  assert.deepEqual(
    sheet.getRow(1).values.slice(1),
    [
      'Дата и время',
      'Профиль',
      'Тип',
      'Статус',
      'Сумма',
      'Изменение баланса',
      'Изменение лимита',
      'Сообщество',
      'Источник / причина',
      'ID операции'
    ]
  );
});
```

Also assert:

- The header is bold and colored.
- The first row is frozen.
- Auto-filter covers all ten columns.
- Text wrapping and useful column widths are configured.
- Monetary cells are numeric and have a ruble number format.
- Missing values are exported as blank cells.
- Values beginning with optional whitespace followed by `=`, `+`, `-`, or `@` are prefixed with an apostrophe to prevent spreadsheet formula injection.
- Duplicate requested IDs produce one row.
- Unknown requested IDs are ignored.
- Selection preserves the server journal order.
- Empty ID lists and lists above 500 IDs are rejected.

**Step 2: Run the test to verify it fails**

Run:

```powershell
node --test tests/admin-financial-export.test.js
```

Expected: FAIL because the export module and ExcelJS dependency do not exist yet.

**Step 3: Install ExcelJS**

Run:

```powershell
npm install exceljs
```

Expected: `exceljs` is added to `dependencies`, and `package-lock.json` is updated.

**Step 4: Implement the focused export module**

Create functions with explicit responsibilities:

```js
function validateOperationIds(operationIds) {}
function selectFinancialOperations(operations, operationIds) {}
function sanitizeSpreadsheetText(value) {}
async function buildAdminFinancialOperationsWorkbook(operations) {}
function normalizeFinancialOperationsExportFilename(value, now = new Date()) {}
```

Implementation requirements:

- Accept between 1 and 500 unique non-empty operation IDs.
- Select only operations present in the authoritative server journal.
- Throw when no requested operation remains after selection.
- Produce exactly the ten table columns, with no hidden technical columns.
- Use worksheet name `Финансовые операции`.
- Freeze the first row and enable an auto-filter.
- Export date/time consistently with the table’s existing formatter.
- Keep amounts numeric; use blank cells for unavailable data.
- Sanitize all user-controlled textual values against formula injection.
- Normalize the response filename to the approved pattern:

```text
Финансовые_операции_YYYY-MM-DD_HH-MM.xlsx
```

**Step 5: Run the workbook tests**

Run:

```powershell
node --test tests/admin-financial-export.test.js
```

Expected: PASS.

### Task 2: Add the main-admin export endpoint

**Files:**
- Modify: `src/handler.js`
- Create: `tests/admin-financial-export-api.test.js`

**Step 1: Write failing endpoint tests**

Cover:

```js
test('main admin can export selected financial operations', async () => {
  const response = await invokeExport({
    session: mainAdminSession,
    body: {
      operationIds: ['operation-2', 'operation-1'],
      fileName: 'Финансовые_операции_2026-06-09_21-30.xlsx'
    }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.isBase64Encoded, true);
  assert.match(response.headers['Content-Type'], /spreadsheetml/);
  assert.match(response.headers['Content-Disposition'], /filename\*=/);
});
```

Also assert:

- A non-main admin receives `403`.
- A request without an authenticated admin session is rejected.
- Invalid JSON, empty IDs, and more than 500 IDs return a validation error.
- Unknown IDs return an error instead of an empty workbook.
- Duplicate IDs do not duplicate workbook rows.
- The server reloads `getAdminFinancialOperations()` and does not trust browser-supplied row data.

**Step 2: Run the endpoint test to verify it fails**

Run:

```powershell
node --test tests/admin-financial-export-api.test.js
```

Expected: FAIL because the endpoint does not exist.

**Step 3: Implement the endpoint**

Add a POST route such as:

```text
?exportAdminFinancialOperations=1
```

Implementation requirements:

- Include the route in the existing admin-session middleware condition.
- Require the same main-admin authorization used by other protected admin actions.
- Parse `{ operationIds, fileName }`.
- Reload the authoritative journal via `getAdminFinancialOperations()`.
- Validate and select the requested operation IDs server-side.
- Build the workbook through `admin-financial-export.js`.
- Return:

```js
{
  statusCode: 200,
  headers: {
    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'Content-Disposition': "attachment; filename=\"financial-operations.xlsx\"; filename*=UTF-8''..."
  },
  isBase64Encoded: true,
  body: buffer.toString('base64')
}
```

- Return structured JSON errors through the project’s existing response helpers.
- Expose only the minimum test seam required by current handler test patterns.

**Step 4: Run endpoint and related journal tests**

Run:

```powershell
node --test tests/admin-financial-export-api.test.js tests/admin-financial-operations.test.js
```

Expected: PASS.

### Task 3: Add the filtered-table download control

**Files:**
- Modify: `adminPanelHTML.js`
- Modify: `tests/admin-panel-financial-operations-contract.test.js`

**Step 1: Write failing browser contract tests**

Extend the existing contract tests to verify:

- A `Скачать Excel (N)` button is rendered next to `Отображается: N`.
- The button is disabled when `N === 0`.
- Filtering updates both the visible count and the button count.
- Every rendered row has a safely encoded operation ID data attribute.
- Export collects IDs only from rows currently visible after all active filters.
- The request body contains only `operationIds` and `fileName`.
- The filename uses browser-local time:

```text
Финансовые_операции_YYYY-MM-DD_HH-MM.xlsx
```

- While waiting, the button displays `Создаем Excel...` and is disabled.
- A successful response is converted to a Blob, downloaded with an object URL, and the URL is revoked.
- An export error is shown inline and disappears automatically.

**Step 2: Run the contract test to verify it fails**

Run:

```powershell
node --test tests/admin-panel-financial-operations-contract.test.js
```

Expected: FAIL because the control and export flow do not exist.

**Step 3: Implement the UI**

Update the generated toolbar to contain one visual action group:

```html
<div class="profile-payment-operations-toolbar">
  <div>Отображается: N</div>
  <button type="button">Скачать Excel (N)</button>
</div>
```

Follow the repository UI rule by keeping grouped controls visually aligned and consistently sized.

Add embedded browser helpers for:

```js
window.getVisibleAdminFinancialOperationIds = function () {};
window.buildAdminFinancialOperationsExportFilename = function () {};
window.exportAdminFinancialOperations = async function () {};
```

Implementation requirements:

- Read IDs only from currently visible table rows.
- Do not export hidden rows or reconstruct filters server-side.
- Reject the action locally when no row is visible.
- POST to the protected endpoint.
- Use `response.blob()`, `URL.createObjectURL`, a temporary anchor, and `URL.revokeObjectURL`.
- Restore the normal button state after success or failure.
- Display a concise inline error and clear it after three seconds.
- Respect the embedded-script escaping rule; do not introduce raw line breaks or single-level `\n` escapes inside quoted strings.

**Step 4: Run UI and inline-script tests**

Run:

```powershell
node --test tests/admin-panel-financial-operations-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS.

### Task 4: Update the functionality registry and run regression verification

**Files:**
- Modify: `FUNCTIONALITY.md`

**Step 1: Document the new functionality**

Update the Admin `Финансовые операции` entry to state that:

- Main admin can export the currently filtered/visible operations to `.xlsx`.
- The workbook contains the same ten table columns.
- Exported rows are selected server-side from the authoritative journal using visible operation IDs.
- The export is capped at 500 rows per request.

**Step 2: Run focused verification**

Run:

```powershell
node --test tests/admin-financial-export.test.js tests/admin-financial-export-api.test.js tests/admin-financial-operations.test.js tests/admin-panel-financial-operations-contract.test.js tests/admin-panel-inline-script-syntax.test.js
```

Expected: PASS.

**Step 3: Run the complete normal test suite**

Use the repository’s established full-suite command. If the Mini App suite is still known to leave an open handle, run its assertions separately and report the process behavior accurately.

Expected: All assertions pass with no new regression.

**Step 4: Inspect the final diff**

Run:

```powershell
git diff -- package.json package-lock.json src/modules/admin-financial-export.js src/handler.js adminPanelHTML.js tests/admin-financial-export.test.js tests/admin-financial-export-api.test.js tests/admin-panel-financial-operations-contract.test.js FUNCTIONALITY.md
```

Confirm:

- No unrelated user changes were reverted.
- No extra workbook columns were introduced.
- The endpoint is main-admin-only.
- The browser exports only visible IDs.
- `FUNCTIONALITY.md` matches the implementation.

### Task 5: Deploy and verify the working version

**Files:**
- No source changes expected.

**Step 1: Deploy**

Run:

```powershell
node scripts\deploy.js
```

Expected: Main function, worker, sender, and configured triggers deploy successfully.

**Step 2: Verify deployment status**

Confirm the deploy script reports the new versions as active and no deployment step failed.

**Step 3: Report completion**

Summarize:

- XLSX export behavior.
- Tests run and their result.
- Deployment result.
- Any known pre-existing open-handle behavior that remains unrelated to this change.
