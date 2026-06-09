# Admin Financial Operations XLSX Export Design

## Goal

Allow the main administrator to download the currently filtered rows from the
Admin `Финансовые операции` table as a real `.xlsx` workbook.

## User Interface

The financial operations toolbar gains a button:

`Скачать Excel (N)`

`N` is the number of currently visible rows after all profile, operation type,
status, date, and text filters are applied. The button is disabled when `N` is
zero.

Clicking the button:

1. collects the normalized operation objects represented by visible rows;
2. sends only those operation IDs to a protected export endpoint;
3. receives an XLSX blob;
4. starts the browser download immediately;
5. revokes the temporary object URL after triggering the download.

The default filename is:

`Финансовые_операции_YYYY-MM-DD_HH-MM.xlsx`

The filename uses the main administrator's browser-local date and time.

## Security And Data Integrity

The export endpoint requires a valid main-admin session. The browser sends the
current principal profile ID plus the visible operation IDs.

The server does not trust row values supplied by the browser. It reloads the
normalized financial journal and selects matching rows by ID. Duplicate IDs in
the request produce one exported row.

The server accepts at most 500 operation IDs per export. Missing, empty, or
oversized selections return a validation error. IDs not present in the current
journal are ignored; if no valid rows remain, the endpoint returns an error.

Cells beginning with `=`, `+`, `-`, or `@` after leading whitespace are prefixed
with an apostrophe before workbook generation. This prevents spreadsheet
formula execution for profile names, descriptions, IDs, and other text values.
Numeric monetary and limit fields remain numeric.

## Workbook

The workbook contains one sheet named `Финансовые операции`.

It exports exactly the ten visible table columns in the same order:

1. `Дата и время`
2. `Профиль`
3. `Тип операции`
4. `Статус`
5. `Денежная сумма`
6. `Изменение баланса`
7. `Изменение лимита`
8. `Сообщество`
9. `Источник / основание`
10. `ID операции`

Formatting:

- bold colored header row;
- frozen header row;
- autofilter across all columns;
- sensible fixed column widths;
- wrapped text for profile, community, source, and ID fields;
- date-time cells formatted for Russian display;
- monetary columns stored as numbers with a ruble-compatible number format;
- missing values stored as empty cells, not zero.

Rows follow the same newest-first order used by the table.

## Server Implementation

Use a maintained Node XLSX writer installed as a project dependency. `exceljs`
is preferred because it supports workbook styling, frozen panes, autofilters,
column widths, wrapping, numeric formats, and in-memory buffers without manual
ZIP/XML generation.

Add a focused workbook module responsible for:

- validating requested operation IDs;
- selecting journal rows in journal order;
- sanitizing spreadsheet text;
- building the workbook buffer;
- returning filename and XLSX MIME type.

The handler endpoint returns the workbook as base64 with:

- `Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
- `Content-Disposition: attachment; filename*=UTF-8''...`
- `isBase64Encoded: true`

## Error Handling

While exporting, the button is disabled and displays `Создаем Excel...`.

On success, the original button label and enabled state are restored after the
download begins. On failure, the toolbar displays an inline error notice that
automatically clears using the existing timeout helper.

Authentication and validation errors use the existing admin JSON error
response conventions. The browser detects non-XLSX responses and displays the
server error instead of downloading them.

## Verification

Automated tests must cover:

- main-admin-only endpoint access;
- empty and oversized selection rejection;
- selection by IDs using server-side journal values;
- duplicate and missing ID handling;
- exact ten-column order;
- numeric values and empty cells;
- filename and response headers;
- frozen header, autofilter, column widths, and worksheet name;
- formula-injection sanitization;
- UI button count tracking visible filtered rows;
- disabled empty state and browser Blob download flow;
- generated inline browser JavaScript syntax.

`FUNCTIONALITY.md` must be updated. After focused and broader regressions, deploy
the verified version according to repository policy.
